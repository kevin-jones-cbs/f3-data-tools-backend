#!/usr/bin/env python3
"""Deploy only the sandbox daily refresh, using the existing personal AWS profile.

AWS payloads use permission-restricted temporary files, deleted after each call.
Secret values are never placed in command arguments or printed.
"""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import time

parser = argparse.ArgumentParser()
parser.add_argument("--profile", default="kevin-personal")
parser.add_argument("--package", type=Path, default=Path("refresh-publish.zip"))
args = parser.parse_args()
REGION = "us-west-1"
ACCOUNT = "311293999880"
BUCKET = "f3-data-tools-config-311293999880"
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "tools/southfork-duckdb"))
from refresh import SUPPORTED_REGIONS
PREFIX = "analytics/sandbox"
KEYS = {region: f"{PREFIX}/{region}.duckdb" for region in SUPPORTED_REGIONS}
FUNCTION = "F3AnalyticsRefresh-sandbox"
ROLE = "F3AnalyticsRefresh-sandbox"
SCHEDULER_ROLE = "F3AnalyticsRefresh-sandbox-scheduler"
SCHEDULE = "F3AnalyticsRefresh-sandbox-daily"
ARN = f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{FUNCTION}"


def aws(service, operation, payload=None, optional=False, extra=()):
    command = ["aws", service, operation, "--profile", args.profile, "--region", REGION, "--output", "json", *extra]
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json") as request:
        if payload is not None:
            json.dump(payload, request)
            request.flush()
            command += ["--cli-input-json", "file://" + request.name]
        result = subprocess.run(command, text=True, capture_output=True)
    if result.returncode:
        if optional and any(code in result.stderr for code in ("NoSuchEntity", "ResourceNotFoundException", "ResourceNotFound")):
            return None
        raise RuntimeError(f"AWS {service} {operation} failed: {result.stderr.strip()}")
    return json.loads(result.stdout) if result.stdout.strip() else {}


def role(name, service, conditions=None):
    statement = {"Effect": "Allow", "Principal": {"Service": service}, "Action": "sts:AssumeRole"}
    if conditions:
        statement["Condition"] = conditions
    trust = json.dumps({"Version": "2012-10-17", "Statement": [statement]})
    current = aws("iam", "get-role", {"RoleName": name}, optional=True)
    if current is None:
        aws("iam", "create-role", {"RoleName": name, "AssumeRolePolicyDocument": trust})
    else:
        aws("iam", "update-assume-role-policy", {"RoleName": name, "PolicyDocument": trust})
    return f"arn:aws:iam::{ACCOUNT}:role/{name}"


def policy(name, statements):
    aws("iam", "put-role-policy", {"RoleName": name, "PolicyName": "SandboxRefresh",
        "PolicyDocument": json.dumps({"Version": "2012-10-17", "Statement": statements})})


identity = aws("sts", "get-caller-identity")
if identity["Account"] != ACCOUNT:
    raise SystemExit("Refusing to deploy outside the configured personal sandbox account.")
package = args.package.resolve()
digest = hashlib.sha256(package.read_bytes()).hexdigest()
package_key = f"deployments/sandbox/refresh-{digest}.zip"
aws("s3", "cp", extra=(str(package), f"s3://{BUCKET}/{package_key}", "--only-show-errors"))
source = aws("lambda", "get-function-configuration", {"FunctionName": "F3Pax-sandbox"})
google = source["Environment"]["Variables"]["GOOGLE_SVC_ACT_JSON"]
execution_role = role(ROLE, "lambda.amazonaws.com")
log_group = f"/aws/lambda/{FUNCTION}"
groups = aws("logs", "describe-log-groups", {"logGroupNamePrefix": log_group})
if not any(group["logGroupName"] == log_group for group in groups["logGroups"]):
    aws("logs", "create-log-group", {"logGroupName": log_group})
aws("logs", "put-retention-policy", {"logGroupName": log_group, "retentionInDays": 30})
policy(ROLE, [
    {"Effect": "Allow", "Action": ["logs:CreateLogStream", "logs:PutLogEvents"],
     "Resource": f"arn:aws:logs:{REGION}:{ACCOUNT}:log-group:{log_group}:*"},
    {"Effect": "Allow", "Action": "s3:PutObject",
     "Resource": [f"arn:aws:s3:::{BUCKET}/{key}" for key in KEYS.values()]},
])
configuration = {"FunctionName": FUNCTION, "Runtime": "python3.13", "Role": execution_role,
    "Handler": "lambda_function.handler", "Timeout": 600, "MemorySize": 1024,
    "Environment": {"Variables": {"GOOGLE_SVC_ACT_JSON": google, "SNAPSHOT_BUCKET": BUCKET,
        "SNAPSHOT_PREFIX": PREFIX, "PATH": "/var/task:/var/lang/bin:/usr/local/bin:/usr/bin:/bin:/opt/bin",
        "DOTNET_SYSTEM_GLOBALIZATION_INVARIANT": "1",
        "DOTNET_BUNDLE_EXTRACT_BASE_DIR": "/tmp/dotnet"}}}
current = aws("lambda", "get-function-configuration", {"FunctionName": FUNCTION}, optional=True)
code = {"S3Bucket": BUCKET, "S3Key": package_key}
if current is None:
    # IAM trust policies can take a few seconds to propagate to Lambda.
    for attempt in range(6):
        try:
            aws("lambda", "create-function", {**configuration, "Code": code, "Architectures": ["x86_64"]})
            break
        except RuntimeError as error:
            if "cannot be assumed" not in str(error) or attempt == 5:
                raise
            time.sleep(5)
    aws("lambda", "wait", extra=("function-active-v2", "--function-name", FUNCTION))
else:
    aws("lambda", "update-function-configuration", configuration)
    aws("lambda", "wait", extra=("function-updated-v2", "--function-name", FUNCTION))
    aws("lambda", "update-function-code", {"FunctionName": FUNCTION, **code})
    aws("lambda", "wait", extra=("function-updated-v2", "--function-name", FUNCTION))
aws("lambda", "put-function-concurrency", {"FunctionName": FUNCTION, "ReservedConcurrentExecutions": 1})
scheduler_role = role(SCHEDULER_ROLE, "scheduler.amazonaws.com", {
    "StringEquals": {"aws:SourceAccount": ACCOUNT},
    "ArnEquals": {"aws:SourceArn": f"arn:aws:scheduler:{REGION}:{ACCOUNT}:schedule-group/default"},
})
policy(SCHEDULER_ROLE, [{"Effect": "Allow", "Action": "lambda:InvokeFunction", "Resource": ARN}])
schedule = {"Name": SCHEDULE, "ScheduleExpression": "cron(0 5 * * ? *)",
    "ScheduleExpressionTimezone": "America/Los_Angeles", "FlexibleTimeWindow": {"Mode": "OFF"},
    "State": "ENABLED", "Target": {"Arn": ARN, "RoleArn": scheduler_role,
        "Input": "{}", "RetryPolicy": {"MaximumEventAgeInSeconds": 3600, "MaximumRetryAttempts": 2}}}
operation = "create-schedule" if aws("scheduler", "get-schedule", {"Name": SCHEDULE}, optional=True) is None else "update-schedule"
for attempt in range(6):
    try:
        aws("scheduler", operation, schedule)
        break
    except RuntimeError as error:
        if "must allow AWS EventBridge Scheduler to assume" not in str(error) or attempt == 5:
            raise
        time.sleep(5)
# Keep the shared chat Lambda's other settings and secrets intact. Only the
# sandbox function may use the new read permissions on its shared IAM role.
chat = aws("lambda", "get-function-configuration", {"FunctionName": "F3Pax-sandbox"})
aws("iam", "put-role-policy", {
    "RoleName": chat["Role"].rsplit("/", 1)[-1], "PolicyName": "F3SandboxAnalyticsSnapshotsRead",
    "PolicyDocument": json.dumps({"Version": "2012-10-17", "Statement": [{
        "Effect": "Allow", "Action": "s3:GetObject",
        "Resource": [f"arn:aws:s3:::{BUCKET}/{key}" for key in KEYS.values()],
        "Condition": {"ArnEquals": {"lambda:SourceFunctionArn":
            f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:F3Pax-sandbox"}},
    }]}),
})
variables = chat["Environment"]["Variables"].copy()
for region in SUPPORTED_REGIONS:
    variables.pop(f"F3_ANALYTICS_REGIONS__{region}", None)
variables["F3_ANALYTICS_S3_PREFIX"] = f"s3://{BUCKET}/{PREFIX}"
if sum(len(key.encode()) + len(value.encode()) for key, value in variables.items()) > 4096:
    raise RuntimeError("Chat environment exceeds Lambda's 4 KiB limit")
aws("lambda", "update-function-configuration", {"FunctionName": "F3Pax-sandbox",
    "RevisionId": chat["RevisionId"], "Environment": {"Variables": variables}})
aws("lambda", "wait", extra=("function-updated-v2", "--function-name", "F3Pax-sandbox"))
print(json.dumps({"function": FUNCTION, "schedule": SCHEDULE, "regions": list(SUPPORTED_REGIONS),
    "time": "05:00 America/Los_Angeles", "package": digest}))
