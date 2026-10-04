#!/usr/bin/env python3
"""Enable sandbox saved-chat reading using the existing local admin password.

Secrets are read in memory and sent through private temporary AWS request files.
"""
import argparse
import json
from pathlib import Path
import subprocess
import tempfile

parser = argparse.ArgumentParser()
parser.add_argument('--profile', default='kevin-personal')
args = parser.parse_args()
ACCOUNT = '311293999880'
FUNCTION = 'F3Pax-sandbox'
REGION = 'us-west-1'


def aws(service, operation, payload=None, extra=()):
    command = ['aws', service, operation, '--profile', args.profile, '--region', REGION, '--output', 'json', *extra]
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json') as request:
        if payload is not None:
            json.dump(payload, request)
            request.flush()
            command += ['--cli-input-json', 'file://' + request.name]
        result = subprocess.run(command, text=True, capture_output=True)
    if result.returncode:
        raise RuntimeError(f'AWS {service} {operation} failed; check account permissions/configuration.')
    return json.loads(result.stdout) if result.stdout.strip() else {}


if aws('sts', 'get-caller-identity')['Account'] != ACCOUNT:
    raise SystemExit('Refusing to configure a different AWS account.')
config = aws('lambda', 'get-function-configuration', {'FunctionName': FUNCTION})
variables = config['Environment']['Variables'].copy()
password = variables.get('F3_CHAT_ADMIN_PASSWORD')
if not password:
    local = Path(__file__).resolve().parents[2] / 'F3Lambda/Secrets/chat.local.json'
    password = json.loads(local.read_text()).get('F3_CHAT_ADMIN_PASSWORD')
if not password or len(password) > 256:
    raise SystemExit('Configure an admin password of 1–256 characters in the local backend settings.')
variables['F3_CHAT_ADMIN_PASSWORD'] = password
if sum(len(k.encode()) + len(v.encode()) for k, v in variables.items()) > 4096:
    raise SystemExit('Lambda environment would exceed its 4 KiB limit.')
uri = variables.get('F3_CHAT_LOG_S3_URI', '')
expected = 's3://f3-data-tools-config-311293999880/chat-logs/sandbox/'
if uri.rstrip('/') != expected.rstrip('/'):
    raise SystemExit('Unexpected sandbox log destination; review permissions before proceeding.')
condition = {'ArnEquals': {'lambda:SourceFunctionArn': f'arn:aws:lambda:{REGION}:{ACCOUNT}:function:{FUNCTION}'}}
policy = {'Version': '2012-10-17', 'Statement': [
    {'Effect': 'Allow', 'Action': 's3:GetObject',
     'Resource': 'arn:aws:s3:::f3-data-tools-config-311293999880/chat-logs/sandbox/*', 'Condition': condition},
    {'Effect': 'Allow', 'Action': 's3:ListBucket',
     'Resource': 'arn:aws:s3:::f3-data-tools-config-311293999880',
     'Condition': {**condition, 'StringLike': {'s3:prefix': 'chat-logs/sandbox/*'}}},
]}
aws('iam', 'put-role-policy', {'RoleName': config['Role'].rsplit('/', 1)[-1],
    'PolicyName': 'F3SandboxChatAdminRead', 'PolicyDocument': json.dumps(policy)})
aws('lambda', 'update-function-configuration', {'FunctionName': FUNCTION,
    'RevisionId': config['RevisionId'], 'Environment': {'Variables': variables}})
aws('lambda', 'wait', extra=('function-updated-v2', '--function-name', FUNCTION))
print('Sandbox admin enabled with the existing admin password and scoped S3 read access.')
