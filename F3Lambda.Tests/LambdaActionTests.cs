using F3Core;
using Xunit;

namespace F3Lambda.Tests
{
    public class LambdaActionTests
    {
        [Fact]
        public void AllActionNamesAreUniqueAndResolvable()
        {
            var actionNames = LambdaActions.All.Select(action => action.Name).ToList();

            Assert.Equal(actionNames.Count, actionNames.Distinct().Count());

            foreach (var actionName in actionNames)
            {
                Assert.True(LambdaActions.TryGetDefinition(actionName, out var definition));
                Assert.NotNull(definition);
                Assert.Equal(actionName, definition!.Name);
            }
        }

        [Fact]
        public void SmokeTestActionsAreReadOnly()
        {
            Assert.All(LambdaActions.SmokeTestActions, action =>
            {
                Assert.False(action.IsWrite);
            });
        }
    }
}
