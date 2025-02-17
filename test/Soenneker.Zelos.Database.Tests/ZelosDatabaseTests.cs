using Soenneker.Tests.FixturedUnit;
using Xunit;

namespace Soenneker.Zelos.Database.Tests;

[Collection("Collection")]
public class ZelosDatabaseTests : FixturedUnitTest
{
 
    public ZelosDatabaseTests(Fixture fixture, ITestOutputHelper output) : base(fixture, output)
    {

    }

    [Fact]
    public void Default()
    {

    }
}