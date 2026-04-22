using Soenneker.Tests.HostedUnit;

namespace Soenneker.Zelos.Database.Tests;

[ClassDataSource<Host>(Shared = SharedType.PerTestSession)]
public class ZelosDatabaseTests : HostedUnitTest
{
 
    public ZelosDatabaseTests(Host host) : base(host)
    {

    }

    [Test]
    public void Default()
    {

    }
}