using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Serilog;
using Soenneker.Fixtures.Unit;
using Soenneker.Utils.Test;

namespace Soenneker.Zelos.Database.Tests;

public class Fixture : UnitFixture
{
    public override ValueTask InitializeAsync()
    {
        SetupIoC(Services);

        return base.InitializeAsync();
    }

    private static void SetupIoC(IServiceCollection services)
    {
        services.AddLogging(builder => { builder.AddSerilog(dispose: false); });

        IConfiguration config = TestUtil.BuildConfig();
        services.AddSingleton(config);
    }
}