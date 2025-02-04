using Microsoft.Extensions.Logging.Abstractions;
using Nexus.DataModel;
using Nexus.Extensibility;
using System.Runtime.InteropServices;
using System.Text.Json;
using Xunit;

namespace Nexus.Sources.Tests;

using MySettings = StructuredFileDataSourceSettings<LeosphereWindIrisSettings, AdditionalFileSourceSettings>;

public class LeosphereWindIrisTests
{
    private const string ROOT = "Database";

    [Fact]
    public async Task ProvidesCatalog()
    {
        // arrange
        var dataSource = new LeosphereWindIris() as IDataSource<MySettings>;
        var context = BuildContext(ROOT);

        await dataSource.SetContextAsync(context, NullLogger.Instance, CancellationToken.None);

        // act
        var actual = await dataSource.EnrichCatalogAsync(new("/A/B/C"), CancellationToken.None);
        var actualIds = actual.Resources!.Take(2).Select(resource => resource.Id).ToList();
        var actualGroups = actual.Resources!.Take(2).SelectMany(resource => resource.Properties?.GetStringArray("groups")!).ToList();
        var (begin, end) = await dataSource.GetTimeRangeAsync("/A/B/C", CancellationToken.None);

        // assert
        var expectedIds = new List<string>() { "Lidar_3_LOS_index", "Lidar_0_LOS_index" };
        var expectedGroups = new List<string>() { "Lidar", "Lidar" };
        var expectedStartDate = new DateTime(2020, 07, 28, 00, 00, 00, DateTimeKind.Utc);
        var expectedEndDate = new DateTime(2020, 08, 01, 00, 10, 00, DateTimeKind.Utc);

        Assert.True(expectedIds.SequenceEqual(actualIds));
        Assert.True(expectedGroups.SequenceEqual(actualGroups));
        Assert.Equal(expectedStartDate, begin);
        Assert.Equal(expectedEndDate, end);
    }

    [Fact]
    public async Task ProvidesDataAvailability()
    {
        // arrange
        var dataSource = new LeosphereWindIris() as IDataSource<MySettings>;
        var context = BuildContext(ROOT);

        await dataSource.SetContextAsync(context, NullLogger.Instance, CancellationToken.None);

        // act
        var actual = new Dictionary<DateTime, double>();
        var begin = new DateTime(2020, 07, 27, 0, 0, 0, DateTimeKind.Utc);
        var end = new DateTime(2020, 08, 02, 0, 0, 0, DateTimeKind.Utc);

        var currentBegin = begin;

        while (currentBegin < end)
        {
            actual[currentBegin] = await dataSource.GetAvailabilityAsync("/A/B/C", currentBegin, currentBegin.AddDays(1), CancellationToken.None);
            currentBegin += TimeSpan.FromDays(1);
        }

        // assert
        var expected = new SortedDictionary<DateTime, double>(Enumerable.Range(0, 6).ToDictionary(
                i => begin.AddDays(i),
                i => 0.0))
        {
            [begin.AddDays(0)] = (0 / 144.0 + 0 / 1) / 2, // 27.
            [begin.AddDays(1)] = (0 / 144.0 + 1 / 1) / 2, // 28.
            [begin.AddDays(2)] = (0 / 144.0 + 0 / 1) / 2, // 29.
            [begin.AddDays(3)] = (0 / 144.0 + 0 / 1) / 2, // 30.
            [begin.AddDays(4)] = (3 / 144.0 + 0 / 1) / 2, // 31.
            [begin.AddDays(5)] = (1 / 144.0 + 0 / 1) / 2 // 01.
        };

        Assert.True(expected.SequenceEqual(new SortedDictionary<DateTime, double>(actual)));
    }

    [Fact]
    public async Task CanReadFullDay_Real_Time()
    {
        // arrange
        var dataSource = new LeosphereWindIris() as IDataSource<MySettings>;
        var context = BuildContext(ROOT);

        await dataSource.SetContextAsync(context, NullLogger.Instance, CancellationToken.None);

        // act
        var catalog = await dataSource.EnrichCatalogAsync(new("/A/B/C"), CancellationToken.None);
        var resource = catalog.Resources!.First(resource => resource.Id == "Lidar_0_RWS");
        var representation = resource.Representations![0];
        var parameters = new Dictionary<string, string>() { ["d"] = "220" };
        var catalogItem = new CatalogItem(catalog, resource, representation, parameters);

        var begin = new DateTime(2020, 08, 01, 0, 0, 0, DateTimeKind.Utc);
        var end = new DateTime(2020, 08, 02, 0, 0, 0, DateTimeKind.Utc);
        var (data, status) = ExtensibilityUtilities.CreateBuffers(representation, begin, end);

        var result = new ReadRequest(resource.Id, catalogItem, data, status);
        await dataSource.ReadAsync(begin, end, [result], default!, new Progress<double>(), CancellationToken.None);

        // assert
        void DoAssert()
        {
            var data = MemoryMarshal.Cast<byte, double>(result.Data.Span);

            Assert.Equal(12.45, data[0]);
            Assert.Equal(11.59, data[149]);
            Assert.Equal(0, data[150]);

            Assert.Equal(1, result.Status.Span[0]);
            Assert.Equal(1, result.Status.Span[149]);
            Assert.Equal(0, result.Status.Span[150]);
        }

        DoAssert();
    }

    [Fact]
    public async Task CanReadFullDay_Average()
    {
        // arrange
        var dataSource = new LeosphereWindIris() as IDataSource<MySettings>;
        var context = BuildContext(ROOT);

        await dataSource.SetContextAsync(context, NullLogger.Instance, CancellationToken.None);

        // act
        var catalog = await dataSource.EnrichCatalogAsync(new("/A/B/C"), CancellationToken.None);
        var resource = catalog.Resources!.First(resource => resource.Id == "Lidar_HWS_hub");
        var representation = resource.Representations![0];
        var parameters = new Dictionary<string, string>() { ["d"] = "50" };
        var catalogItem = new CatalogItem(catalog, resource, representation, parameters);

        var begin = new DateTime(2020, 07, 28, 0, 0, 0, DateTimeKind.Utc);
        var end = new DateTime(2020, 07, 29, 0, 0, 0, DateTimeKind.Utc);
        var (data, status) = ExtensibilityUtilities.CreateBuffers(representation, begin, end);

        var result = new ReadRequest(resource.Id, catalogItem, data, status);
        await dataSource.ReadAsync(begin, end, [result], default!, new Progress<double>(), CancellationToken.None);

        // assert
        void DoAssert()
        {
            var data = MemoryMarshal.Cast<byte, double>(result.Data.Span);

            Assert.Equal(9.76, data[0]);
            Assert.Equal(9.32, data[143]);

            Assert.Equal(1, result.Status.Span[0]);
            Assert.Equal(1, result.Status.Span[143]);
        }

        DoAssert();
    }

    private DataSourceContext<MySettings> BuildContext(string settingsFolderPath)
    {
        var configFilePath = Path.Combine(settingsFolderPath, "config.json");

        if (!File.Exists(configFilePath))
            throw new Exception($"The configuration file does not exist on path {configFilePath}.");

        var jsonString = File.ReadAllText(configFilePath);
        var sourceConfiguration = JsonSerializer.Deserialize<MySettings>(jsonString, JsonSerializerOptions.Web)!;

        var context = new DataSourceContext<MySettings>(
            ResourceLocator: new Uri(ROOT, UriKind.Relative),
            SourceConfiguration: sourceConfiguration,
            RequestConfiguration: default!
        );

        return context;
    }
}