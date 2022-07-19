using Microsoft.Extensions.Logging.Abstractions;
using Nexus.DataModel;
using Nexus.Extensibility;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Nexus.Sources.Tests
{
    public class LeosphereWindIrisTests
    {
        [Fact]
        public async Task ProvidesCatalog()
        {
            // arrange
            var dataSource = new LeosphereWindIris() as IDataSource;

            var context = new DataSourceContext(
                ResourceLocator: new Uri("Database", UriKind.Relative),
                SystemConfiguration: default!,
                SourceConfiguration: default!,
                RequestConfiguration: default!);

            await dataSource.SetContextAsync(context, NullLogger.Instance, CancellationToken.None);

            // act
            var actual = await dataSource.GetCatalogAsync("/A/B/C", CancellationToken.None);
            var actualIds = actual.Resources.Take(2).Select(resource => resource.Id).ToList();
            var actualGroups = actual.Resources.Take(2).SelectMany(resource => resource.Properties.GetStringArray("groups")).ToList();
            var actualTimeRange = await dataSource.GetTimeRangeAsync("/A/B/C", CancellationToken.None);

            // assert
            var expectedIds = new List<string>() { "Lidar_050m_3_LOS_index", "Lidar_080m_3_LOS_index" };
            var expectedGroups = new List<string>() { "Lidar (050 m)", "Lidar (080 m)" };
            var expectedStartDate = new DateTime(2020, 07, 28, 00, 00, 00, DateTimeKind.Utc);
            var expectedEndDate = new DateTime(2020, 08, 01, 00, 10, 00, DateTimeKind.Utc);

            Assert.True(expectedIds.SequenceEqual(actualIds));
            Assert.True(expectedGroups.SequenceEqual(actualGroups));
            Assert.Equal(expectedStartDate, actualTimeRange.Begin);
            Assert.Equal(expectedEndDate, actualTimeRange.End);
        }

        [Fact]
        public async Task ProvidesDataAvailability()
        {
            // arrange
            var dataSource = new LeosphereWindIris() as IDataSource;

            var context = new DataSourceContext(
                ResourceLocator: new Uri("Database", UriKind.Relative),
                SystemConfiguration: default!,
                SourceConfiguration: default!,
                RequestConfiguration: default!);

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
                    i => 0.0));

            expected[begin.AddDays(0)] = (0 / 144.0 + 0 / 1) / 2; // 27.
            expected[begin.AddDays(1)] = (0 / 144.0 + 1 / 1) / 2; // 28.
            expected[begin.AddDays(2)] = (0 / 144.0 + 0 / 1) / 2; // 29.
            expected[begin.AddDays(3)] = (0 / 144.0 + 0 / 1) / 2; // 30.
            expected[begin.AddDays(4)] = (3 / 144.0 + 0 / 1) / 2; // 31.
            expected[begin.AddDays(5)] = (1 / 144.0 + 0 / 1) / 2; // 01.

            Assert.True(expected.SequenceEqual(new SortedDictionary<DateTime, double>(actual)));
        }

        [Fact]
        public async Task CanReadFullDay_Real_Time()
        {
            // arrange
            var dataSource = new LeosphereWindIris() as IDataSource;

            var context = new DataSourceContext(
                ResourceLocator: new Uri("Database", UriKind.Relative),
                SystemConfiguration: default!,
                SourceConfiguration: default!,
                RequestConfiguration: default!);

            await dataSource.SetContextAsync(context, NullLogger.Instance, CancellationToken.None);

            // act
            var catalog = await dataSource.GetCatalogAsync("/A/B/C", CancellationToken.None);
            var resource = catalog.Resources.First(resource => resource.Id == "Lidar_220m_0_RWS");
            var representation = resource.Representations.First();
            var catalogItem = new CatalogItem(catalog, resource, representation);

            var begin = new DateTime(2020, 08, 01, 0, 0, 0, DateTimeKind.Utc);
            var end = new DateTime(2020, 08, 02, 0, 0, 0, DateTimeKind.Utc);
            var (data, status) = ExtensibilityUtilities.CreateBuffers(representation, begin, end);

            var result = new ReadRequest(catalogItem, data, status);
            await dataSource.ReadAsync(begin, end, new ReadRequest[] { result }, default!, new Progress<double>(), CancellationToken.None);

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
            var dataSource = new LeosphereWindIris() as IDataSource;

            var context = new DataSourceContext(
                ResourceLocator: new Uri("Database", UriKind.Relative),
                SystemConfiguration: default!,
                SourceConfiguration: default!,
                RequestConfiguration: default!);

            await dataSource.SetContextAsync(context, NullLogger.Instance, CancellationToken.None);

            // act
            var catalog = await dataSource.GetCatalogAsync("/A/B/C", CancellationToken.None);
            var resource = catalog.Resources.First(resource => resource.Id == "Lidar_050m_HWS_hub");
            var representation = resource.Representations.First();
            var catalogItem = new CatalogItem(catalog, resource, representation);

            var begin = new DateTime(2020, 07, 28, 0, 0, 0, DateTimeKind.Utc);
            var end = new DateTime(2020, 07, 29, 0, 0, 0, DateTimeKind.Utc);
            var (data, status) = ExtensibilityUtilities.CreateBuffers(representation, begin, end);

            var result = new ReadRequest(catalogItem, data, status);
            await dataSource.ReadAsync(begin, end, new ReadRequest[] { result }, default!, new Progress<double>(), CancellationToken.None);

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
    }
}