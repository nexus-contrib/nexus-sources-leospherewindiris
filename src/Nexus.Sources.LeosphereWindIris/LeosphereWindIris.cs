using Microsoft.Extensions.Logging;
using Nexus.DataModel;
using Nexus.Extensibility;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Nexus.Sources.LeosphereWindIris
{
    [ExtensionDescription("Provides access to databases with Leosphere wind iris files.")]
    public class LeosphereWindIris : StructuredFileDataSource
    {
        #region Fields

        private Dictionary<string, CatalogDescription> _config = null!;
        private NumberFormatInfo _nfi;

        #endregion

        #region Properties

        private DataSourceContext Context { get; set; } = null!;

        #endregion

        #region Constructors

        public LeosphereWindIris()
        {
            _nfi = new NumberFormatInfo()
            {
                NumberDecimalSeparator = ".",
                NumberGroupSeparator = string.Empty
            };
        }

        #endregion

        #region Methods

        protected override async Task SetContextAsync(DataSourceContext context, CancellationToken cancellationToken)
        {
            this.Context = context;

            var configFilePath = Path.Combine(this.Root, "config.json");

            if (!File.Exists(configFilePath))
                throw new Exception($"Configuration file {configFilePath} not found.");

            var jsonString = await File.ReadAllTextAsync(configFilePath, cancellationToken);
            _config = JsonSerializer.Deserialize<Dictionary<string, CatalogDescription>>(jsonString) ?? throw new Exception("config is null");
        }

        protected override Task<FileSourceProvider> GetFileSourceProviderAsync(CancellationToken cancellationToken)
        {
            var allFileSources = _config.ToDictionary(
                config => config.Key,
                config => config.Value.FileSources.Cast<FileSource>().ToArray());

            var fileSourceProvider = new FileSourceProvider(
                All: allFileSources,
                Single: catalogItem =>
                {
                    var properties = catalogItem.Resource.Properties;

                    if (properties is null)
                        throw new ArgumentNullException(nameof(properties));

                    return allFileSources[catalogItem.Catalog.Id]
                        .First(fileSource => ((ExtendedFileSource)fileSource).Name == properties["FileSource"]);
                });

            return Task.FromResult(fileSourceProvider);
        }

        protected override Task<string[]> GetCatalogIdsAsync(CancellationToken cancellationToken)
        {
            return Task.FromResult(_config.Keys.ToArray());
        }

        protected override Task<ResourceCatalog> GetCatalogAsync(string catalogId, CancellationToken cancellationToken)
        {
            var catalogDescription = _config[catalogId];
            var catalog = new ResourceCatalog(id: catalogId);

            foreach (var fileSource in catalogDescription.FileSources)
            {
                var fileSourceNameParts = fileSource.Name.Split(';');
                var instrument = fileSourceNameParts[0];
                var mode = fileSourceNameParts[1];
                var filePaths = default(string[]);

                if (fileSource.CatalogSourceFiles is not null)
                {
                    filePaths = fileSource.CatalogSourceFiles
                        .Select(filePath => Path.Combine(this.Root, filePath))
                        .ToArray();
                }
                else
                {
                    // this.TryGetFirstFile works only with non-empty folders (2020-07 folder does not contain averaged data)
                    var filePath = Directory.EnumerateFiles(this.Root, $"*{mode}*.csv", SearchOption.AllDirectories).First();

                    filePaths = new[] { filePath };
                }

                cancellationToken.ThrowIfCancellationRequested();

                foreach (var filePath in filePaths)
                {
                    if (string.IsNullOrWhiteSpace(filePath))
                        continue;

                    using var file = new StreamReader(File.OpenRead(filePath));

                    var customParamters = fileSource.CustomParameters;

                    if (customParamters is null)
                        throw new Exception("custom parameters is null");

                    var samplePeriod = TimeSpan.Parse(customParamters["SamplePeriod"]);

                    var distances = customParamters["Distances"]
                        .Split(",")
                        .Select(value => int.Parse(value))
                        .ToList();

                    var resources = mode == "real_time"
                        ? this.GetRawResources(file, instrument, samplePeriod, fileSource, distances)
                        : this.GetAverageResources(file, instrument, samplePeriod, fileSource, distances);

                    var duplicateKeys = resources.GroupBy(x => x.Id)
                        .Where(group => group.Count() > 1)
                        .Select(group => group.Key);

                    var newCatalog = new ResourceCatalogBuilder(id: catalogId)
                        .AddResources(resources)
                        .Build();

                    catalog = catalog.Merge(newCatalog, MergeMode.NewWins);
                }
            }

            return Task.FromResult(catalog);
        }

        protected override Task ReadSingleAsync(ReadInfo info, CancellationToken cancellationToken)
        {
            var properties = info.CatalogItem.Resource.Properties;

            if (properties is null)
                throw new Exception("properties is null");

            if (properties.Where(current => current.Key.StartsWith("Groups")).Any(current => current.Value.Contains("avg")))
                return this.ReadSingleAverageAsync(info, cancellationToken);

            else
                return this.ReadSingleRawAsync(info, cancellationToken);
        }

        private Task ReadSingleAverageAsync(ReadInfo info, CancellationToken cancellationToken)
        {
            return Task.Run(() =>
            {
                var resourceIdParts = info.CatalogItem.Resource.Id.Split('_', 3);
                var instrument = resourceIdParts[0];
                var distance = int.Parse(resourceIdParts[1].Substring(0, 3));
                var resourceName = resourceIdParts[2];

                var lines = File.ReadAllLines(info.FilePath);
                (var columns, var distances) = this.GetAverageFileParameters(lines);

                // column
                var column = columns.IndexOf(resourceName);

                if (column == -1)
                    throw new Exception("The requested representation does not exist.");

                // row
                var indexOfDistance = distances.IndexOf(distance);

                if (indexOfDistance >= 0)
                {
                    var lineOffset = distances.IndexOf(distance) + 1;

                    // go
                    var linesToSkip = distances.Count;

                    // the files have two empty line at the end ... except when they are exported
                    // by the Leosphere software, then there is just one line -.-
                    var requiredLineCount = info.FileLength * linesToSkip;

                    if (lines.Length >= requiredLineCount && !string.IsNullOrWhiteSpace(lines[requiredLineCount - 1]))
                    {
                        var result = new double[info.FileBlock];

                        for (int i = 0; i < info.FileLength; i++)
                        {
                            var dataParts = lines[i * linesToSkip + lineOffset].Split(';', count: Math.Max(column + 2, 2));

                            // check that we have the correct distance, might be wrong if file has inconsistent distances
                            var actualDistance = (int)double.Parse(dataParts[1], _nfi);

                            if (actualDistance != distance)
                                continue;

                            var value = dataParts[column];
                            result[i] = double.Parse(value, _nfi);
                        }

                        // write data
                        var byteResult = MemoryMarshal.AsBytes(result.AsSpan());
                        var offset = (int)info.FileOffset * info.CatalogItem.Representation.ElementSize;

                        byteResult
                            .Slice(offset)
                            .CopyTo(info.Data.Span);

                        info
                            .Status
                            .Span
                            .Fill(1);
                    }
                    else
                    {
                        this.Context.Logger.LogDebug("The actual buffer size does not match the expected size, which indicates an incomplete file");
                    }
                }
            }, cancellationToken);
        }

        private Task ReadSingleRawAsync(ReadInfo info, CancellationToken cancellationToken) 
        {
            return Task.Run(() =>
            {
                var resourceIdParts = info.CatalogItem.Resource.Id.Split('_', 4);
                var instrument = resourceIdParts[0];
                var distance = int.Parse(resourceIdParts[1].Substring(0, 3));
                var beam = int.Parse(resourceIdParts[2]);
                var resourceName = resourceIdParts[3];

                var lines = File.ReadAllLines(info.FilePath);
                (var columns, var distances, var firstBeam) = this.GetRawFileParameters(lines);

                // column
                var column = columns.IndexOf(resourceName);

                if (column == -1)
                    throw new Exception("The requested representation does not exist.");

                // row
                var beamPosition = (4 - firstBeam + beam) % 4; // The raw files may start with an arbitrary beam, e.g. beam 3.
                var indexOfDistance = distances.IndexOf(distance);

                if (indexOfDistance >= 0)
                {
                    var lineOffset = distances.Count * beamPosition + indexOfDistance + 1;

                    // go
                    var linesToSkip = distances.Count * 4;

                    if (info.FileLength * linesToSkip == lines.Length - 2) // the files have an empty extra line at the end
                    {
                        var result = new double[info.FileBlock];

                        for (int i = 0; i < info.FileLength; i++)
                        {
                            var dataParts = lines[i * linesToSkip + lineOffset].Split(';', count: Math.Max(column + 2, 3));

                            // check that we have the correct distance, might be wrong if file has inconsistent distances
                            var actualDistance = (int)double.Parse(dataParts[2], _nfi);

                            if (actualDistance != distance)
                                continue;

                            var value = dataParts[column];
                            result[i] = double.Parse(value, _nfi);
                        }

                        // write data
                        var byteResult = MemoryMarshal.AsBytes(result.AsSpan());
                        var offset = (int)info.FileOffset * info.CatalogItem.Representation.ElementSize;

                        byteResult
                            .Slice(offset)
                            .CopyTo(info.Data.Span);

                        info
                            .Status
                            .Span
                            .Fill(1);
                    }
                    else
                    {
                        this.Context.Logger.LogDebug("The actual buffer size does not match the expected size, which indicates an incomplete file");
                    }
                }
                else
                {
                    this.Context.Logger.LogDebug("Distance {Distance} does not exist.", distance);
                }
            }, cancellationToken);
        }

        private (List<string> Columns, List<int> Distances) GetAverageFileParameters(string[] lines)
        {
            // columns
            var headerLine = lines[0];
            var headerLineParts = headerLine.Split(';');

            var columns = headerLineParts.Select(value =>
            {
                return Utilities
                    .EnforceNamingConvention(value)
                    .Replace("__", "_")
                    .Trim('_');
            }).ToList();

            // distances
            var dataLines = lines.Skip(1);
            var distances = new List<int>();

            foreach (var line in dataLines)
            {
                var dataParts = line.Split(';', 4);
                var distance = (int)double.Parse(dataParts[1], _nfi);

                if (!distances.Any() || distance > distances[0])
                    distances.Add(distance);

                else
                    break;
            }

            return (columns, distances);
        }

        private (List<string> Columns, List<int> Distances, int FirstBeam) GetRawFileParameters(string[] lines)
        {
            // columns
            var headerLine = lines[0];
            var headerLineParts = headerLine.Split(';');

            var columns = headerLineParts.Select(value =>
            {
                return Utilities
                    .EnforceNamingConvention(value)
                    .Replace("__", "_")
                    .Trim('_');
            }).ToList();

            // first beam
            var dataLines = lines.Skip(1);
            var firstBeam = int.Parse(dataLines.First().Split(';', 3)[1]);

            // distances
            var distances = new List<int>();

            foreach (var line in dataLines)
            {
                var dataParts = line.Split(';', 4);
                var distance = (int)double.Parse(dataParts[2], _nfi);

                if (!distances.Any() || distance > distances[0])
                    distances.Add(distance);

                else
                    break;
            }

            return (columns, distances, firstBeam);
        }

        private List<Resource> GetAverageResources(
            StreamReader file,
            string instrument,
            TimeSpan samplePeriod,
            ExtendedFileSource fileSource,
            List<int> distances)
        {
            var line = file.ReadLine();

            if (line is null)
                throw new Exception("line is null");

            return line.Split(';')
                .Skip(1)
                .SelectMany(value =>
                {
                    var name = Utilities.EnforceNamingConvention(value).Replace("__", "_").Trim('_');
                    var resources = new List<Resource>();

                    foreach (var distance in distances)
                    {
                        var representation = new Representation(
                            dataType: NexusDataType.FLOAT64,
                            samplePeriod: samplePeriod);

                        var resourceId = $"{instrument}_{distance:D3}m_{name}";

                        var resource = new ResourceBuilder(id: resourceId)
                            .WithGroups($"{instrument} ({distance:D3} m, avg)")
                            .WithProperty("FileSource", fileSource.Name)
                            .AddRepresentation(representation)
                            .Build();
                        
                        resources.Add(resource);
                    }

                    return resources;
                }).ToList();
        }

        private List<Resource> GetRawResources(
            StreamReader file,
            string instrument,
            TimeSpan samplePeriod,
            ExtendedFileSource fileSource,
            List<int> distances)
        {
            var line = file.ReadLine();

            if (line is null)
                throw new Exception("line is null");

            return line.Split(';')
                .Skip(1)
                .SelectMany(value =>
                {
                    var name = Utilities.EnforceNamingConvention(value).Replace("__", "_").Trim('_');
                    var resources = new List<Resource>();

                    for (int i = 0; i < 4; i++)
                    {
                        foreach (var distance in distances)
                        {
                            var representation = new Representation(
                                dataType: NexusDataType.FLOAT64,
                                samplePeriod: samplePeriod);

                            var resourceId = $"{instrument}_{distance:D3}m_{(i + 3) % 4}_{name}";

                            var resource = new ResourceBuilder(id: resourceId)
                                .WithGroups($"{instrument} ({distance:D3} m)")
                                .WithProperty("FileSource", fileSource.Name)
                                .AddRepresentation(representation)
                                .Build();
                            
                            resources.Add(resource);
                        }
                    }                   

                    return resources;
                }).ToList();
        }

        #endregion
    }
}
