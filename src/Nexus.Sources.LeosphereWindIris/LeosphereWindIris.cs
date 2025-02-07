using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Nexus.DataModel;
using Nexus.Extensibility;

namespace Nexus.Sources;

/// <summary>
/// Additional extension-specific settings.
/// </summary>
/// <param name="TitleMap">The catalog ID to title map. Add an entry here to specify a custom catalog title.</param>
public record LeosphereWindIrisSettings(
    Dictionary<string, string> TitleMap
);

/// <summary>
/// Additional file source settings.
/// </summary>
/// <param name="SamplePeriod">The sample period of the data.</param>
/// <param name="CatalogSourceFiles">The source files to populate the catalog with resources.</param>
public record AdditionalFileSourceSettings(
    TimeSpan SamplePeriod,
    IReadOnlyList<string> CatalogSourceFiles
);

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

[ExtensionDescription(
    "Provides access to databases with Leosphere wind iris files.",
    "https://github.com/Apollo3zehn/nexus-sources-leospherewindiris",
    "https://github.com/Apollo3zehn/nexus-sources-leospherewindiris")]
public class LeosphereWindIris
    : StructuredFileDataSource<LeosphereWindIrisSettings, AdditionalFileSourceSettings>
    
{
    private readonly NumberFormatInfo _nfi;

    public LeosphereWindIris()
    {
        _nfi = new NumberFormatInfo()
        {
            NumberDecimalSeparator = ".",
            NumberGroupSeparator = string.Empty
        };
    }

    protected override Task<CatalogRegistration[]> GetCatalogRegistrationsAsync(
        string path, 
        CancellationToken cancellationToken
    )
    {
        if (path == "/")
        {
            return Task.FromResult(Context.SourceConfiguration.FileSourceGroupsMap
                .Select(entry =>
                    {
                        Context.SourceConfiguration.AdditionalSettings.TitleMap.TryGetValue(entry.Key, out var title);
                        return new CatalogRegistration(entry.Key, title);
                    }
                ).ToArray());
        }

        else
        {
            return Task.FromResult(Array.Empty<CatalogRegistration>());
        }
    }

    protected override Task<ResourceCatalog> EnrichCatalogAsync(
        ResourceCatalog catalog, 
        CancellationToken cancellationToken
    )
    {
        var fileSourceGroupsMap = Context.SourceConfiguration.FileSourceGroupsMap[catalog.Id];

        foreach (var (fileSourceId, fileSourceGroup) in fileSourceGroupsMap)
        {
            foreach (var fileSource in fileSourceGroup)
            {
                var fileSourceIdParts = fileSourceId.Split(';');
                var instrument = fileSourceIdParts[0];
                var mode = fileSourceIdParts[1];
                var filePaths = default(string[]);
                var catalogSourceFiles = fileSource.AdditionalSettings.CatalogSourceFiles;

                if (catalogSourceFiles is not null)
                {
                    filePaths = catalogSourceFiles
                        .Where(filePath => filePath is not null)
                        .Select(filePath => Path.Combine(Root, filePath!))
                        .ToArray();
                }
                else
                {
                    if (!TryGetFirstFile(fileSource, out var filePath))
                        continue;

                    filePaths = [filePath];
                }

                cancellationToken.ThrowIfCancellationRequested();

                foreach (var filePath in filePaths)
                {
                    if (string.IsNullOrWhiteSpace(filePath))
                        continue;

                    using var file = new StreamReader(File.OpenRead(filePath));

                    var samplePeriod = fileSource.AdditionalSettings.SamplePeriod;

                    var resources = mode == "real_time"
                        ? GetRawResources(file, instrument, samplePeriod, fileSourceId)
                        : GetAverageResources(file, instrument, samplePeriod, fileSourceId);

                    var duplicateKeys = resources.GroupBy(x => x.Id)
                        .Where(group => group.Count() > 1)
                        .Select(group => group.Key);

                    var newCatalog = new ResourceCatalogBuilder(id: catalog.Id)
                        .AddResources(resources)
                        .Build();

                    catalog = catalog.Merge(newCatalog);
                }
            }
        }

        return Task.FromResult(catalog);
    }

    protected override Task ReadAsync(
        ReadInfo<AdditionalFileSourceSettings> info, 
        ReadRequest[] readRequests, CancellationToken cancellationToken
    )
    {
        return Task.Run(async () =>
        {
            foreach (var readRequest in readRequests)
            {
                var properties = readRequest.CatalogItem.Resource.Properties ?? throw new Exception("properties is null");
                var groups = properties.GetStringArray("groups")!;

                if (groups.Any(group => group!.Contains("avg")))
                    await ReadSingleAverageAsync(info, readRequest, cancellationToken);

                else
                    await ReadSingleRawAsync(info, readRequest, cancellationToken);
            }
        }, cancellationToken);
    }

    private Task ReadSingleAverageAsync(
        ReadInfo<AdditionalFileSourceSettings> info, 
        ReadRequest readRequest, 
        CancellationToken cancellationToken
    )
    {
        return Task.Run(() =>
        {
            var resourceIdParts = readRequest.CatalogItem.Resource.Id.Split('_', 2);
            var instrument = resourceIdParts[0];
            var resourceName = resourceIdParts[1];

            if (readRequest.CatalogItem.Parameters is not null &&
                readRequest.CatalogItem.Parameters.TryGetValue("d", out var distanceString) &&
                int.TryParse(distanceString, out var distance))
            {
                // do nothing
            }

            else
            {
                throw new Exception("The distance parameter is required.");
            }

            var lines = File.ReadAllLines(info.FilePath);
            (var columns, var distances) = GetAverageFileParameters(lines);

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
                    var offset = (int)info.FileOffset * readRequest.CatalogItem.Representation.ElementSize;

                    byteResult[offset..]
                        .CopyTo(readRequest.Data.Span);

                    readRequest
                        .Status
                        .Span
                        .Fill(1);
                }
                else
                {
                    Logger.LogDebug("The actual buffer size does not match the expected size, which indicates an incomplete file");
                }
            }
        }, cancellationToken);
    }

    private Task ReadSingleRawAsync(
        ReadInfo<AdditionalFileSourceSettings> info, 
        ReadRequest readRequest, 
        CancellationToken cancellationToken
    )
    {
        return Task.Run(() =>
        {
            var resourceIdParts = readRequest.CatalogItem.Resource.Id.Split('_', 3);
            var instrument = resourceIdParts[0];
            var beam = int.Parse(resourceIdParts[1]);
            var resourceName = resourceIdParts[2];

            if (readRequest.CatalogItem.Parameters is not null &&
                readRequest.CatalogItem.Parameters.TryGetValue("d", out var distanceString) &&
                int.TryParse(distanceString, out var distance))
            {
                // do nothing
            }
            else
            {
                throw new Exception("The distance parameter is required.");
            }

            var lines = File.ReadAllLines(info.FilePath);
            (var columns, var distances, var firstBeam) = GetRawFileParameters(lines);

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

                // the files have two empty line at the end ... except when they are exported
                // by the Leosphere software, then there is just one line -.-
                var requiredLineCount = info.FileLength * linesToSkip;

                if (lines.Length >= requiredLineCount && !string.IsNullOrWhiteSpace(lines[requiredLineCount - 1]))
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
                    var offset = (int)info.FileOffset * readRequest.CatalogItem.Representation.ElementSize;

                    byteResult[offset..]
                        .CopyTo(readRequest.Data.Span);

                    readRequest
                        .Status
                        .Span
                        .Fill(1);
                }
                else
                {
                    Logger.LogDebug("The actual buffer size does not match the expected size, which indicates an incomplete file");
                }
            }
            else
            {
                Logger.LogDebug("Distance {Distance} does not exist", distance);
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
            if (!TryEnforceNamingConvention(value, out var name))
                throw new Exception($"The name {value} is not a valid resource id.");

            return name;
        }).ToList();

        // distances
        var dataLines = lines.Skip(1);
        var distances = new List<int>();

        foreach (var line in dataLines)
        {
            var dataParts = line.Split(';', 4);
            var distance = (int)double.Parse(dataParts[1], _nfi);

            if (distances.Count == 0 || distance > distances[0])
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
            if (!TryEnforceNamingConvention(value, out var name))
                throw new Exception($"The name {value} is not a valid resource id.");

            return name;
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

            if (distances.Count == 0 || distance > distances[0])
                distances.Add(distance);

            else
                break;
        }

        return (columns, distances, firstBeam);
    }

    private const string PARAMETER = 
$$"""
{
  "type": "input-integer",
  "label": "Distance / m",
  "default": 0,
  "minimum": 0,
  "maximum": 10000
}
""";

    private static List<Resource> GetAverageResources(
        StreamReader file,
        string instrument,
        TimeSpan samplePeriod,
        string fileSourceId)
    {
        var line = file.ReadLine() ?? throw new Exception("line is null");
        var parameters = new Dictionary<string, JsonElement>
        {
            ["d"] = JsonSerializer.Deserialize<JsonElement>(PARAMETER)
        };

        return line.Split(';')
            .Skip(1)
            .SelectMany(originalName =>
            {
                if (!TryEnforceNamingConvention(originalName, out var name))
                    throw new Exception($"The name {originalName} is not a valid resource id.");

                var resources = new List<Resource>();

                var representation = new Representation(
                    dataType: NexusDataType.FLOAT64,
                    samplePeriod,
                    parameters);

                var resourceId = $"{instrument}_{name}";

                var resource = new ResourceBuilder(id: resourceId)
                    .WithGroups($"{instrument} (avg)")
                    .WithFileSourceId(fileSourceId)
                    .WithOriginalName(originalName)
                    .AddRepresentation(representation)
                    .Build();

                resources.Add(resource);

                return resources;
            }).ToList();
    }

    private static List<Resource> GetRawResources(
        StreamReader file,
        string instrument,
        TimeSpan samplePeriod,
        string fileSourceId)
    {
        var line = file.ReadLine() ?? throw new Exception("line is null");
        var parameters = new Dictionary<string, JsonElement>
        {
            ["d"] = JsonSerializer.Deserialize<JsonElement>(PARAMETER)
        };

        return line.Split(';')
            .Skip(1)
            .SelectMany(originalName =>
            {
                if (!TryEnforceNamingConvention(originalName, out var name))
                    throw new Exception($"The name {originalName} is not a valid resource id.");

                var resources = new List<Resource>();

                for (int i = 0; i < 4; i++)
                {
                    var representation = new Representation(
                        dataType: NexusDataType.FLOAT64,
                        samplePeriod,
                        parameters);

                    var resourceId = $"{instrument}_{(i + 3) % 4}_{name}";

                    var resource = new ResourceBuilder(id: resourceId)
                        .WithGroups($"{instrument}")
                        .WithFileSourceId(fileSourceId)
                        .WithOriginalName(originalName)
                        .AddRepresentation(representation)
                        .Build();

                    resources.Add(resource);
                }

                return resources;
            }).ToList();
    }

    private static bool TryEnforceNamingConvention(string resourceId, [NotNullWhen(returnValue: true)] out string newResourceId)
    {
        newResourceId = resourceId;
        newResourceId = Resource.InvalidIdCharsExpression.Replace(newResourceId, "_");
        newResourceId = Resource.InvalidIdStartCharsExpression.Replace(newResourceId, "_");
        newResourceId = newResourceId.Replace("__", "_").Trim('_');

        return Resource.ValidIdExpression.IsMatch(newResourceId);
    }
}

#pragma warning restore CS1591 // Missing XML comment for publicly visible type or member