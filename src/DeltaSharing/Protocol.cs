using System.Text.Json;

namespace DeltaSharing;

public sealed record Schema(string Name, string Share);

public sealed record Share(string Name, string Id);

public sealed record Table(string Name,
    string Schema,
    string Share,
    string ShareId,
    string Id);

public sealed record FileAction(string Url,
    string Id,
    Dictionary<string, string> PartitionValues,
    long Size,
    long Version,
    long Timestamp);
    

public sealed record Format(string Provider);
public sealed record Metadata(string Id,
    string Name,
    string Description,
    Format Format,
    string SchemaString,
    string[] PartitionColumns,
    Dictionary<string, string> Configuration,
    long Version,
    long Size,
    long NumFiles);

public sealed record Protocol(int MinReaderVersion);

public sealed record Profile(string Endpoint, string BearerToken, DateTime? ExpirationTime)
{
    public static async Task<Profile> FromFile(string profilePath)
    {
        var fileText = await File.ReadAllTextAsync(profilePath);
        var options = new JsonSerializerOptions(JsonSerializerDefaults.Web);
        return JsonSerializer.Deserialize<Profile>(fileText, options) ?? throw new InvalidOperationException("Not a valid profile file.");
    }
}