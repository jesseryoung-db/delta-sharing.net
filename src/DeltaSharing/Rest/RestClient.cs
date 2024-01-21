using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
namespace DeltaSharing.Rest;

internal class RestClient : IRestClient
{
    private readonly Profile profile;
    private readonly JsonSerializerOptions serializerOptions;

    private readonly HttpClient httpClient;

    public RestClient(HttpClient httpClient, Profile profile)
    {
        this.profile = profile;
        this.serializerOptions = new JsonSerializerOptions(JsonSerializerDefaults.Web);
        this.httpClient = httpClient;
    }
    public RestClient(HttpClient httpClient, string endpoint, string bearerToken) : this(httpClient, new(endpoint, bearerToken, null)) { }

    public async Task<PagedResponse<Share>?> ListSharesAsync(string? nextPageToken = null, CancellationToken cancellationToken = default)
    {

        var requestUrl = "shares";
        if (nextPageToken != null)
        {
            requestUrl += $"?{WebUtility.UrlEncode(nextPageToken)}";
        }

        return await this.Get<PagedResponse<Share>>(requestUrl, cancellationToken);
    }
    public async Task<PagedResponse<Schema>?> ListSchemasAsync(string share, string? nextPageToken = null, CancellationToken cancellationToken = default)
    {
        var requestUrl = $"shares/{share}/schemas";
        if (nextPageToken != null)
        {
            requestUrl += $"?{WebUtility.UrlEncode(nextPageToken)}";
        }
        return await this.Get<PagedResponse<Schema>>(requestUrl, cancellationToken);
    }

    public async Task<Share?> GetShareAsync(string share, CancellationToken cancellationToken = default)
    {
        return await this.Get<Share>($"shares/{share}", cancellationToken);
    }

    public async Task<PagedResponse<Table>?> ListTablesAsync(string share, string schema, string? nextPageToken = null, CancellationToken cancellationToken = default)
    {
        var requestUrl = $"shares/{share}/schemas/{schema}/tables";
        if (nextPageToken != null)
        {
            requestUrl += $"?{WebUtility.UrlEncode(nextPageToken)}";
        }
        return await this.Get<PagedResponse<Table>>(requestUrl, cancellationToken);
    }

    public async Task<long?> GetTableVersionAsync(string share, string schema, string table, CancellationToken cancellationToken = default)
    {
        using var response = await this.Get($"shares/{share}/schemas/{schema}/tables/{table}/version", cancellationToken);

        var headers = response.Headers;
        if (headers.Contains("Delta-Table-Version"))
        {
            return long.Parse(headers.GetValues("Delta-Table-Version").First());
        }
        else
        {
            return null;
        }
    }

    public async Task<MetadataResponse?> GetTableMetadataAsync(string share, string schema, string table, CancellationToken cancellationToken = default)
    {
        return await this.Get<MetadataResponse>($"shares/{share}/schemas/{schema}/tables/{table}/metadata", cancellationToken);
    }

    public async Task<TableQueryResponse?> QueryTableAsync(string share, string schema, string table, QueryRequest queryRequest, CancellationToken cancellationToken = default)
    {
        // TODO: Exception handling
        using var queryResponse = await this.Post($"shares/{share}/schemas/{schema}/tables/{table}/query", queryRequest, cancellationToken);

        using (var queryResponseStream = await queryResponse.Content.ReadAsStreamAsync(cancellationToken))
        using (var queryResponseStreamReader = new StreamReader(queryResponseStream))
        {
            var protocolJson = await queryResponseStreamReader.ReadLineAsync(cancellationToken);
            var metadataJson = await queryResponseStreamReader.ReadLineAsync(cancellationToken);
            var protocol = JsonSerializer.Deserialize<ProtocolResponse>(protocolJson!, this.serializerOptions);
            var metadata = JsonSerializer.Deserialize<MetadataResponse>(metadataJson!, this.serializerOptions);

            var files = new List<FileAction>();
            while (!queryResponseStreamReader.EndOfStream)
            {
                var fileJson = await queryResponseStreamReader.ReadLineAsync(cancellationToken);
                files.Add(JsonSerializer.Deserialize<FileResponse>(fileJson!, this.serializerOptions)!.File);
            }

            return new(protocol!.Protocol, metadata!.MetaData, files.ToArray());
        }
    }

    private async Task<HttpResponseMessage> Get(string requestUri, CancellationToken cancellationToken)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, $"{this.profile.Endpoint}/{requestUri}");
        request.Headers.Authorization = new("Bearer", this.profile.BearerToken);
        var response = await this.httpClient.SendAsync(request, cancellationToken);
        response.EnsureSuccessStatusCode();
        return response;
    }

    private async Task<T?> Get<T>(string requestUri, CancellationToken cancellationToken)
    {
        using var response = await this.Get(requestUri, cancellationToken);
        return await response.Content.ReadFromJsonAsync<T>(cancellationToken);
    }

    private async Task<HttpResponseMessage> Post<T>(string requestUri, T payload, CancellationToken cancellationToken)
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, $"{this.profile.Endpoint}/{requestUri}");
        request.Content = JsonContent.Create(payload);
        request.Headers.Authorization = new("Bearer", this.profile.BearerToken);
        var response = await this.httpClient.SendAsync(request, cancellationToken);
        response.EnsureSuccessStatusCode();

        return response;
    }


    // public async Task<ChangeDataFeedResponse?> GetTableChangesAsync(
    //     string share,
    //     string schema,
    //     string table,
    //     DateTime? startingTimestamp,
    //     int? startingVersion,
    //     int? endingVersion,
    //     DateTime? endingTimestamp,
    //     bool? includeHistoricalMetadata)
    // {
    //     var queryParams = new List<string>();

    //     if (startingTimestamp.HasValue)
    //     {
    //         queryParams.Add($"startingTimestamp={WebUtility.UrlEncode(startingTimestamp.Value.ToString("o"))}");
    //     }
    //     if (startingVersion.HasValue)
    //     {
    //         queryParams.Add($"startingVersion={startingVersion.Value}");
    //     }
    //     if (endingVersion.HasValue)
    //     {
    //         queryParams.Add($"endingVersion={endingVersion.Value}");
    //     }
    //     if (endingTimestamp.HasValue)
    //     {
    //         queryParams.Add($"endingTimestamp={WebUtility.UrlEncode(endingTimestamp.Value.ToString("o"))}");
    //     }
    //     if (includeHistoricalMetadata.HasValue)
    //     {
    //         queryParams.Add($"includeHistoricalMetadata={includeHistoricalMetadata.Value}");
    //     }

    //     var queryString = string.Join("&", queryParams);
    //     var url = $"shares/{share}/schemas/{schema}/tables/{table}/changes?{queryString}";

    //     return await httpClient.GetFromJsonAsync<ChangeDataFeedResponse>(url);
    // }


}




record PagedResponse<T>(T[] Items, string NextPageToken);
record FileResponse(FileAction File);
record MetadataResponse(Metadata MetaData);
record ProtocolResponse(Protocol Protocol);

record QueryRequest(JsonPredicateHints? JsonPredicateHints = null,
    int? LimitHint = null,
    int? Version = null,
    DateTime? Timestamp = null,
    int? StartingVersion = null,
    int? EndingVersion = null);

record JsonPredicateHints(
    string Op,
    JsonPredicateHints[]? Children,
    string Name,
    string Value,
    string ValueType);


record TableQueryResponse(Protocol Protocol, Metadata Metadata, FileAction[] Files);