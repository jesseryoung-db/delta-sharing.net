using System.Runtime.CompilerServices;

using DeltaSharing.Rest;
namespace DeltaSharing;

public sealed class SharingClient
{
    private readonly HttpClient httpClient;
    private readonly IRestClient restClient;

    public static SharingClient CreateClient(HttpClient httpClient, Profile profile) => new SharingClient(httpClient, new RestClient(httpClient, profile));

    public static SharingClient CreateClient(HttpClient httpClient, string endpoint, string bearerToken) => new SharingClient(httpClient, new RestClient(httpClient, new Profile(endpoint, bearerToken, null)));

    internal SharingClient(HttpClient httpClient, IRestClient restClient)
    {
        this.httpClient = httpClient;
        this.restClient = restClient;
    }

    public async IAsyncEnumerable<string> ListSharesAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var shares = await this.restClient.ListSharesAsync(cancellationToken: cancellationToken);

        while (shares != null)
        {
            foreach (var share in shares.Items)
            {
                yield return share.Name;
            }
            if (shares.NextPageToken == null)
            {
                yield break;
            }

            shares = await this.restClient.ListSharesAsync(nextPageToken: shares.NextPageToken, cancellationToken: cancellationToken);
        }
    }

    public async IAsyncEnumerable<string> ListSchemasAsync(string share, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var schemas = await this.restClient.ListSchemasAsync(share, cancellationToken: cancellationToken);

        while (schemas != null)
        {
            foreach (var schema in schemas.Items)
            {
                yield return schema.Name;
            }
            if (schemas.NextPageToken == null)
            {
                yield break;
            }

            schemas = await this.restClient.ListSchemasAsync(share, nextPageToken: schemas.NextPageToken, cancellationToken: cancellationToken);
        }
    }

    public async IAsyncEnumerable<string> ListTablesAsync(string share, string schema, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var tables = await this.restClient.ListTablesAsync(share, schema, cancellationToken: cancellationToken);

        while (tables != null)
        {
            foreach (var table in tables.Items)
            {
                yield return table.Name;
            }
            if (tables.NextPageToken == null)
            {
                yield break;
            }

            tables = await this.restClient.ListTablesAsync(share, schema, nextPageToken: tables.NextPageToken, cancellationToken: cancellationToken);
        }
    }

    public async Task<DeltaShareReader> QueryTableAsync(string share, string schema, string table, int? limit = null, int? version = null, DateTime? timestamp = null, CancellationToken cancellationToken = default)
    {
        // TODO: Handle null responses
        var queryRequest = new QueryRequest(LimitHint: limit, Version: version, Timestamp: timestamp);
        var tableResponse = await this.restClient.QueryTableAsync(share, schema, table, queryRequest, cancellationToken: cancellationToken);

        return new DeltaShareReader(this.httpClient, tableResponse!);
    }

}