namespace DeltaSharing.Rest;

internal interface IRestClient
{
    Task<PagedResponse<Share>?> ListSharesAsync(string? nextPageToken = null, CancellationToken cancellationToken = default);
    Task<PagedResponse<Schema>?> ListSchemasAsync(string share, string? nextPageToken = null, CancellationToken cancellationToken = default);
    Task<Share?> GetShareAsync(string share, CancellationToken cancellationToken = default);
    Task<PagedResponse<Table>?> ListTablesAsync(string share, string schema, string? nextPageToken = null, CancellationToken cancellationToken = default);
    Task<long?> GetTableVersionAsync(string share, string schema, string table, CancellationToken cancellationToken = default);
    Task<MetadataResponse?> GetTableMetadataAsync(string share, string schema, string table, CancellationToken cancellationToken = default);
    Task<TableQueryResponse?> QueryTableAsync(string share, string schema, string table, QueryRequest queryRequest, CancellationToken cancellationToken = default);
    // Uncomment and implement the following method if needed
    // Task<ChangeDataFeedResponse?> GetTableChangesAsync(string share, string schema, string table, DateTime? startingTimestamp, int? startingVersion, int? endingVersion, DateTime? endingTimestamp, bool? includeHistoricalMetadata);
}