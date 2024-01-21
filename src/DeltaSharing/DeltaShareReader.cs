using System.Runtime.CompilerServices;
using DeltaSharing.Rest;
using Parquet.Serialization;

namespace DeltaSharing;

public sealed class DeltaShareReader
{
    private readonly TableQueryResponse tableQueryResponse;
    private readonly HttpClient httpClient;

    internal DeltaShareReader(HttpClient httpClient, TableQueryResponse tableQueryResponse)
    {
        this.tableQueryResponse = tableQueryResponse;
        this.httpClient = httpClient;
    }

    public async IAsyncEnumerable<T> ReadTableAsync<T>([EnumeratorCancellation] CancellationToken cancellationToken = default)
        where T : new()
    {
        foreach (var table in this.tableQueryResponse.Files)
        {
            using var ms = await this.ReadFile(table.Url);
            var data = ParquetSerializer.DeserializeAllAsync<T>(ms, cancellationToken: cancellationToken);

            await foreach (var row in data)
            {
                yield return row;
            }
        }
    }

    private async Task<MemoryStream> ReadFile(string url)
    {
        var ms = new MemoryStream();
        using var responseStream = await this.httpClient.GetStreamAsync(url);
        await responseStream.CopyToAsync(ms);
        return ms;
    }
}