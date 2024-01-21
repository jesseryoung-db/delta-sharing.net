using DeltaSharing.Rest;

namespace DeltaSharing.Tests.Integration;

class COVID19NYT
{
    public string date { get; set; }
}

public class DeltaReaderTests : IClassFixture<RestClientFixture>
{
    private readonly SharingClient client;

    public DeltaReaderTests(RestClientFixture restClientFixture)
    {
        this.client = new SharingClient(restClientFixture.HttpClient, restClientFixture.Client);
    }

    [Fact]
    public async void TestCanReadTable()
    {
        var tableReader = await this.client.QueryTableAsync("delta_sharing", "default", "COVID_19_NYT", limit: 1);

        var results = await tableReader.ReadTableAsync<COVID19NYT>().ToArrayAsync();

        Assert.NotEmpty(results);
    }
}