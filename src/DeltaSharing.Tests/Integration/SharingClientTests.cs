namespace DeltaSharing.Tests.Integration;

public class SharingClientTests : IClassFixture<RestClientFixture>
{
    private readonly SharingClient client;

    public SharingClientTests(RestClientFixture restClientFixture)
    {
        this.client = new SharingClient(restClientFixture.HttpClient, restClientFixture.Client);
    }

    [Fact]
    public async void TestCanListShares()
    {
        var shares = await this.client.ListSharesAsync().ToArrayAsync();
        Assert.NotEmpty(shares);
    }


    [Fact]
    public async void TestCanQueryTable()
    {
        var tableReader = await this.client.QueryTableAsync("delta_sharing", "default", "nyctaxi_2019");
        Assert.NotNull(tableReader);
    }

}