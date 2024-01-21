using DeltaSharing.Rest;
namespace DeltaSharing.Tests.Integration;

public class RestClientTests : IClassFixture<RestClientFixture>
{
    private readonly RestClient client;

    public RestClientTests(RestClientFixture fixture)
    {
        this.client = fixture.Client;
    }

    [Fact]
    public async Task TestCanListShares()
    {

        var shares = await this.client.ListSharesAsync();
        Assert.NotNull(shares);
        Assert.NotEmpty(shares.Items);
    }

    [Fact]
    public async Task TestCanListSchemas()
    {
        var schemas = await this.client.ListSchemasAsync("delta_sharing");
        Assert.NotNull(schemas);
        Assert.NotEmpty(schemas.Items);
    }

    [Fact]
    public async Task TestCanListTables()
    {
        var tables = await this.client.ListTablesAsync("delta_sharing", "default");
        Assert.NotNull(tables);
        Assert.NotEmpty(tables.Items);
    }

    [Fact]
    public async Task TestCanQueryTable()
    {
        var queryResponse = await this.client.QueryTableAsync("delta_sharing", "default", "nyctaxi_2019", new());
        Assert.NotNull(queryResponse);
    }
}