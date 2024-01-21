using DeltaSharing.Rest;

namespace DeltaSharing.Tests.Integration;

public class RestClientFixture : IDisposable
{
    public RestClientFixture()
    {
        this.HttpClient = new HttpClient();
        this.Client = new RestClient(this.HttpClient, "https://sharing.delta.io/delta-sharing", "faaie590d541265bcab1f2de9813274bf233");
    }

    public HttpClient HttpClient { get; }

    internal RestClient Client { get; }

    public void Dispose()
    {
        this.HttpClient.Dispose();
    }
}