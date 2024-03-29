Delta Sharing Client for .NET
=============================

Basic Usage
```csharp
using DeltaSharing;

using var httpClient = new HttpClient();
var sharingClient = SharingClient.CreateClient(httpClient, "https://sharing.delta.io/delta-sharing", "faaie590d541265bcab1f2de9813274bf233");
var shares = sharingClient.ListSharesAsync();
await foreach (var share in shares)
{
    Console.WriteLine(share.Name);
}
```

Reading a table
```csharp
class COVID19NYT
{
    public string date { get; set; }
}

...

var tableReader = await sharing.QueryTableAsync("delta_sharing", "default", "COVID_19_NYT", limit: 1);
await foreach (var row in tableReader.ReadTableAsync<COVID19NYT>())
{
    Console.WriteLine(row.date);
}

```

Publishing Fit & Finish
=======================
Checklist for fit & finish before publishing to Nuget

- [ ] Implement missing metadata and protocol methods on `SharingClient`
- [ ] Add `SharingClient` interface
- [ ] Document all public facing methods
- [ ] Add in unit tests to test basic functionality
- [ ] Build Github CI/CD to test and publish package
- [ ] Add in example usage project