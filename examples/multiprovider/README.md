# Multiprovider

This example shows how to use iroh-blobs to download concurrently from multiple
providers. As of now, the infrastructure to do this is included in the example.
It will move into the main crate soon.

## Usage

This example requires the `rpc` feature, so is is easiest to run it with
`--all-features`. Also, if you want try it out with large blobs such as ML
models, it is best to run in release mode.

There are two subcommands, `provide` and `download`.

### Provide

Provide provides a *single* blob, printing the blob hash and the node id.

### Download

Download downloads a *single* hash from any number of node ids.

In the long
term we are going to have content discovery based on trackers or other mechanisms,
but for this example you just have to provide the node ids in the command line.

To have a stable node id, it is
possible to provide the iroh node secret in an environment variable.

**This is fine for an example, but don't do it in production**

## Trying it out

Multiprovider downloads are mostly relevant for very large downloads, so let's
use a large file, a ~4GB ML model.

Terminal 1:

```
> IROH_SECRET=<secret1> \
    cargo run --release --all-features --example multiprovider \
    provide ~/.ollama/models/blobs/sha256-96c415656d377afbff962f6cdb2394ab092ccbcbaab4b82525bc4ca800fe8a49
added /Users/rklaehn/.ollama/models/blobs/sha256-96c415656d377afbff962f6cdb2394ab092ccbcbaab4b82525bc4ca800fe8a49 as e5njueepdum3ks2usqdxw3ofztj63jgedtnfak34smgvw5b6cr3a, 4683073184 bytes, 4573314 chunks
listening on 28300fcb69830c3e094c68f383ffd568dd9aa9126a6aa537c3dcfec077b60af9
```

Terminal 2:

```
❯ IROH_SECRET=<secret2> \
    cargo run --release --all-features --example multiprovider \
    provide ~/.ollama/models/blobs/sha256-96c415656d377afbff962f6cdb2394ab092ccbcbaab4b82525bc4ca800fe8a49
added /Users/rklaehn/.ollama/models/blobs/sha256-96c415656d377afbff962f6cdb2394ab092ccbcbaab4b82525bc4ca800fe8a49 as e5njueepdum3ks2usqdxw3ofztj63jgedtnfak34smgvw5b6cr3a, 4683073184 bytes, 4573314 chunks
listening on 77d81595422c0a757b9e3f739f9a67eab9646f13d941654e9074982c5c800a5a
```

So now we got 2 node ids,
`77d81595422c0a757b9e3f739f9a67eab9646f13d941654e9074982c5c800a5a` and
`28300fcb69830c3e094c68f383ffd568dd9aa9126a6aa537c3dcfec077b60af9`, providing
the data.

Note that the provide side is not in any way special. It is just using the
existing iroh-blobs protocol, so any other iroh node could be used as well.

For downloading, we don't need a stable node id, so we don't need to bother with
setting IROH_SECRET.

```
> cargo run --release --all-features --example multiprovider \
    download e5njueepdum3ks2usqdxw3ofztj63jgedtnfak34smgvw5b6cr3a \
        28300fcb69830c3e094c68f383ffd568dd9aa9126a6aa537c3dcfec077b60af9 \
        77d81595422c0a757b9e3f739f9a67eab9646f13d941654e9074982c5c800a5a

peer discovered for hash e5njueepdum3ks2usqdxw3ofztj63jgedtnfak34smgvw5b6cr3a: 28300fcb69830c3e094c68f383ffd568dd9aa9126a6aa537c3dcfec077b60af9
peer discovered for hash e5njueepdum3ks2usqdxw3ofztj63jgedtnfak34smgvw5b6cr3a: 77d81595422c0a757b9e3f739f9a67eab9646f13d941654e9074982c5c800a5a
█████████▓   ░█████████░                                                                                                                                                                                                                         
```

The download side will initially download from the first peer, then quickly
rebalance the download as a new peer becomes available. It will currently
download from each peer in "stripes".

When running without `--path` argument it will download into a memory store.
When providing a `--path` argument it will download into a persitent store at the
given path, and the download will resume if you interrupt the download process.

## Notes on the current state

The current state of the downloader is highly experimental. While peers that don't
respond at all are handled properly, peers that are slow or become slow over time
are not properly punished. Also, there is not yet a mechanism to limit the number
of peers to download from.