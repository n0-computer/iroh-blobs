(function() {
    var type_impls = Object.fromEntries([["iroh_blobs",[["<details class=\"toggle implementors-toggle\" open><summary><section id=\"impl-Client%3CC%3E\" class=\"impl\"><a class=\"src rightside\" href=\"src/iroh_blobs/rpc/client/blobs.rs.html#117-469\">Source</a><a href=\"#impl-Client%3CC%3E\" class=\"anchor\">§</a><h3 class=\"code-header\">impl&lt;C&gt; <a class=\"struct\" href=\"iroh_blobs/rpc/client/blobs/struct.Client.html\" title=\"struct iroh_blobs::rpc::client::blobs::Client\">Client</a>&lt;C&gt;<div class=\"where\">where\n    C: Connector&lt;<a class=\"struct\" href=\"iroh_blobs/rpc/proto/struct.RpcService.html\" title=\"struct iroh_blobs::rpc::proto::RpcService\">RpcService</a>&gt;,</div></h3></section></summary><div class=\"impl-items\"><details class=\"toggle method-toggle\" open><summary><section id=\"method.new\" class=\"method\"><a class=\"src rightside\" href=\"src/iroh_blobs/rpc/client/blobs.rs.html#122-124\">Source</a><h4 class=\"code-header\">pub fn <a href=\"iroh_blobs/rpc/client/blobs/struct.Client.html#tymethod.new\" class=\"fn\">new</a>(rpc: RpcClient&lt;<a class=\"struct\" href=\"iroh_blobs/rpc/proto/struct.RpcService.html\" title=\"struct iroh_blobs::rpc::proto::RpcService\">RpcService</a>, C&gt;) -&gt; Self</h4></section></summary><div class=\"docblock\"><p>Create a new client</p>\n</div></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.tags\" class=\"method\"><a class=\"src rightside\" href=\"src/iroh_blobs/rpc/client/blobs.rs.html#127-129\">Source</a><h4 class=\"code-header\">pub fn <a href=\"iroh_blobs/rpc/client/blobs/struct.Client.html#tymethod.tags\" class=\"fn\">tags</a>(&amp;self) -&gt; <a class=\"struct\" href=\"iroh_blobs/rpc/client/tags/struct.Client.html\" title=\"struct iroh_blobs::rpc::client::tags::Client\">Client</a>&lt;C&gt;</h4></section></summary><div class=\"docblock\"><p>Get a tags client.</p>\n</div></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.status\" class=\"method\"><a class=\"src rightside\" href=\"src/iroh_blobs/rpc/client/blobs.rs.html#135-138\">Source</a><h4 class=\"code-header\">pub async fn <a href=\"iroh_blobs/rpc/client/blobs/struct.Client.html#tymethod.status\" class=\"fn\">status</a>(&amp;self, hash: <a class=\"struct\" href=\"iroh_blobs/struct.Hash.html\" title=\"struct iroh_blobs::Hash\">Hash</a>) -&gt; <a class=\"type\" href=\"https://docs.rs/anyhow/1.0.95/anyhow/type.Result.html\" title=\"type anyhow::Result\">Result</a>&lt;<a class=\"enum\" href=\"iroh_blobs/rpc/client/blobs/enum.BlobStatus.html\" title=\"enum iroh_blobs::rpc::client::blobs::BlobStatus\">BlobStatus</a>&gt;</h4></section></summary><div class=\"docblock\"><p>Check if a blob is completely stored on the node.</p>\n<p>Note that this will return false for blobs that are partially stored on\nthe node.</p>\n</div></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.has\" class=\"method\"><a class=\"src rightside\" href=\"src/iroh_blobs/rpc/client/blobs.rs.html#143-149\">Source</a><h4 class=\"code-header\">pub async fn <a href=\"iroh_blobs/rpc/client/blobs/struct.Client.html#tymethod.has\" class=\"fn\">has</a>(&amp;self, hash: <a class=\"struct\" href=\"iroh_blobs/struct.Hash.html\" title=\"struct iroh_blobs::Hash\">Hash</a>) -&gt; <a class=\"type\" href=\"https://docs.rs/anyhow/1.0.95/anyhow/type.Result.html\" title=\"type anyhow::Result\">Result</a>&lt;<a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.bool.html\">bool</a>&gt;</h4></section></summary><div class=\"docblock\"><p>Check if a blob is completely stored on the node.</p>\n<p>This is just a convenience wrapper around <code>status</code> that returns a boolean.</p>\n</div></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.batch\" class=\"method\"><a class=\"src rightside\" href=\"src/iroh_blobs/rpc/client/blobs.rs.html#156-161\">Source</a><h4 class=\"code-header\">pub async fn <a href=\"iroh_blobs/rpc/client/blobs/struct.Client.html#tymethod.batch\" class=\"fn\">batch</a>(&amp;self) -&gt; <a class=\"type\" href=\"https://docs.rs/anyhow/1.0.95/anyhow/type.Result.html\" title=\"type anyhow::Result\">Result</a>&lt;<a class=\"struct\" href=\"iroh_blobs/rpc/client/blobs/struct.Batch.html\" title=\"struct iroh_blobs::rpc::client::blobs::Batch\">Batch</a>&lt;C&gt;&gt;</h4></section></summary><div class=\"docblock\"><p>Create a new batch for adding data.</p>\n<p>A batch is a context in which temp tags are created and data is added to the node. Temp tags\nare automatically deleted when the batch is dropped, leading to the data being garbage collected\nunless a permanent tag is created for it.</p>\n</div></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.read\" class=\"method\"><a class=\"src rightside\" href=\"src/iroh_blobs/rpc/client/blobs.rs.html#166-168\">Source</a><h4 class=\"code-header\">pub async fn <a href=\"iroh_blobs/rpc/client/blobs/struct.Client.html#tymethod.read\" class=\"fn\">read</a>(&amp;self, hash: <a class=\"struct\" href=\"iroh_blobs/struct.Hash.html\" title=\"struct iroh_blobs::Hash\">Hash</a>) -&gt; <a class=\"type\" href=\"https://docs.rs/anyhow/1.0.95/anyhow/type.Result.html\" title=\"type anyhow::Result\">Result</a>&lt;<a class=\"struct\" href=\"iroh_blobs/rpc/client/blobs/struct.Reader.html\" title=\"struct iroh_blobs::rpc::client::blobs::Reader\">Reader</a>&gt;</h4></section></summary><div class=\"docblock\"><p>Stream the contents of a a single blob.</p>\n<p>Returns a <a href=\"iroh_blobs/rpc/client/blobs/struct.Reader.html\" title=\"struct iroh_blobs::rpc::client::blobs::Reader\"><code>Reader</code></a>, which can report the size of the blob before reading it.</p>\n</div></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.read_at\" class=\"method\"><a class=\"src rightside\" href=\"src/iroh_blobs/rpc/client/blobs.rs.html#173-175\">Source</a><h4 class=\"code-header\">pub async fn <a href=\"iroh_blobs/rpc/client/blobs/struct.Client.html#tymethod.read_at\" class=\"fn\">read_at</a>(\n    &amp;self,\n    hash: <a class=\"struct\" href=\"iroh_blobs/struct.Hash.html\" title=\"struct iroh_blobs::Hash\">Hash</a>,\n    offset: <a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.u64.html\">u64</a>,\n    len: <a class=\"enum\" href=\"iroh_blobs/rpc/client/blobs/enum.ReadAtLen.html\" title=\"enum iroh_blobs::rpc::client::blobs::ReadAtLen\">ReadAtLen</a>,\n) -&gt; <a class=\"type\" href=\"https://docs.rs/anyhow/1.0.95/anyhow/type.Result.html\" title=\"type anyhow::Result\">Result</a>&lt;<a class=\"struct\" href=\"iroh_blobs/rpc/client/blobs/struct.Reader.html\" title=\"struct iroh_blobs::rpc::client::blobs::Reader\">Reader</a>&gt;</h4></section></summary><div class=\"docblock\"><p>Read offset + len from a single blob.</p>\n<p>If <code>len</code> is <code>None</code> it will read the full blob.</p>\n</div></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.read_to_bytes\" class=\"method\"><a class=\"src rightside\" href=\"src/iroh_blobs/rpc/client/blobs.rs.html#182-187\">Source</a><h4 class=\"code-header\">pub async fn <a href=\"iroh_blobs/rpc/client/blobs/struct.Client.html#tymethod.read_to_bytes\" class=\"fn\">read_to_bytes</a>(&amp;self, hash: <a class=\"struct\" href=\"iroh_blobs/struct.Hash.html\" title=\"struct iroh_blobs::Hash\">Hash</a>) -&gt; <a class=\"type\" href=\"https://docs.rs/anyhow/1.0.95/anyhow/type.Result.html\" title=\"type anyhow::Result\">Result</a>&lt;Bytes&gt;</h4></section></summary><div class=\"docblock\"><p>Read all bytes of single blob.</p>\n<p>This allocates a buffer for the full blob. Use only if you know that the blob you’re\nreading is small. If not sure, use <a href=\"iroh_blobs/rpc/client/blobs/struct.Client.html#method.read\" title=\"method iroh_blobs::rpc::client::blobs::Client::read\"><code>Self::read</code></a> and check the size with\n<a href=\"iroh_blobs/rpc/client/blobs/struct.Reader.html#method.size\" title=\"method iroh_blobs::rpc::client::blobs::Reader::size\"><code>Reader::size</code></a> before calling <a href=\"iroh_blobs/rpc/client/blobs/struct.Reader.html#method.read_to_bytes\" title=\"method iroh_blobs::rpc::client::blobs::Reader::read_to_bytes\"><code>Reader::read_to_bytes</code></a>.</p>\n</div></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.read_at_to_bytes\" class=\"method\"><a class=\"src rightside\" href=\"src/iroh_blobs/rpc/client/blobs.rs.html#192-197\">Source</a><h4 class=\"code-header\">pub async fn <a href=\"iroh_blobs/rpc/client/blobs/struct.Client.html#tymethod.read_at_to_bytes\" class=\"fn\">read_at_to_bytes</a>(\n    &amp;self,\n    hash: <a class=\"struct\" href=\"iroh_blobs/struct.Hash.html\" title=\"struct iroh_blobs::Hash\">Hash</a>,\n    offset: <a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.u64.html\">u64</a>,\n    len: <a class=\"enum\" href=\"iroh_blobs/rpc/client/blobs/enum.ReadAtLen.html\" title=\"enum iroh_blobs::rpc::client::blobs::ReadAtLen\">ReadAtLen</a>,\n) -&gt; <a class=\"type\" href=\"https://docs.rs/anyhow/1.0.95/anyhow/type.Result.html\" title=\"type anyhow::Result\">Result</a>&lt;Bytes&gt;</h4></section></summary><div class=\"docblock\"><p>Read all bytes of single blob at <code>offset</code> for length <code>len</code>.</p>\n<p>This allocates a buffer for the full length.</p>\n</div></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.add_from_path\" class=\"method\"><a class=\"src rightside\" href=\"src/iroh_blobs/rpc/client/blobs.rs.html#205-222\">Source</a><h4 class=\"code-header\">pub async fn <a href=\"iroh_blobs/rpc/client/blobs/struct.Client.html#tymethod.add_from_path\" class=\"fn\">add_from_path</a>(\n    &amp;self,\n    path: <a class=\"struct\" href=\"https://doc.rust-lang.org/nightly/std/path/struct.PathBuf.html\" title=\"struct std::path::PathBuf\">PathBuf</a>,\n    in_place: <a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.bool.html\">bool</a>,\n    tag: <a class=\"enum\" href=\"iroh_blobs/util/enum.SetTagOption.html\" title=\"enum iroh_blobs::util::SetTagOption\">SetTagOption</a>,\n    wrap: <a class=\"enum\" href=\"iroh_blobs/rpc/client/blobs/enum.WrapOption.html\" title=\"enum iroh_blobs::rpc::client::blobs::WrapOption\">WrapOption</a>,\n) -&gt; <a class=\"type\" href=\"https://docs.rs/anyhow/1.0.95/anyhow/type.Result.html\" title=\"type anyhow::Result\">Result</a>&lt;<a class=\"struct\" href=\"iroh_blobs/rpc/client/blobs/struct.AddProgress.html\" title=\"struct iroh_blobs::rpc::client::blobs::AddProgress\">AddProgress</a>&gt;</h4></section></summary><div class=\"docblock\"><p>Import a blob from a filesystem path.</p>\n<p><code>path</code> should be an absolute path valid for the file system on which\nthe node runs.\nIf <code>in_place</code> is true, Iroh will assume that the data will not change and will share it in\nplace without copying to the Iroh data directory.</p>\n</div></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.create_collection\" class=\"method\"><a class=\"src rightside\" href=\"src/iroh_blobs/rpc/client/blobs.rs.html#228-243\">Source</a><h4 class=\"code-header\">pub async fn <a href=\"iroh_blobs/rpc/client/blobs/struct.Client.html#tymethod.create_collection\" class=\"fn\">create_collection</a>(\n    &amp;self,\n    collection: <a class=\"struct\" href=\"iroh_blobs/format/collection/struct.Collection.html\" title=\"struct iroh_blobs::format::collection::Collection\">Collection</a>,\n    tag: <a class=\"enum\" href=\"iroh_blobs/util/enum.SetTagOption.html\" title=\"enum iroh_blobs::util::SetTagOption\">SetTagOption</a>,\n    tags_to_delete: <a class=\"struct\" href=\"https://doc.rust-lang.org/nightly/alloc/vec/struct.Vec.html\" title=\"struct alloc::vec::Vec\">Vec</a>&lt;<a class=\"struct\" href=\"iroh_blobs/util/struct.Tag.html\" title=\"struct iroh_blobs::util::Tag\">Tag</a>&gt;,\n) -&gt; <a class=\"type\" href=\"https://docs.rs/anyhow/1.0.95/anyhow/type.Result.html\" title=\"type anyhow::Result\">Result</a>&lt;(<a class=\"struct\" href=\"iroh_blobs/struct.Hash.html\" title=\"struct iroh_blobs::Hash\">Hash</a>, <a class=\"struct\" href=\"iroh_blobs/util/struct.Tag.html\" title=\"struct iroh_blobs::util::Tag\">Tag</a>)&gt;</h4></section></summary><div class=\"docblock\"><p>Create a collection from already existing blobs.</p>\n<p>For automatically clearing the tags for the passed in blobs you can set\n<code>tags_to_delete</code> to those tags, and they will be deleted once the collection is created.</p>\n</div></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.add_reader\" class=\"method\"><a class=\"src rightside\" href=\"src/iroh_blobs/rpc/client/blobs.rs.html#246-254\">Source</a><h4 class=\"code-header\">pub async fn <a href=\"iroh_blobs/rpc/client/blobs/struct.Client.html#tymethod.add_reader\" class=\"fn\">add_reader</a>(\n    &amp;self,\n    reader: impl AsyncRead + <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/marker/trait.Unpin.html\" title=\"trait core::marker::Unpin\">Unpin</a> + <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/marker/trait.Send.html\" title=\"trait core::marker::Send\">Send</a> + 'static,\n    tag: <a class=\"enum\" href=\"iroh_blobs/util/enum.SetTagOption.html\" title=\"enum iroh_blobs::util::SetTagOption\">SetTagOption</a>,\n) -&gt; <a class=\"type\" href=\"https://docs.rs/anyhow/1.0.95/anyhow/type.Result.html\" title=\"type anyhow::Result\">Result</a>&lt;<a class=\"struct\" href=\"iroh_blobs/rpc/client/blobs/struct.AddProgress.html\" title=\"struct iroh_blobs::rpc::client::blobs::AddProgress\">AddProgress</a>&gt;</h4></section></summary><div class=\"docblock\"><p>Write a blob by passing an async reader.</p>\n</div></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.add_stream\" class=\"method\"><a class=\"src rightside\" href=\"src/iroh_blobs/rpc/client/blobs.rs.html#257-280\">Source</a><h4 class=\"code-header\">pub async fn <a href=\"iroh_blobs/rpc/client/blobs/struct.Client.html#tymethod.add_stream\" class=\"fn\">add_stream</a>(\n    &amp;self,\n    input: impl Stream&lt;Item = <a class=\"type\" href=\"https://doc.rust-lang.org/nightly/std/io/error/type.Result.html\" title=\"type std::io::error::Result\">Result</a>&lt;Bytes&gt;&gt; + <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/marker/trait.Send.html\" title=\"trait core::marker::Send\">Send</a> + <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/marker/trait.Unpin.html\" title=\"trait core::marker::Unpin\">Unpin</a> + 'static,\n    tag: <a class=\"enum\" href=\"iroh_blobs/util/enum.SetTagOption.html\" title=\"enum iroh_blobs::util::SetTagOption\">SetTagOption</a>,\n) -&gt; <a class=\"type\" href=\"https://docs.rs/anyhow/1.0.95/anyhow/type.Result.html\" title=\"type anyhow::Result\">Result</a>&lt;<a class=\"struct\" href=\"iroh_blobs/rpc/client/blobs/struct.AddProgress.html\" title=\"struct iroh_blobs::rpc::client::blobs::AddProgress\">AddProgress</a>&gt;</h4></section></summary><div class=\"docblock\"><p>Write a blob by passing a stream of bytes.</p>\n</div></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.add_bytes\" class=\"method\"><a class=\"src rightside\" href=\"src/iroh_blobs/rpc/client/blobs.rs.html#283-286\">Source</a><h4 class=\"code-header\">pub async fn <a href=\"iroh_blobs/rpc/client/blobs/struct.Client.html#tymethod.add_bytes\" class=\"fn\">add_bytes</a>(&amp;self, bytes: impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/convert/trait.Into.html\" title=\"trait core::convert::Into\">Into</a>&lt;Bytes&gt;) -&gt; <a class=\"type\" href=\"https://docs.rs/anyhow/1.0.95/anyhow/type.Result.html\" title=\"type anyhow::Result\">Result</a>&lt;<a class=\"struct\" href=\"iroh_blobs/rpc/client/blobs/struct.AddOutcome.html\" title=\"struct iroh_blobs::rpc::client::blobs::AddOutcome\">AddOutcome</a>&gt;</h4></section></summary><div class=\"docblock\"><p>Write a blob by passing bytes.</p>\n</div></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.add_bytes_named\" class=\"method\"><a class=\"src rightside\" href=\"src/iroh_blobs/rpc/client/blobs.rs.html#289-298\">Source</a><h4 class=\"code-header\">pub async fn <a href=\"iroh_blobs/rpc/client/blobs/struct.Client.html#tymethod.add_bytes_named\" class=\"fn\">add_bytes_named</a>(\n    &amp;self,\n    bytes: impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/convert/trait.Into.html\" title=\"trait core::convert::Into\">Into</a>&lt;Bytes&gt;,\n    name: impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/convert/trait.Into.html\" title=\"trait core::convert::Into\">Into</a>&lt;<a class=\"struct\" href=\"iroh_blobs/util/struct.Tag.html\" title=\"struct iroh_blobs::util::Tag\">Tag</a>&gt;,\n) -&gt; <a class=\"type\" href=\"https://docs.rs/anyhow/1.0.95/anyhow/type.Result.html\" title=\"type anyhow::Result\">Result</a>&lt;<a class=\"struct\" href=\"iroh_blobs/rpc/client/blobs/struct.AddOutcome.html\" title=\"struct iroh_blobs::rpc::client::blobs::AddOutcome\">AddOutcome</a>&gt;</h4></section></summary><div class=\"docblock\"><p>Write a blob by passing bytes, setting an explicit tag name.</p>\n</div></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.validate\" class=\"method\"><a class=\"src rightside\" href=\"src/iroh_blobs/rpc/client/blobs.rs.html#303-312\">Source</a><h4 class=\"code-header\">pub async fn <a href=\"iroh_blobs/rpc/client/blobs/struct.Client.html#tymethod.validate\" class=\"fn\">validate</a>(\n    &amp;self,\n    repair: <a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.bool.html\">bool</a>,\n) -&gt; <a class=\"type\" href=\"https://docs.rs/anyhow/1.0.95/anyhow/type.Result.html\" title=\"type anyhow::Result\">Result</a>&lt;impl Stream&lt;Item = <a class=\"type\" href=\"https://docs.rs/anyhow/1.0.95/anyhow/type.Result.html\" title=\"type anyhow::Result\">Result</a>&lt;<a class=\"enum\" href=\"iroh_blobs/store/enum.ValidateProgress.html\" title=\"enum iroh_blobs::store::ValidateProgress\">ValidateProgress</a>&gt;&gt;&gt;</h4></section></summary><div class=\"docblock\"><p>Validate hashes on the running node.</p>\n<p>If <code>repair</code> is true, repair the store by removing invalid data.</p>\n</div></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.consistency_check\" class=\"method\"><a class=\"src rightside\" href=\"src/iroh_blobs/rpc/client/blobs.rs.html#317-326\">Source</a><h4 class=\"code-header\">pub async fn <a href=\"iroh_blobs/rpc/client/blobs/struct.Client.html#tymethod.consistency_check\" class=\"fn\">consistency_check</a>(\n    &amp;self,\n    repair: <a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.bool.html\">bool</a>,\n) -&gt; <a class=\"type\" href=\"https://docs.rs/anyhow/1.0.95/anyhow/type.Result.html\" title=\"type anyhow::Result\">Result</a>&lt;impl Stream&lt;Item = <a class=\"type\" href=\"https://docs.rs/anyhow/1.0.95/anyhow/type.Result.html\" title=\"type anyhow::Result\">Result</a>&lt;<a class=\"enum\" href=\"iroh_blobs/store/enum.ConsistencyCheckProgress.html\" title=\"enum iroh_blobs::store::ConsistencyCheckProgress\">ConsistencyCheckProgress</a>&gt;&gt;&gt;</h4></section></summary><div class=\"docblock\"><p>Validate hashes on the running node.</p>\n<p>If <code>repair</code> is true, repair the store by removing invalid data.</p>\n</div></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.download\" class=\"method\"><a class=\"src rightside\" href=\"src/iroh_blobs/rpc/client/blobs.rs.html#329-340\">Source</a><h4 class=\"code-header\">pub async fn <a href=\"iroh_blobs/rpc/client/blobs/struct.Client.html#tymethod.download\" class=\"fn\">download</a>(\n    &amp;self,\n    hash: <a class=\"struct\" href=\"iroh_blobs/struct.Hash.html\" title=\"struct iroh_blobs::Hash\">Hash</a>,\n    node: NodeAddr,\n) -&gt; <a class=\"type\" href=\"https://docs.rs/anyhow/1.0.95/anyhow/type.Result.html\" title=\"type anyhow::Result\">Result</a>&lt;<a class=\"struct\" href=\"iroh_blobs/rpc/client/blobs/struct.DownloadProgress.html\" title=\"struct iroh_blobs::rpc::client::blobs::DownloadProgress\">DownloadProgress</a>&gt;</h4></section></summary><div class=\"docblock\"><p>Download a blob from another node and add it to the local database.</p>\n</div></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.download_hash_seq\" class=\"method\"><a class=\"src rightside\" href=\"src/iroh_blobs/rpc/client/blobs.rs.html#343-354\">Source</a><h4 class=\"code-header\">pub async fn <a href=\"iroh_blobs/rpc/client/blobs/struct.Client.html#tymethod.download_hash_seq\" class=\"fn\">download_hash_seq</a>(\n    &amp;self,\n    hash: <a class=\"struct\" href=\"iroh_blobs/struct.Hash.html\" title=\"struct iroh_blobs::Hash\">Hash</a>,\n    node: NodeAddr,\n) -&gt; <a class=\"type\" href=\"https://docs.rs/anyhow/1.0.95/anyhow/type.Result.html\" title=\"type anyhow::Result\">Result</a>&lt;<a class=\"struct\" href=\"iroh_blobs/rpc/client/blobs/struct.DownloadProgress.html\" title=\"struct iroh_blobs::rpc::client::blobs::DownloadProgress\">DownloadProgress</a>&gt;</h4></section></summary><div class=\"docblock\"><p>Download a hash sequence from another node and add it to the local database.</p>\n</div></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.download_with_opts\" class=\"method\"><a class=\"src rightside\" href=\"src/iroh_blobs/rpc/client/blobs.rs.html#357-381\">Source</a><h4 class=\"code-header\">pub async fn <a href=\"iroh_blobs/rpc/client/blobs/struct.Client.html#tymethod.download_with_opts\" class=\"fn\">download_with_opts</a>(\n    &amp;self,\n    hash: <a class=\"struct\" href=\"iroh_blobs/struct.Hash.html\" title=\"struct iroh_blobs::Hash\">Hash</a>,\n    opts: <a class=\"struct\" href=\"iroh_blobs/rpc/client/blobs/struct.DownloadOptions.html\" title=\"struct iroh_blobs::rpc::client::blobs::DownloadOptions\">DownloadOptions</a>,\n) -&gt; <a class=\"type\" href=\"https://docs.rs/anyhow/1.0.95/anyhow/type.Result.html\" title=\"type anyhow::Result\">Result</a>&lt;<a class=\"struct\" href=\"iroh_blobs/rpc/client/blobs/struct.DownloadProgress.html\" title=\"struct iroh_blobs::rpc::client::blobs::DownloadProgress\">DownloadProgress</a>&gt;</h4></section></summary><div class=\"docblock\"><p>Download a blob, with additional options.</p>\n</div></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.export\" class=\"method\"><a class=\"src rightside\" href=\"src/iroh_blobs/rpc/client/blobs.rs.html#392-409\">Source</a><h4 class=\"code-header\">pub async fn <a href=\"iroh_blobs/rpc/client/blobs/struct.Client.html#tymethod.export\" class=\"fn\">export</a>(\n    &amp;self,\n    hash: <a class=\"struct\" href=\"iroh_blobs/struct.Hash.html\" title=\"struct iroh_blobs::Hash\">Hash</a>,\n    destination: <a class=\"struct\" href=\"https://doc.rust-lang.org/nightly/std/path/struct.PathBuf.html\" title=\"struct std::path::PathBuf\">PathBuf</a>,\n    format: <a class=\"enum\" href=\"iroh_blobs/store/enum.ExportFormat.html\" title=\"enum iroh_blobs::store::ExportFormat\">ExportFormat</a>,\n    mode: <a class=\"enum\" href=\"iroh_blobs/store/enum.ExportMode.html\" title=\"enum iroh_blobs::store::ExportMode\">ExportMode</a>,\n) -&gt; <a class=\"type\" href=\"https://docs.rs/anyhow/1.0.95/anyhow/type.Result.html\" title=\"type anyhow::Result\">Result</a>&lt;<a class=\"struct\" href=\"iroh_blobs/rpc/client/blobs/struct.ExportProgress.html\" title=\"struct iroh_blobs::rpc::client::blobs::ExportProgress\">ExportProgress</a>&gt;</h4></section></summary><div class=\"docblock\"><p>Export a blob from the internal blob store to a path on the node’s filesystem.</p>\n<p><code>destination</code> should be an writeable, absolute path on the local node’s filesystem.</p>\n<p>If <code>format</code> is set to <a href=\"iroh_blobs/store/enum.ExportFormat.html#variant.Collection\" title=\"variant iroh_blobs::store::ExportFormat::Collection\"><code>ExportFormat::Collection</code></a>, and the <code>hash</code> refers to a collection,\nall children of the collection will be exported. See <a href=\"iroh_blobs/store/enum.ExportFormat.html\" title=\"enum iroh_blobs::store::ExportFormat\"><code>ExportFormat</code></a> for details.</p>\n<p>The <code>mode</code> argument defines if the blob should be copied to the target location or moved out of\nthe internal store into the target location. See <a href=\"iroh_blobs/store/enum.ExportMode.html\" title=\"enum iroh_blobs::store::ExportMode\"><code>ExportMode</code></a> for details.</p>\n</div></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.list\" class=\"method\"><a class=\"src rightside\" href=\"src/iroh_blobs/rpc/client/blobs.rs.html#412-415\">Source</a><h4 class=\"code-header\">pub async fn <a href=\"iroh_blobs/rpc/client/blobs/struct.Client.html#tymethod.list\" class=\"fn\">list</a>(&amp;self) -&gt; <a class=\"type\" href=\"https://docs.rs/anyhow/1.0.95/anyhow/type.Result.html\" title=\"type anyhow::Result\">Result</a>&lt;impl Stream&lt;Item = <a class=\"type\" href=\"https://docs.rs/anyhow/1.0.95/anyhow/type.Result.html\" title=\"type anyhow::Result\">Result</a>&lt;<a class=\"struct\" href=\"iroh_blobs/rpc/client/blobs/struct.BlobInfo.html\" title=\"struct iroh_blobs::rpc::client::blobs::BlobInfo\">BlobInfo</a>&gt;&gt;&gt;</h4></section></summary><div class=\"docblock\"><p>List all complete blobs.</p>\n</div></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.list_incomplete\" class=\"method\"><a class=\"src rightside\" href=\"src/iroh_blobs/rpc/client/blobs.rs.html#418-421\">Source</a><h4 class=\"code-header\">pub async fn <a href=\"iroh_blobs/rpc/client/blobs/struct.Client.html#tymethod.list_incomplete\" class=\"fn\">list_incomplete</a>(\n    &amp;self,\n) -&gt; <a class=\"type\" href=\"https://docs.rs/anyhow/1.0.95/anyhow/type.Result.html\" title=\"type anyhow::Result\">Result</a>&lt;impl Stream&lt;Item = <a class=\"type\" href=\"https://docs.rs/anyhow/1.0.95/anyhow/type.Result.html\" title=\"type anyhow::Result\">Result</a>&lt;<a class=\"struct\" href=\"iroh_blobs/rpc/client/blobs/struct.IncompleteBlobInfo.html\" title=\"struct iroh_blobs::rpc::client::blobs::IncompleteBlobInfo\">IncompleteBlobInfo</a>&gt;&gt;&gt;</h4></section></summary><div class=\"docblock\"><p>List all incomplete (partial) blobs.</p>\n</div></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.get_collection\" class=\"method\"><a class=\"src rightside\" href=\"src/iroh_blobs/rpc/client/blobs.rs.html#424-426\">Source</a><h4 class=\"code-header\">pub async fn <a href=\"iroh_blobs/rpc/client/blobs/struct.Client.html#tymethod.get_collection\" class=\"fn\">get_collection</a>(&amp;self, hash: <a class=\"struct\" href=\"iroh_blobs/struct.Hash.html\" title=\"struct iroh_blobs::Hash\">Hash</a>) -&gt; <a class=\"type\" href=\"https://docs.rs/anyhow/1.0.95/anyhow/type.Result.html\" title=\"type anyhow::Result\">Result</a>&lt;<a class=\"struct\" href=\"iroh_blobs/format/collection/struct.Collection.html\" title=\"struct iroh_blobs::format::collection::Collection\">Collection</a>&gt;</h4></section></summary><div class=\"docblock\"><p>Read the content of a collection.</p>\n</div></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.list_collections\" class=\"method\"><a class=\"src rightside\" href=\"src/iroh_blobs/rpc/client/blobs.rs.html#429-436\">Source</a><h4 class=\"code-header\">pub fn <a href=\"iroh_blobs/rpc/client/blobs/struct.Client.html#tymethod.list_collections\" class=\"fn\">list_collections</a>(\n    &amp;self,\n) -&gt; <a class=\"type\" href=\"https://docs.rs/anyhow/1.0.95/anyhow/type.Result.html\" title=\"type anyhow::Result\">Result</a>&lt;impl Stream&lt;Item = <a class=\"type\" href=\"https://docs.rs/anyhow/1.0.95/anyhow/type.Result.html\" title=\"type anyhow::Result\">Result</a>&lt;<a class=\"struct\" href=\"iroh_blobs/rpc/client/blobs/struct.CollectionInfo.html\" title=\"struct iroh_blobs::rpc::client::blobs::CollectionInfo\">CollectionInfo</a>&gt;&gt;&gt;</h4></section></summary><div class=\"docblock\"><p>List all collections.</p>\n</div></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.delete_blob\" class=\"method\"><a class=\"src rightside\" href=\"src/iroh_blobs/rpc/client/blobs.rs.html#461-464\">Source</a><h4 class=\"code-header\">pub async fn <a href=\"iroh_blobs/rpc/client/blobs/struct.Client.html#tymethod.delete_blob\" class=\"fn\">delete_blob</a>(&amp;self, hash: <a class=\"struct\" href=\"iroh_blobs/struct.Hash.html\" title=\"struct iroh_blobs::Hash\">Hash</a>) -&gt; <a class=\"type\" href=\"https://docs.rs/anyhow/1.0.95/anyhow/type.Result.html\" title=\"type anyhow::Result\">Result</a>&lt;<a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.unit.html\">()</a>&gt;</h4></section></summary><div class=\"docblock\"><p>Delete a blob.</p>\n<p><strong>Warning</strong>: this operation deletes the blob from the local store even\nif it is tagged. You should usually not do this manually, but rely on the\nnode to remove data that is not tagged.</p>\n</div></details></div></details>",0,"iroh_blobs::rpc::client::blobs::MemClient"],["<details class=\"toggle implementors-toggle\" open><summary><section id=\"impl-Clone-for-Client%3CC%3E\" class=\"impl\"><a class=\"src rightside\" href=\"src/iroh_blobs/rpc/client/blobs.rs.html#108\">Source</a><a href=\"#impl-Clone-for-Client%3CC%3E\" class=\"anchor\">§</a><h3 class=\"code-header\">impl&lt;C: <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/clone/trait.Clone.html\" title=\"trait core::clone::Clone\">Clone</a>&gt; <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/clone/trait.Clone.html\" title=\"trait core::clone::Clone\">Clone</a> for <a class=\"struct\" href=\"iroh_blobs/rpc/client/blobs/struct.Client.html\" title=\"struct iroh_blobs::rpc::client::blobs::Client\">Client</a>&lt;C&gt;</h3></section></summary><div class=\"impl-items\"><details class=\"toggle method-toggle\" open><summary><section id=\"method.clone\" class=\"method trait-impl\"><a class=\"src rightside\" href=\"src/iroh_blobs/rpc/client/blobs.rs.html#108\">Source</a><a href=\"#method.clone\" class=\"anchor\">§</a><h4 class=\"code-header\">fn <a href=\"https://doc.rust-lang.org/nightly/core/clone/trait.Clone.html#tymethod.clone\" class=\"fn\">clone</a>(&amp;self) -&gt; <a class=\"struct\" href=\"iroh_blobs/rpc/client/blobs/struct.Client.html\" title=\"struct iroh_blobs::rpc::client::blobs::Client\">Client</a>&lt;C&gt;</h4></section></summary><div class='docblock'>Returns a copy of the value. <a href=\"https://doc.rust-lang.org/nightly/core/clone/trait.Clone.html#tymethod.clone\">Read more</a></div></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.clone_from\" class=\"method trait-impl\"><span class=\"rightside\"><span class=\"since\" title=\"Stable since Rust version 1.0.0\">1.0.0</span> · <a class=\"src\" href=\"https://doc.rust-lang.org/nightly/src/core/clone.rs.html#174\">Source</a></span><a href=\"#method.clone_from\" class=\"anchor\">§</a><h4 class=\"code-header\">fn <a href=\"https://doc.rust-lang.org/nightly/core/clone/trait.Clone.html#method.clone_from\" class=\"fn\">clone_from</a>(&amp;mut self, source: &amp;Self)</h4></section></summary><div class='docblock'>Performs copy-assignment from <code>source</code>. <a href=\"https://doc.rust-lang.org/nightly/core/clone/trait.Clone.html#method.clone_from\">Read more</a></div></details></div></details>","Clone","iroh_blobs::rpc::client::blobs::MemClient"],["<details class=\"toggle implementors-toggle\" open><summary><section id=\"impl-Debug-for-Client%3CC%3E\" class=\"impl\"><a class=\"src rightside\" href=\"src/iroh_blobs/rpc/client/blobs.rs.html#108\">Source</a><a href=\"#impl-Debug-for-Client%3CC%3E\" class=\"anchor\">§</a><h3 class=\"code-header\">impl&lt;C: <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/fmt/trait.Debug.html\" title=\"trait core::fmt::Debug\">Debug</a>&gt; <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/fmt/trait.Debug.html\" title=\"trait core::fmt::Debug\">Debug</a> for <a class=\"struct\" href=\"iroh_blobs/rpc/client/blobs/struct.Client.html\" title=\"struct iroh_blobs::rpc::client::blobs::Client\">Client</a>&lt;C&gt;</h3></section></summary><div class=\"impl-items\"><details class=\"toggle method-toggle\" open><summary><section id=\"method.fmt\" class=\"method trait-impl\"><a class=\"src rightside\" href=\"src/iroh_blobs/rpc/client/blobs.rs.html#108\">Source</a><a href=\"#method.fmt\" class=\"anchor\">§</a><h4 class=\"code-header\">fn <a href=\"https://doc.rust-lang.org/nightly/core/fmt/trait.Debug.html#tymethod.fmt\" class=\"fn\">fmt</a>(&amp;self, f: &amp;mut <a class=\"struct\" href=\"https://doc.rust-lang.org/nightly/core/fmt/struct.Formatter.html\" title=\"struct core::fmt::Formatter\">Formatter</a>&lt;'_&gt;) -&gt; <a class=\"type\" href=\"https://doc.rust-lang.org/nightly/core/fmt/type.Result.html\" title=\"type core::fmt::Result\">Result</a></h4></section></summary><div class='docblock'>Formats the value using the given formatter. <a href=\"https://doc.rust-lang.org/nightly/core/fmt/trait.Debug.html#tymethod.fmt\">Read more</a></div></details></div></details>","Debug","iroh_blobs::rpc::client::blobs::MemClient"],["<details class=\"toggle implementors-toggle\" open><summary><section id=\"impl-SimpleStore-for-Client%3CC%3E\" class=\"impl\"><a class=\"src rightside\" href=\"src/iroh_blobs/rpc/client/blobs.rs.html#471-478\">Source</a><a href=\"#impl-SimpleStore-for-Client%3CC%3E\" class=\"anchor\">§</a><h3 class=\"code-header\">impl&lt;C&gt; <a class=\"trait\" href=\"iroh_blobs/format/collection/trait.SimpleStore.html\" title=\"trait iroh_blobs::format::collection::SimpleStore\">SimpleStore</a> for <a class=\"struct\" href=\"iroh_blobs/rpc/client/blobs/struct.Client.html\" title=\"struct iroh_blobs::rpc::client::blobs::Client\">Client</a>&lt;C&gt;<div class=\"where\">where\n    C: Connector&lt;<a class=\"struct\" href=\"iroh_blobs/rpc/proto/struct.RpcService.html\" title=\"struct iroh_blobs::rpc::proto::RpcService\">RpcService</a>&gt;,</div></h3></section></summary><div class=\"impl-items\"><details class=\"toggle method-toggle\" open><summary><section id=\"method.load\" class=\"method trait-impl\"><a class=\"src rightside\" href=\"src/iroh_blobs/rpc/client/blobs.rs.html#475-477\">Source</a><a href=\"#method.load\" class=\"anchor\">§</a><h4 class=\"code-header\">async fn <a href=\"iroh_blobs/format/collection/trait.SimpleStore.html#tymethod.load\" class=\"fn\">load</a>(&amp;self, hash: <a class=\"struct\" href=\"iroh_blobs/struct.Hash.html\" title=\"struct iroh_blobs::Hash\">Hash</a>) -&gt; <a class=\"type\" href=\"https://docs.rs/anyhow/1.0.95/anyhow/type.Result.html\" title=\"type anyhow::Result\">Result</a>&lt;Bytes&gt;</h4></section></summary><div class='docblock'>Load a blob from the store</div></details></div></details>","SimpleStore","iroh_blobs::rpc::client::blobs::MemClient"]]]]);
    if (window.register_type_impls) {
        window.register_type_impls(type_impls);
    } else {
        window.pending_type_impls = type_impls;
    }
})()
//{"start":55,"fragment_lengths":[35490]}