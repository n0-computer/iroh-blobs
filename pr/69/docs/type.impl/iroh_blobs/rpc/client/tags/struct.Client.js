(function() {
    var type_impls = Object.fromEntries([["iroh_blobs",[["<details class=\"toggle implementors-toggle\" open><summary><section id=\"impl-Client%3CC%3E\" class=\"impl\"><a class=\"src rightside\" href=\"src/iroh_blobs/rpc/client/tags.rs.html#158-241\">Source</a><a href=\"#impl-Client%3CC%3E\" class=\"anchor\">§</a><h3 class=\"code-header\">impl&lt;C&gt; <a class=\"struct\" href=\"iroh_blobs/rpc/client/tags/struct.Client.html\" title=\"struct iroh_blobs::rpc::client::tags::Client\">Client</a>&lt;C&gt;<div class=\"where\">where\n    C: Connector&lt;<a class=\"struct\" href=\"iroh_blobs/rpc/proto/struct.RpcService.html\" title=\"struct iroh_blobs::rpc::proto::RpcService\">RpcService</a>&gt;,</div></h3></section></summary><div class=\"impl-items\"><details class=\"toggle method-toggle\" open><summary><section id=\"method.new\" class=\"method\"><a class=\"src rightside\" href=\"src/iroh_blobs/rpc/client/tags.rs.html#163-165\">Source</a><h4 class=\"code-header\">pub fn <a href=\"iroh_blobs/rpc/client/tags/struct.Client.html#tymethod.new\" class=\"fn\">new</a>(rpc: RpcClient&lt;<a class=\"struct\" href=\"iroh_blobs/rpc/proto/struct.RpcService.html\" title=\"struct iroh_blobs::rpc::proto::RpcService\">RpcService</a>, C&gt;) -&gt; Self</h4></section></summary><div class=\"docblock\"><p>Creates a new client</p>\n</div></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.list_with_opts\" class=\"method\"><a class=\"src rightside\" href=\"src/iroh_blobs/rpc/client/tags.rs.html#171-180\">Source</a><h4 class=\"code-header\">pub async fn <a href=\"iroh_blobs/rpc/client/tags/struct.Client.html#tymethod.list_with_opts\" class=\"fn\">list_with_opts</a>(\n    &amp;self,\n    options: <a class=\"struct\" href=\"iroh_blobs/rpc/client/tags/struct.ListOptions.html\" title=\"struct iroh_blobs::rpc::client::tags::ListOptions\">ListOptions</a>,\n) -&gt; <a class=\"type\" href=\"https://docs.rs/anyhow/1.0.97/anyhow/type.Result.html\" title=\"type anyhow::Result\">Result</a>&lt;impl Stream&lt;Item = <a class=\"type\" href=\"https://docs.rs/anyhow/1.0.97/anyhow/type.Result.html\" title=\"type anyhow::Result\">Result</a>&lt;<a class=\"struct\" href=\"iroh_blobs/rpc/client/tags/struct.TagInfo.html\" title=\"struct iroh_blobs::rpc::client::tags::TagInfo\">TagInfo</a>&gt;&gt;&gt;</h4></section></summary><div class=\"docblock\"><p>List all tags with options.</p>\n<p>This is the most flexible way to list tags. All the other list methods are just convenience\nmethods that call this one with the appropriate options.</p>\n</div></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.set\" class=\"method\"><a class=\"src rightside\" href=\"src/iroh_blobs/rpc/client/tags.rs.html#183-193\">Source</a><h4 class=\"code-header\">pub async fn <a href=\"iroh_blobs/rpc/client/tags/struct.Client.html#tymethod.set\" class=\"fn\">set</a>(\n    &amp;self,\n    name: impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/convert/trait.AsRef.html\" title=\"trait core::convert::AsRef\">AsRef</a>&lt;[<a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.u8.html\">u8</a>]&gt;,\n    value: impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/convert/trait.Into.html\" title=\"trait core::convert::Into\">Into</a>&lt;<a class=\"struct\" href=\"iroh_blobs/struct.HashAndFormat.html\" title=\"struct iroh_blobs::HashAndFormat\">HashAndFormat</a>&gt;,\n) -&gt; <a class=\"type\" href=\"https://docs.rs/anyhow/1.0.97/anyhow/type.Result.html\" title=\"type anyhow::Result\">Result</a>&lt;<a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.unit.html\">()</a>&gt;</h4></section></summary><div class=\"docblock\"><p>Set the value for a single tag</p>\n</div></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.get\" class=\"method\"><a class=\"src rightside\" href=\"src/iroh_blobs/rpc/client/tags.rs.html#196-201\">Source</a><h4 class=\"code-header\">pub async fn <a href=\"iroh_blobs/rpc/client/tags/struct.Client.html#tymethod.get\" class=\"fn\">get</a>(&amp;self, name: impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/convert/trait.AsRef.html\" title=\"trait core::convert::AsRef\">AsRef</a>&lt;[<a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.u8.html\">u8</a>]&gt;) -&gt; <a class=\"type\" href=\"https://docs.rs/anyhow/1.0.97/anyhow/type.Result.html\" title=\"type anyhow::Result\">Result</a>&lt;<a class=\"enum\" href=\"https://doc.rust-lang.org/nightly/core/option/enum.Option.html\" title=\"enum core::option::Option\">Option</a>&lt;<a class=\"struct\" href=\"iroh_blobs/rpc/client/tags/struct.TagInfo.html\" title=\"struct iroh_blobs::rpc::client::tags::TagInfo\">TagInfo</a>&gt;&gt;</h4></section></summary><div class=\"docblock\"><p>Get the value of a single tag</p>\n</div></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.list_range\" class=\"method\"><a class=\"src rightside\" href=\"src/iroh_blobs/rpc/client/tags.rs.html#204-210\">Source</a><h4 class=\"code-header\">pub async fn <a href=\"iroh_blobs/rpc/client/tags/struct.Client.html#tymethod.list_range\" class=\"fn\">list_range</a>&lt;R, E&gt;(\n    &amp;self,\n    range: R,\n) -&gt; <a class=\"type\" href=\"https://docs.rs/anyhow/1.0.97/anyhow/type.Result.html\" title=\"type anyhow::Result\">Result</a>&lt;impl Stream&lt;Item = <a class=\"type\" href=\"https://docs.rs/anyhow/1.0.97/anyhow/type.Result.html\" title=\"type anyhow::Result\">Result</a>&lt;<a class=\"struct\" href=\"iroh_blobs/rpc/client/tags/struct.TagInfo.html\" title=\"struct iroh_blobs::rpc::client::tags::TagInfo\">TagInfo</a>&gt;&gt;&gt;<div class=\"where\">where\n    R: <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/ops/range/trait.RangeBounds.html\" title=\"trait core::ops::range::RangeBounds\">RangeBounds</a>&lt;E&gt;,\n    E: <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/convert/trait.AsRef.html\" title=\"trait core::convert::AsRef\">AsRef</a>&lt;[<a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.u8.html\">u8</a>]&gt;,</div></h4></section></summary><div class=\"docblock\"><p>List a range of tags</p>\n</div></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.list_prefix\" class=\"method\"><a class=\"src rightside\" href=\"src/iroh_blobs/rpc/client/tags.rs.html#213-219\">Source</a><h4 class=\"code-header\">pub async fn <a href=\"iroh_blobs/rpc/client/tags/struct.Client.html#tymethod.list_prefix\" class=\"fn\">list_prefix</a>(\n    &amp;self,\n    prefix: impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/convert/trait.AsRef.html\" title=\"trait core::convert::AsRef\">AsRef</a>&lt;[<a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.u8.html\">u8</a>]&gt;,\n) -&gt; <a class=\"type\" href=\"https://docs.rs/anyhow/1.0.97/anyhow/type.Result.html\" title=\"type anyhow::Result\">Result</a>&lt;impl Stream&lt;Item = <a class=\"type\" href=\"https://docs.rs/anyhow/1.0.97/anyhow/type.Result.html\" title=\"type anyhow::Result\">Result</a>&lt;<a class=\"struct\" href=\"iroh_blobs/rpc/client/tags/struct.TagInfo.html\" title=\"struct iroh_blobs::rpc::client::tags::TagInfo\">TagInfo</a>&gt;&gt;&gt;</h4></section></summary><div class=\"docblock\"><p>Lists all tags.</p>\n</div></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.list\" class=\"method\"><a class=\"src rightside\" href=\"src/iroh_blobs/rpc/client/tags.rs.html#222-224\">Source</a><h4 class=\"code-header\">pub async fn <a href=\"iroh_blobs/rpc/client/tags/struct.Client.html#tymethod.list\" class=\"fn\">list</a>(&amp;self) -&gt; <a class=\"type\" href=\"https://docs.rs/anyhow/1.0.97/anyhow/type.Result.html\" title=\"type anyhow::Result\">Result</a>&lt;impl Stream&lt;Item = <a class=\"type\" href=\"https://docs.rs/anyhow/1.0.97/anyhow/type.Result.html\" title=\"type anyhow::Result\">Result</a>&lt;<a class=\"struct\" href=\"iroh_blobs/rpc/client/tags/struct.TagInfo.html\" title=\"struct iroh_blobs::rpc::client::tags::TagInfo\">TagInfo</a>&gt;&gt;&gt;</h4></section></summary><div class=\"docblock\"><p>Lists all tags.</p>\n</div></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.list_hash_seq\" class=\"method\"><a class=\"src rightside\" href=\"src/iroh_blobs/rpc/client/tags.rs.html#227-229\">Source</a><h4 class=\"code-header\">pub async fn <a href=\"iroh_blobs/rpc/client/tags/struct.Client.html#tymethod.list_hash_seq\" class=\"fn\">list_hash_seq</a>(&amp;self) -&gt; <a class=\"type\" href=\"https://docs.rs/anyhow/1.0.97/anyhow/type.Result.html\" title=\"type anyhow::Result\">Result</a>&lt;impl Stream&lt;Item = <a class=\"type\" href=\"https://docs.rs/anyhow/1.0.97/anyhow/type.Result.html\" title=\"type anyhow::Result\">Result</a>&lt;<a class=\"struct\" href=\"iroh_blobs/rpc/client/tags/struct.TagInfo.html\" title=\"struct iroh_blobs::rpc::client::tags::TagInfo\">TagInfo</a>&gt;&gt;&gt;</h4></section></summary><div class=\"docblock\"><p>Lists all tags with a hash_seq format.</p>\n</div></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.delete_with_opts\" class=\"method\"><a class=\"src rightside\" href=\"src/iroh_blobs/rpc/client/tags.rs.html#232-235\">Source</a><h4 class=\"code-header\">pub async fn <a href=\"iroh_blobs/rpc/client/tags/struct.Client.html#tymethod.delete_with_opts\" class=\"fn\">delete_with_opts</a>(&amp;self, options: <a class=\"struct\" href=\"iroh_blobs/rpc/client/tags/struct.DeleteOptions.html\" title=\"struct iroh_blobs::rpc::client::tags::DeleteOptions\">DeleteOptions</a>) -&gt; <a class=\"type\" href=\"https://docs.rs/anyhow/1.0.97/anyhow/type.Result.html\" title=\"type anyhow::Result\">Result</a>&lt;<a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.unit.html\">()</a>&gt;</h4></section></summary><div class=\"docblock\"><p>Deletes a tag.</p>\n</div></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.delete\" class=\"method\"><a class=\"src rightside\" href=\"src/iroh_blobs/rpc/client/tags.rs.html#238-240\">Source</a><h4 class=\"code-header\">pub async fn <a href=\"iroh_blobs/rpc/client/tags/struct.Client.html#tymethod.delete\" class=\"fn\">delete</a>(&amp;self, name: <a class=\"struct\" href=\"iroh_blobs/util/struct.Tag.html\" title=\"struct iroh_blobs::util::Tag\">Tag</a>) -&gt; <a class=\"type\" href=\"https://docs.rs/anyhow/1.0.97/anyhow/type.Result.html\" title=\"type anyhow::Result\">Result</a>&lt;<a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.unit.html\">()</a>&gt;</h4></section></summary><div class=\"docblock\"><p>Deletes a tag.</p>\n</div></details></div></details>",0,"iroh_blobs::rpc::client::tags::MemClient"],["<details class=\"toggle implementors-toggle\" open><summary><section id=\"impl-Clone-for-Client%3CC%3E\" class=\"impl\"><a class=\"src rightside\" href=\"src/iroh_blobs/rpc/client/tags.rs.html#29\">Source</a><a href=\"#impl-Clone-for-Client%3CC%3E\" class=\"anchor\">§</a><h3 class=\"code-header\">impl&lt;C: <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/clone/trait.Clone.html\" title=\"trait core::clone::Clone\">Clone</a>&gt; <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/clone/trait.Clone.html\" title=\"trait core::clone::Clone\">Clone</a> for <a class=\"struct\" href=\"iroh_blobs/rpc/client/tags/struct.Client.html\" title=\"struct iroh_blobs::rpc::client::tags::Client\">Client</a>&lt;C&gt;</h3></section></summary><div class=\"impl-items\"><details class=\"toggle method-toggle\" open><summary><section id=\"method.clone\" class=\"method trait-impl\"><a class=\"src rightside\" href=\"src/iroh_blobs/rpc/client/tags.rs.html#29\">Source</a><a href=\"#method.clone\" class=\"anchor\">§</a><h4 class=\"code-header\">fn <a href=\"https://doc.rust-lang.org/nightly/core/clone/trait.Clone.html#tymethod.clone\" class=\"fn\">clone</a>(&amp;self) -&gt; <a class=\"struct\" href=\"iroh_blobs/rpc/client/tags/struct.Client.html\" title=\"struct iroh_blobs::rpc::client::tags::Client\">Client</a>&lt;C&gt;</h4></section></summary><div class='docblock'>Returns a copy of the value. <a href=\"https://doc.rust-lang.org/nightly/core/clone/trait.Clone.html#tymethod.clone\">Read more</a></div></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.clone_from\" class=\"method trait-impl\"><span class=\"rightside\"><span class=\"since\" title=\"Stable since Rust version 1.0.0\">1.0.0</span> · <a class=\"src\" href=\"https://doc.rust-lang.org/nightly/src/core/clone.rs.html#174\">Source</a></span><a href=\"#method.clone_from\" class=\"anchor\">§</a><h4 class=\"code-header\">fn <a href=\"https://doc.rust-lang.org/nightly/core/clone/trait.Clone.html#method.clone_from\" class=\"fn\">clone_from</a>(&amp;mut self, source: &amp;Self)</h4></section></summary><div class='docblock'>Performs copy-assignment from <code>source</code>. <a href=\"https://doc.rust-lang.org/nightly/core/clone/trait.Clone.html#method.clone_from\">Read more</a></div></details></div></details>","Clone","iroh_blobs::rpc::client::tags::MemClient"],["<details class=\"toggle implementors-toggle\" open><summary><section id=\"impl-Debug-for-Client%3CC%3E\" class=\"impl\"><a class=\"src rightside\" href=\"src/iroh_blobs/rpc/client/tags.rs.html#29\">Source</a><a href=\"#impl-Debug-for-Client%3CC%3E\" class=\"anchor\">§</a><h3 class=\"code-header\">impl&lt;C: <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/fmt/trait.Debug.html\" title=\"trait core::fmt::Debug\">Debug</a>&gt; <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/fmt/trait.Debug.html\" title=\"trait core::fmt::Debug\">Debug</a> for <a class=\"struct\" href=\"iroh_blobs/rpc/client/tags/struct.Client.html\" title=\"struct iroh_blobs::rpc::client::tags::Client\">Client</a>&lt;C&gt;</h3></section></summary><div class=\"impl-items\"><details class=\"toggle method-toggle\" open><summary><section id=\"method.fmt\" class=\"method trait-impl\"><a class=\"src rightside\" href=\"src/iroh_blobs/rpc/client/tags.rs.html#29\">Source</a><a href=\"#method.fmt\" class=\"anchor\">§</a><h4 class=\"code-header\">fn <a href=\"https://doc.rust-lang.org/nightly/core/fmt/trait.Debug.html#tymethod.fmt\" class=\"fn\">fmt</a>(&amp;self, f: &amp;mut <a class=\"struct\" href=\"https://doc.rust-lang.org/nightly/core/fmt/struct.Formatter.html\" title=\"struct core::fmt::Formatter\">Formatter</a>&lt;'_&gt;) -&gt; <a class=\"type\" href=\"https://doc.rust-lang.org/nightly/core/fmt/type.Result.html\" title=\"type core::fmt::Result\">Result</a></h4></section></summary><div class='docblock'>Formats the value using the given formatter. <a href=\"https://doc.rust-lang.org/nightly/core/fmt/trait.Debug.html#tymethod.fmt\">Read more</a></div></details></div></details>","Debug","iroh_blobs::rpc::client::tags::MemClient"]]]]);
    if (window.register_type_impls) {
        window.register_type_impls(type_impls);
    } else {
        window.pending_type_impls = type_impls;
    }
})()
//{"start":55,"fragment_lengths":[15019]}