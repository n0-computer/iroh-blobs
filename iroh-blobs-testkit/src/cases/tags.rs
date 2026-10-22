//! Tags, checked against a model.
use std::collections::BTreeMap;

use anyhow::{ensure, Context};
use iroh_blobs::{
    api::{
        proto::{DeleteTagsRequest, ListTagsRequest, TagInfo},
        Store, Tag,
    },
    BlobFormat, Hash, HashAndFormat,
};
use proptest::{option, prelude::*};

use crate::harness::Harness;

/// A tag operation.
#[derive(Debug, Clone)]
enum Op {
    Set(Tag, HashAndFormat),
    Create(HashAndFormat),
    Delete {
        from: Option<Tag>,
        to: Option<Tag>,
    },
    Rename {
        from: Tag,
        to: Tag,
    },
    List {
        from: Option<Tag>,
        to: Option<Tag>,
        raw: bool,
        hash_seq: bool,
    },
}

/// What the tags should be: a sorted map from name to value.
type Model = BTreeMap<Tag, HashAndFormat>;

/// Every sequence of tag operations has the same effect as on a sorted map.
///
/// The full list of tags is compared with the model after every operation.
pub fn tags_model(h: &Harness) -> anyhow::Result<()> {
    let strategy = proptest::collection::vec(op(), 1..20);
    h.check_prop(strategy, async |store, ops| {
        let mut model = Model::new();
        for (i, op) in ops.iter().enumerate() {
            apply(&store, &mut model, op)
                .await
                .with_context(|| format!("operation {i}: {op:?}"))?;
            let all = list(&store, None, None, true, true).await?;
            let expected = select(&model, &None, &None, true, true);
            ensure!(
                all == expected,
                "contract: tags behave like a sorted map; after operation {i} ({op:?}) the tags are {all:?}, expected {expected:?}"
            );
        }
        Ok(())
    })
}

async fn apply(store: &Store, model: &mut Model, op: &Op) -> anyhow::Result<()> {
    let tags = store.tags();
    match op {
        Op::Set(name, value) => {
            tags.set(name, *value).await?;
            model.insert(name.clone(), *value);
        }
        Op::Create(value) => {
            let name = tags.create(*value).await?;
            ensure!(
                !model.contains_key(&name),
                "contract: CreateTag returns a new name; got existing tag {name:?}"
            );
            model.insert(name, *value);
        }
        Op::Delete { from, to } => {
            let deleted = tags
                .delete_with_opts(DeleteTagsRequest {
                    from: from.clone(),
                    to: to.clone(),
                })
                .await?;
            let expected = select(model, from, to, true, true);
            for info in &expected {
                model.remove(&info.name);
            }
            ensure!(
                deleted == expected.len() as u64,
                "contract: DeleteTags returns the number of deleted tags; got {deleted}, expected {}",
                expected.len()
            );
        }
        Op::Rename { from, to } => {
            let result = tags.rename(from, to).await;
            match model.remove(from) {
                Some(value) => {
                    result.context("contract: renaming an existing tag succeeds")?;
                    model.insert(to.clone(), value);
                }
                None => ensure!(
                    result.is_err(),
                    "contract: renaming a tag that does not exist fails"
                ),
            }
        }
        Op::List {
            from,
            to,
            raw,
            hash_seq,
        } => {
            let listed = list(store, from.clone(), to.clone(), *raw, *hash_seq).await?;
            let expected = select(model, from, to, *raw, *hash_seq);
            ensure!(
                listed == expected,
                "contract: ListTags returns the tags in the range with a selected format, sorted by name; got {listed:?}, expected {expected:?}"
            );
        }
    }
    Ok(())
}

async fn list(
    store: &Store,
    from: Option<Tag>,
    to: Option<Tag>,
    raw: bool,
    hash_seq: bool,
) -> anyhow::Result<Vec<TagInfo>> {
    let request = ListTagsRequest {
        hash_seq,
        raw,
        from,
        to,
    };
    let items = store.client().rpc(request).await?;
    Ok(items.into_iter().collect::<Result<_, _>>()?)
}

/// The tags in the model that a list request with these options returns.
fn select(
    model: &Model,
    from: &Option<Tag>,
    to: &Option<Tag>,
    raw: bool,
    hash_seq: bool,
) -> Vec<TagInfo> {
    model
        .iter()
        .filter(|(name, _)| from.as_ref().is_none_or(|from| *name >= from))
        .filter(|(name, _)| to.as_ref().is_none_or(|to| *name < to))
        .filter(|(_, value)| match value.format {
            BlobFormat::Raw => raw,
            BlobFormat::HashSeq => hash_seq,
        })
        .map(|(name, value)| TagInfo::new(name, *value))
        .collect()
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        3 => (name(), value()).prop_map(|(name, value)| Op::Set(name, value)),
        1 => value().prop_map(Op::Create),
        2 => (option::of(name()), option::of(name())).prop_map(|(from, to)| Op::Delete { from, to }),
        2 => (name(), name()).prop_map(|(from, to)| Op::Rename { from, to }),
        2 => (option::of(name()), option::of(name()), any::<bool>(), any::<bool>())
            .prop_map(|(from, to, raw, hash_seq)| Op::List { from, to, raw, hash_seq }),
    ]
}

/// Short names from a small alphabet, so operations hit the same tags and
/// ranges overlap.
fn name() -> impl Strategy<Value = Tag> {
    "[ab]{0,3}".prop_map(Tag::from)
}

/// A small set of values. Tags may point to blobs the store does not have.
fn value() -> impl Strategy<Value = HashAndFormat> {
    let hash = (0u8..3).prop_map(|i| Hash::new([i]));
    let format = prop_oneof![Just(BlobFormat::Raw), Just(BlobFormat::HashSeq)];
    (hash, format).prop_map(|(hash, format)| HashAndFormat { hash, format })
}
