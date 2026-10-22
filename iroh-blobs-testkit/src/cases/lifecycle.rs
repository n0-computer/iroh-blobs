//! Store wide requests.
use super::{check, setup};
use crate::{harness::Harness, inputs};

pub fn sync_and_wait_idle(h: &Harness) -> anyhow::Result<()> {
    h.check_prop(inputs::blob(), async |store, blob| {
        let reference = blob.reference();
        let _tt = setup::add(&store, &reference).await?;
        store.sync_db().await?;
        store.wait_idle().await?;
        check::complete(&store, &reference).await
    })
}
