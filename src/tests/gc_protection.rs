use crate::{api::proto::FinishGcProtectionOutcome, store::GcProtectionSet, Hash};

#[test]
fn stale_finish_cannot_release_a_newer_gc_cycle() {
    let mut protection = GcProtectionSet::default();
    let first = protection.start().expect("first cycle must start");
    let first_hash = Hash::new(b"first cycle");
    protection.protect(first_hash);
    assert!(protection.contains(&first_hash));
    assert!(
        protection.start().is_err(),
        "only one garbage-collection cycle may own the protection set"
    );

    assert_eq!(
        protection.finish(first),
        FinishGcProtectionOutcome::Finished
    );
    let second = protection.start().expect("second cycle must start");
    let second_hash = Hash::new(b"second cycle");
    protection.protect(second_hash);

    assert_eq!(
        protection.finish(first),
        FinishGcProtectionOutcome::Stale,
        "delayed cleanup from an older cycle must be idempotent"
    );
    assert!(
        protection.contains(&second_hash),
        "stale cleanup must not release the active cycle"
    );
    assert_eq!(
        protection.finish(second),
        FinishGcProtectionOutcome::Finished
    );
    assert!(!protection.contains(&second_hash));
}
