//! The public API: cases, the store factory and the test runner.
use std::{collections::BTreeMap, fmt, future::Future, pin::Pin, sync::Arc, time::Duration};

use iroh_blobs::api::Store;
use libtest_mimic::{Arguments, Failed, Trial};

use crate::{
    cases::CASES,
    harness::{Harness, Settings},
};

/// Number of generated inputs per case if `PROPTEST_CASES` is not set.
const DEFAULT_PROPTEST_CASES: u32 = 32;

/// Default time limit for one run of a case, including store creation and
/// shutdown.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;
pub(crate) type Factory = Arc<dyn Fn() -> BoxFuture<anyhow::Result<TestStore>> + Send + Sync>;

/// All conformance cases.
pub fn cases() -> &'static [Case] {
    CASES
}

/// A conformance case.
///
/// Get all of them with [`cases`], and run one with [`Testkit::run`].
#[derive(Clone, Copy)]
pub struct Case {
    name: &'static str,
    description: &'static str,
    run: fn(&Harness) -> anyhow::Result<()>,
}

impl Case {
    pub(crate) const fn new(
        name: &'static str,
        description: &'static str,
        run: fn(&Harness) -> anyhow::Result<()>,
    ) -> Self {
        Self {
            name,
            description,
            run,
        }
    }

    /// The name of the case, e.g. `export_bao`.
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// What the case checks, in one sentence.
    pub fn description(&self) -> &'static str {
        self.description
    }
}

impl fmt::Debug for Case {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Case")
            .field("name", &self.name)
            .field("description", &self.description)
            .finish_non_exhaustive()
    }
}

/// A store under test, created by the factory passed to [`Testkit::new`].
pub struct TestStore {
    pub(crate) store: Store,
    pub(crate) guard: Option<Box<dyn Send>>,
}

impl TestStore {
    /// Wraps a store, e.g. a [`MemStore`](iroh_blobs::store::mem::MemStore).
    pub fn new(store: impl Into<Store>) -> Self {
        Self {
            store: store.into(),
            guard: None,
        }
    }

    /// Keeps `guard` alive until the store has been shut down.
    ///
    /// Use this for resources the store needs, such as a temp dir or a server
    /// task.
    pub fn with_guard(mut self, guard: impl Send + 'static) -> Self {
        self.guard = Some(Box::new(guard));
        self
    }
}

impl fmt::Debug for TestStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TestStore")
            .field("store", &self.store)
            .finish_non_exhaustive()
    }
}

/// Runs conformance cases against stores from a factory.
#[derive(Clone)]
pub struct Testkit {
    factory: Factory,
    settings: Settings,
    skip: BTreeMap<&'static str, String>,
}

impl fmt::Debug for Testkit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Testkit")
            .field("settings", &self.settings)
            .field("skip", &self.skip)
            .finish_non_exhaustive()
    }
}

impl Testkit {
    /// Creates a testkit for stores created by `factory`.
    ///
    /// The factory is called for every generated input and must return a
    /// fresh, empty store each time.
    pub fn new<F, Fut>(factory: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = anyhow::Result<TestStore>> + Send + 'static,
    {
        let proptest_cases = std::env::var("PROPTEST_CASES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_PROPTEST_CASES);
        Self {
            factory: Arc::new(move || Box::pin(factory())),
            settings: Settings {
                proptest_cases,
                timeout: DEFAULT_TIMEOUT,
            },
            skip: BTreeMap::new(),
        }
    }

    /// Sets the number of generated inputs per case.
    ///
    /// Defaults to the `PROPTEST_CASES` environment variable, or 32.
    pub fn proptest_cases(mut self, cases: u32) -> Self {
        self.settings.proptest_cases = cases;
        self
    }

    /// Sets the time limit for one run of a case, including store creation
    /// and shutdown. Defaults to 30 seconds.
    ///
    /// A run that takes longer fails, since a store must answer every
    /// request.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.settings.timeout = timeout;
        self
    }

    /// Marks a case as skipped, e.g. because it tests a known bug.
    ///
    /// [`Testkit::main`] reports skipped cases as ignored, so they still run
    /// with `--include-ignored`.
    ///
    /// # Panics
    ///
    /// If there is no case named `case`.
    pub fn skip(mut self, case: &str, reason: impl Into<String>) -> Self {
        let case = cases()
            .iter()
            .find(|c| c.name == case)
            .unwrap_or_else(|| panic!("Testkit::skip: there is no case named {case:?}"));
        self.skip.insert(case.name, reason.into());
        self
    }

    /// The reason a case is skipped, if it is.
    pub fn skip_reason(&self, case: &Case) -> Option<&str> {
        self.skip.get(case.name).map(String::as_str)
    }

    /// Runs one case, whether or not it is skipped.
    ///
    /// On failure, the error states the violated contract and the minimal
    /// failing input.
    pub fn run(&self, case: &Case) -> anyhow::Result<()> {
        let harness = Harness::new(self.factory.clone(), self.settings);
        (case.run)(&harness)
    }

    /// One [`Trial`] per case, for running with [libtest-mimic].
    ///
    /// Skipped cases are marked as ignored.
    ///
    /// [libtest-mimic]: https://docs.rs/libtest-mimic
    pub fn trials(&self) -> Vec<Trial> {
        cases()
            .iter()
            .map(|case| {
                let testkit = self.clone();
                let case = *case;
                Trial::test(case.name, move || {
                    testkit
                        .run(&case)
                        .map_err(|cause| Failed::from(format!("{cause:#}")))
                })
                .with_ignored_flag(self.skip.contains_key(case.name))
            })
            .collect()
    }

    /// Runs all cases like `cargo test` does, then exits.
    ///
    /// Use this as the `main` of a test target with `harness = false`. It
    /// supports the usual test arguments, such as a name filter, `--list` and
    /// `--include-ignored`.
    pub fn main(&self) -> ! {
        let args = Arguments::from_args();
        if !args.list {
            for (name, reason) in &self.skip {
                eprintln!("skipping {name}: {reason}");
            }
        }
        libtest_mimic::run(&args, self.trials()).exit()
    }
}
