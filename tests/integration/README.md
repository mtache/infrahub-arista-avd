# Integration validation

Run these tests only when explicitly requested. Use the
`infrahub-run-integration-tests` skill to prepare an isolated remote worktree,
verify the committed SHA and locked dependencies, and build a branch-specific
image. Run stacks serially with the CI resource settings, including one Prefect
background service replica. Dependency sync and image builds precede pytest.

The suite retains 53 scenarios, including eleven seed selections and a default
L2LS deployment scenario. Four separate stacks cover the nine schema contracts,
three standalone loading cases, 22 E2E cases and eleven seed selections; the
eight PyAVD/import cases do not need a stack. The L2LS scenario checks generated
roles, MLAG on both tiers and pure Layer-2 EOS configurations, sharing the E2E
class's stack and artifacts. The performance target is less than 1,800 seconds
for pytest including startup and
teardown. Working budgets are
13 minutes for seeds, 12 minutes for E2E and five minutes for the remaining tests.

## Targeted runs

### Schema contracts

The ASN and device-design contracts share `TestSchemaContracts`, one schema load
and one class-scoped event loop. Every selected case requests the schema fixture,
so no preceding test is needed. The ASN uniqueness case deletes only its own
node in `finally`. This stack loads no seeds, repository or triggers.

The previous ASN/device-design node IDs now use the prefix
`tests/integration/test_schema_contracts.py::TestSchemaContracts::`; method names
and parameter IDs are unchanged. Run either case alone with a fresh temporary
directory, then select both in either order when validating their independence:

```bash
uv run --no-sync pytest \
  tests/integration/test_schema_contracts.py::TestSchemaContracts::test_asn_is_globally_unique \
  tests/integration/test_schema_contracts.py::TestSchemaContracts::test_generic_shape \
  -v --capture=tee-sys --durations=0 --durations-min=0 \
  --basetemp=/tmp/infrahub-it-schema-new --junitxml=/tmp/infrahub-it-schema-junit.xml
```

### E2E scenarios

Each E2E node ID selects its prerequisites through lazy fixtures; preceding test
methods no longer need to be selected. Only the fabric generator initiates the
baseline. Pod, rack, structured configuration and backfill generation still run
through the real triggers. At most two fabrics cascade concurrently; each batch
settles before the next kickoff, with one stack and the same CI resources. Graph
mutations use distinct branches from the generated baseline, published once into
the temporary default branch because Infrahub 1.11 requires that origin. The
source graph is checked after each scenario. Artifact tests prepare their own
definitions only when selected. They use the pipeline branch while it is open,
or the temporary default branch after publication.
The fabric's `anta_enabled` flag is branch-aware. Its proposed-change scenario
prepares a disabled destination before forking, enables the source, checks that
the destination remains disabled, then restores the original baseline. The
`dci_pool` field is agnostic; cleanup restores its initial value and deletes only
any newly created DCI pool before checking the source snapshot. The DCI scenario
uses both border pairs in `Fabric-L3LS-Multi-Domain`, verifies its four seeded
inter-DC links, then adds one cross-DC link with a `/31` from the existing pool.
It verifies complete `l3_edge` contents for all four borders and materializes
single-valued relationships after the scenario. FileObjects use the file download endpoint with
their branch; rendered artifact content uses the object-storage endpoint.

```bash
uv run --no-sync pytest \
  tests/integration/test_e2e_pipeline.py::TestE2EPipeline::test_dci_link_generates_l3_edge_hostvars \
  -v --capture=tee-sys --durations=0 --durations-min=0 \
  --basetemp=/tmp/infrahub-it-dci-new --junitxml=/tmp/infrahub-it-dci-junit.xml
```

Use a fresh external temporary directory per invocation. Rerun only the affected
lot while investigating; run the complete suite after all affected lots pass.
Test two E2E nodes separately and in reversed order before the complete gate.

Select the L2LS deployment check directly without a fabric option:

```bash
uv run --no-sync pytest \
  tests/integration/test_e2e_pipeline.py::TestE2EPipeline::test_l2ls_deployment \
  -v --capture=tee-sys --durations=0 --durations-min=0 \
  --basetemp=/tmp/infrahub-it-l2ls-new --junitxml=/tmp/infrahub-it-l2ls-junit.xml
```

## Coverage and evidence

The seed matrix loads the schema once, forks an empty branch for every selection,
and invokes the actual `invoke load-objects` command, including Common each time.
It checks exact fabrics, profile/template selections and pod/rack references.
Branch-agnostic kinds are discovered through schema introspection; additions are
visible on the source during that case, then removed before the next selection
without touching built-in objects. Branch-aware nodes remain isolated throughout.

Idempotence still executes two passes for all racks, including `DC1_BORDER`, and
two hostvar passes. Generator requests contain the complete target set. Readiness
initializes branch-local hostvar instances even when files were inherited, then
keeps later rack regeneration targeted. It
observes new task IDs and their cascades, including generator/artifact tasks
without branch metadata. Snapshot polling reads metadata; once settled,
comparisons read actual storage contents, checksums and IP assignments.
IP uniqueness checks include primary interface assignments. Sharing is accepted
only for the same VTEP IP on the two virtual `Loopback1` interfaces of an exact
two-peer MLAG domain, with matching device VTEP references; other duplicates fail.
Storage downloads are cached per client and storage ID with at most two concurrent
reads. The targeted artifact endpoint receives artifact IDs rather than device
IDs. Rack snapshots paginate all collections, preserve both passes on disk, and
show the divergent records on failure. Definitions already rendered by a merge
are observed without generating them again. A definition is Ready only when all
of its existing outputs are stored and Ready. Missing definitions settle before
the next one is triggered. Transient reads have bounded retries and respect `Retry-After`; permanent
GraphQL errors and failed tasks stop the test immediately. Mutations are never
replayed by the read retry helper.
Artifact POSTs retry only an explicit `X-Infrahub-Admission: shed` rejection,
which guarantees that the handler did not run. These attempts respect `Retry-After`
and a deadline; unmarked errors and lost responses fail without replay.

Each container fixture writes `instance.log`, `tasks.json`, failed-task logs and
any diagnostic error beneath its temporary directory before teardown. Preserve
these files, pytest output and JUnit results for every run. `--durations=0` reports
setup, call and teardown separately. Compare timings by test file against the
original failure report: seed matrix 1,367.81 s (612.84 s setup), E2E 2,570.54 s,
and other tests 316.99 s. Separate avoided failure timeouts from reduced successful
execution costs when reporting the gain.

The full-stack CI job remains manual and selects `tests/integration`, covering
all 53 scenarios, including the nine schema contracts and eleven seed selections.

## Performance comparison

Sharing the schema contracts avoids one stack lifecycle and one initial schema
load. Its runtime gain remains to be measured; this change does not establish
that the complete suite meets the 1,800-second target.

After explicit authorization, compare a baseline that already runs L2LS by
default with the grouped candidate. Use the same existing image, resources,
53 active scenarios and logical order, with fresh evidence directories and new
jobs. Map only the nine moved schema node IDs between revisions.

Use the same external measurement plugin for both runs. Record monotonic,
non-overlapping intervals for Compose startup through healthy services, port
discovery, schema loading/convergence, seeds, repository synchronization,
triggers, cascades, artifact rendering and baseline publication. Separate
branch/global-object cleanup and evidence collection from Compose shutdown.
Startup includes server initialization; E2E fixture setup also includes data
loading and cascades and must not be reported as Docker boot time.
