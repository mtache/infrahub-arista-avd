# Integration validation

Run these tests only when explicitly requested. Use the
`infrahub-run-integration-tests` skill to prepare an isolated remote worktree,
verify the committed SHA and locked dependencies, and build a branch-specific
image. Run stacks serially with the CI resource settings, including one Prefect
background service replica. Dependency sync and image builds precede pytest.

The suite retains 53 scenarios, including eleven seed selections and the optional
fabric scenario (skipped without `--fabric`). The performance target is less than
1,800 seconds for pytest including startup and teardown. Working budgets are
13 minutes for seeds, 12 minutes for E2E and five minutes for the remaining tests.

## Targeted runs

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
The fabric's `anta_enabled` and `dci_pool` fields are agnostic. Scenario cleanup
restores their initial values and deletes only its newly created DCI pool before
checking the source snapshot. FileObjects use the file download endpoint with
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

## Coverage and evidence

The seed matrix loads the schema once, forks an empty branch for every selection,
and invokes the actual `invoke load-objects` command, including Common each time.
It checks exact fabrics, profile/template selections and pod/rack references.
Branch-agnostic kinds are discovered through schema introspection; additions are
visible on the source during that case, then removed before the next selection
without touching built-in objects. Branch-aware nodes remain isolated throughout.

Idempotence still executes two passes for all racks, including `DC1_BORDER`, and
two hostvar passes. Generator requests contain the complete target set. Readiness
observes new task IDs and their cascades, including generator/artifact tasks
without branch metadata. Snapshot polling reads metadata; once settled,
comparisons read actual storage contents, checksums and IP assignments.
Storage downloads are cached per client and storage ID with at most two concurrent
reads. The targeted artifact endpoint receives artifact IDs rather than device
IDs. Rack snapshots paginate all collections, preserve both passes on disk, and
show the divergent records on failure. Each artifact definition settles before
the next one is triggered. Transient reads have bounded retries and respect `Retry-After`; permanent
GraphQL errors and failed tasks stop the test immediately. Mutations are never
replayed by the read retry helper.

Each container fixture writes `instance.log`, `tasks.json`, failed-task logs and
any diagnostic error beneath its temporary directory before teardown. Preserve
these files, pytest output and JUnit results for every run. `--durations=0` reports
setup, call and teardown separately. Compare timings by test file against the
original failure report: seed matrix 1,367.81 s (612.84 s setup), E2E 2,570.54 s,
and other tests 316.99 s. Separate avoided failure timeouts from reduced successful
execution costs when reporting the gain.

The full-stack CI job remains manual and includes the eleven seed selections.
