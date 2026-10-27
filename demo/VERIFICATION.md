# Verification record

27 October 2026. Local environment: Python 3.12, Databricks SDK 0.157.0, Streamlit 1.66.1, SQL connector 4.8.0, Databricks CLI 1.22.1.

| Check | Actual result |
|---|---|
| Untouched enhanced branch ee73f1d test suite in isolated archive | 11 passed |
| Updated unit/regression/dashboard suite | 47 passed, 0 failed |
| Editable project install with app/dev extras | Succeeded |
| Python compilation and diff whitespace check | Passed |
| `python -m smct.cli demo --output demo-output` | PASS scenario true; FAIL scenario false; original unchanged; clone triggers absent |
| SDK model round trip | Clone task payload/environment preserved |
| Streamlit AppTest | Seven tabs, synthetic banner, no runtime exception |
| Official CLI Bundle validate | Blocked by absent default workspace credentials; not a successful validation |
| Workspace Spark validation notebook | User-provided final output at 2026-10-27T15:09:18.584506+00:00: matching PASS, intentional FAIL; see workspace-validation-evidence.json |
| Delta evidence persistence | Notebook reached final print after write calls; saved tables not independently queried |
| Live Bundle / App deployment and compute selection | Not independently verified; no run ID/compute metadata supplied |
| Genuine classic execution / A/B jobs | Not verified |
| Demo video | Not recorded |
| Repository visibility | Public at inspection; unchanged |
| Judge Admin access / submission email | Not changed / not sent |

Tests cover typed and empty schemas, duplicate/key groups, nulls, type collisions, non-finite values, SQL identifier rejection, table cap, same-table rejection, stale source/cluster configurations, trigger types, unsupported task fields, partial discovery, backup corruption, wave selection and persistent synthetic evidence.

**NO-GO for submission scoring:** workspace validation now has user-reported execution evidence, but fleet inventory, dashboard deployment and the mandatory demo video remain unverified or incomplete. Authentication succeeded in a personal trial sandbox; the console subsequently blocked automated browser control, and the Genie connection could not be enabled in this session. No genuine classic run is claimed. Partner Academy Medallion Labs classic compute availability is unverified.
