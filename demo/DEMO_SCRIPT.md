# Five-minute live demo checklist

Only use a mock or internal sandbox. Never publish real customer paths, identities or hostnames.

0:00-0:30 — Architecture, supported limitations and safety boundaries.
0:30-1:30 — `smct scan` and Streamlit inventory, blockers and readiness.
1:30-2:30 — `smct approve`, `smct clone` dry-run and `smct clone --apply` on a safe 1-task sandbox job; show clone trigger absent.
2:30-3:30 — Compare isolated outputs using `smct validate`; present one pass and one fail; explain row caps and side-effect gap.
3:30-4:30 — `smct gate` and audit records, show original job unmodified.
4:30-5:00 — `databricks bundle run smct_fleet_scan` result, known gaps, future enhancements.

Do not claim execution or proof you have not completed in a real environment.
