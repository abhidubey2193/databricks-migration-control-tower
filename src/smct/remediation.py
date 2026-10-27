"""Reviewable dependency and source changes. Never executes retrieved scripts."""

import ast
import base64
import copy
import difflib
import hashlib
import re
import shlex

from .ab import safe_table

MAX_SCRIPT_BYTES = 65536


def read_init_script(w, uri, *, s3_client=None):
    """Read using caller credentials; bounded reads, no shell or credential export."""
    if uri.startswith("dbfs:/"):
        response = w.dbfs.read(uri[5:], length=MAX_SCRIPT_BYTES + 1)
        data = base64.b64decode(response.data or "")
    elif uri.startswith("s3://"):
        bucket, sep, key = uri[5:].partition("/")
        if not bucket or not sep or not key:
            raise ValueError("S3 script requires bucket and key")
        if s3_client is None:
            import boto3

            s3_client = boto3.client("s3")
        response = s3_client.get_object(Bucket=bucket, Key=key)
        try:
            data = response["Body"].read(MAX_SCRIPT_BYTES + 1)
        finally:
            response["Body"].close()
    elif uri.startswith(("/Workspace/", "/Volumes/")):
        if uri.startswith("/Volumes/"):
            response = w.files.download(uri)
            try:
                data = response.contents.read(MAX_SCRIPT_BYTES + 1)
            finally:
                response.contents.close()
        else:
            with w.workspace.download(uri) as stream:
                data = stream.read(MAX_SCRIPT_BYTES + 1)
    else:
        raise ValueError("Supported script locations: S3, DBFS, Workspace or Volume")
    if len(data) > MAX_SCRIPT_BYTES:
        raise ValueError("Init script exceeds 64 KiB review limit")
    return data.decode("utf-8")


def dependency_plan(script):
    """Translate only simple pinned pip installs; every other command blocks apply."""
    dependencies, unresolved = [], []
    for number, line in enumerate(script.splitlines(), 1):
        line = line.strip()
        if (
            not line
            or line.startswith("#")
            or line in ("set -e", "set -eu", "set -euo pipefail")
        ):
            continue
        if any(x in line for x in ("$", "`", ";", "|", "&", ">", "<", "\\")):
            unresolved.append(
                {"line": number, "reason": "Shell expansion/control requires review"}
            )
            continue
        try:
            words = shlex.split(line, comments=True)
        except ValueError:
            words = []
        prefix = next(
            (
                p
                for p in (
                    ["pip", "install"],
                    ["pip3", "install"],
                    ["python", "-m", "pip", "install"],
                    ["python3", "-m", "pip", "install"],
                )
                if words[: len(p)] == p
            ),
            None,
        )
        packages = words[len(prefix) :] if prefix else []
        valid = packages and all(
            re.fullmatch(
                r"[A-Za-z0-9][A-Za-z0-9_.-]*(?:\[[A-Za-z0-9_,.-]+\])?==[A-Za-z0-9_.+!-]+",
                p,
            )
            or re.fullmatch(r"/Volumes/[A-Za-z0-9_./-]+\.whl", p)
            for p in packages
        )
        if not valid:
            unresolved.append(
                {
                    "line": number,
                    "reason": "Only pinned packages or Volume wheels can be translated",
                }
            )
        else:
            dependencies.extend(packages)
    return {
        "dependencies": sorted(set(dependencies)),
        "unresolved": unresolved,
        "can_apply": not unresolved,
        "script_sha256": hashlib.sha256(script.encode()).hexdigest(),
    }


def translate_job(
    settings, *, script_plans=(), library_replacements=None, task_scoped=False
):
    """Return reviewed source + dependency spec. Original stays unchanged.

    Custom images, profiles, ML runtimes and Spark settings still block the compiler.
    Replacement packages must be explicit; JAR/egg conversion is never guessed.
    task_scoped=True returns a task-key-to-dependencies map for isolated environments;
    the default flat list is retained for existing callers.
    """
    result = copy.deepcopy(settings)
    deps = set()
    per_task = {}
    cluster_deps = {}
    plans = {p["uri"]: p for p in script_plans}
    clusters = [t.get("new_cluster", {}) for t in result.get("tasks", [])]
    clusters += [c.get("new_cluster", {}) for c in result.get("job_clusters", [])]
    for cluster in clusters:
        local_deps = set()
        for script in cluster.get("init_scripts", []):
            entries = [
                v.get("destination") for v in script.values() if isinstance(v, dict)
            ]
            if (
                len(entries) != 1
                or entries[0] not in plans
                or not plans[entries[0]].get("can_apply")
            ):
                raise ValueError(
                    "Every init script requires a complete reviewed translation"
                )
            local_deps.update(plans[entries[0]]["dependencies"])
        deps.update(local_deps)
        cluster_deps[id(cluster)] = local_deps
        cluster.pop("init_scripts", None)
    replacements = library_replacements or {}
    shared = {
        c["job_cluster_key"]: c.get("new_cluster", {})
        for c in result.get("job_clusters", [])
    }
    for task in result.get("tasks", []):
        cluster = task.get("new_cluster") or shared.get(task.get("job_cluster_key"), {})
        task_deps = set(cluster_deps.get(id(cluster), ()))
        for library in task.get("libraries", []):
            if set(library) == {"pypi"} and set(library["pypi"]) == {"package"}:
                package = library["pypi"]["package"]
            elif set(library) == {"whl"} and library["whl"].startswith("/Volumes/"):
                package = library["whl"]
            else:
                key = str(
                    library.get("egg")
                    or library.get("jar")
                    or (library.get("maven") or {}).get("coordinates")
                    or ""
                )
                if not key or key not in replacements:
                    raise ValueError(
                        "Unsupported library needs an explicit wheel/PyPI replacement"
                    )
                package = replacements[key]
            if not dependency_plan("pip install " + shlex.quote(package))["can_apply"]:
                raise ValueError(
                    "Dependency must be pinned PyPI or an accessible Volume wheel"
                )
            deps.add(package)
            task_deps.add(package)
        task.pop("libraries", None)
        if task_scoped:
            key = task["task_key"]
            if key in per_task:
                raise ValueError("Duplicate task key")
            per_task[key] = sorted(task_deps)
    return result, per_task if task_scoped else sorted(deps)


def assign_task_environments(candidate, dependencies):
    """Isolate reviewed dependencies; tasks with identical sets share one environment."""
    result = copy.deepcopy(candidate)
    keys = {t["task_key"] for t in result["tasks"]}
    if set(dependencies) != keys:
        raise ValueError("Dependency plan must cover every task exactly")
    groups = {}
    for task in result["tasks"]:
        packages = dependencies[task["task_key"]]
        if not isinstance(packages, (list, tuple)) or any(
            not isinstance(p, str)
            or not dependency_plan("pip install " + shlex.quote(p))["can_apply"]
            for p in packages
        ):
            raise ValueError("Unreviewable environment dependency")
        versions = {}
        for package in packages:
            if package.startswith("/Volumes/"):
                continue
            name, version = package.split("==", 1)
            name = re.sub(r"[-_.]+", "-", name.split("[", 1)[0]).lower()
            if name in versions and versions[name] != version:
                raise ValueError(
                    "Conflicting package pins within task " + task["task_key"]
                )
            versions[name] = version
        group = tuple(sorted(set(packages)))
        groups.setdefault(group, []).append(task)
    environments = []
    for i, (packages, tasks) in enumerate(sorted(groups.items()), 1):
        key = "smct_env" if len(groups) == 1 else f"smct_env_{i}"
        for task in tasks:
            task["environment_key"] = key
        environments.append(
            {
                "environment_key": key,
                "spec": {"environment_version": "2", "dependencies": list(packages)},
            }
        )
    result["environments"] = environments
    return result


def rewrite_python(
    source, *, delta_views=None, remove_cache=False, available_now=False
):
    """AST-based opt-in proposal; caller reviews the diff and validates semantics.

    Deliberately accepts pure Python only. Mixed-language notebooks fail rather
    than rewriting SQL strings, comments, Scala or R as though they were Python.
    """
    tree = ast.parse(source)
    mappings = delta_views or {}
    for name, table in mappings.items():
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
            raise ValueError("Global view names must be plain identifiers")
        safe_table(table)

    class Rewriter(ast.NodeTransformer):
        def visit_Call(self, node):
            node = self.generic_visit(node)
            if not isinstance(node.func, ast.Attribute):
                return node
            method = node.func.attr
            if (
                remove_cache
                and method in ("cache", "persist")
                and not node.args
                and not node.keywords
            ):
                return node.func.value
            if (
                method in ("createGlobalTempView", "createOrReplaceGlobalTempView")
                and len(node.args) == 1
                and isinstance(node.args[0], ast.Constant)
            ):
                name = node.args[0].value
                if name not in mappings:
                    raise ValueError(
                        "Every global view needs an explicit Delta table mapping"
                    )
                mode = (
                    "overwrite"
                    if method.startswith("createOrReplace")
                    else "errorifexists"
                )
                if mode == "errorifexists":
                    raise ValueError(
                        "createGlobalTempView requires explicit table-existence semantics; review manually"
                    )
                expr = ast.parse(
                    f"_frame.write.format('delta').mode('overwrite').saveAsTable({mappings[name]!r})",
                    mode="eval",
                ).body
                expr.func.value.func.value.func.value.value = node.func.value
                return ast.copy_location(expr, node)
            if (
                method in ("table", "sql")
                and node.args
                and isinstance(node.args[0], ast.Constant)
                and isinstance(node.args[0].value, str)
            ):
                text = node.args[0].value
                for name, table in mappings.items():
                    text = re.sub(
                        r"\bglobal_temp\." + re.escape(name) + r"\b", table, text
                    )
                node.args[0].value = text
            if available_now and method in ("start", "toTable"):
                chain = ast.unparse(node.func.value)
                if "writeStream" in chain and ".trigger(" not in chain:
                    node.func.value = ast.Call(
                        ast.Attribute(node.func.value, "trigger", ast.Load()),
                        [],
                        [ast.keyword("availableNow", ast.Constant(True))],
                    )
            return node

    changed = ast.unparse(ast.fix_missing_locations(Rewriter().visit(tree))) + "\n"
    return {
        "source": changed,
        "diff": "".join(
            difflib.unified_diff(
                source.splitlines(True),
                changed.splitlines(True),
                fromfile="original.py",
                tofile="proposed.py",
            )
        ),
        "requires_review": True,
        "original_sha256": hashlib.sha256(source.encode()).hexdigest(),
    }


def rewrite_sql(source, *, tolerant_casts=False, tolerant_division=False):
    """Opt-in parsed SQL proposal. TRY_* changes failure/null semantics: A/B required."""
    import sqlglot
    from sqlglot import exp

    statements = sqlglot.parse(source, read="databricks")

    def replace(node):
        if tolerant_casts and type(node) is exp.Cast:
            return exp.TryCast(this=node.this.copy(), to=node.args["to"].copy())
        if tolerant_division and type(node) is exp.Div:
            return exp.Anonymous(
                this="TRY_DIVIDE",
                expressions=[node.this.copy(), node.expression.copy()],
            )
        return node

    output = (
        ";\n".join(
            s.transform(replace).sql(dialect="databricks", pretty=True)
            for s in statements
            if s
        )
        + ";\n"
    )
    return dict(
        source=output,
        requires_review=True,
        original_sha256=hashlib.sha256(source.encode()).hexdigest(),
        diff="".join(
            difflib.unified_diff(
                source.splitlines(True),
                output.splitlines(True),
                fromfile="original.sql",
                tofile="proposed.sql",
            )
        ),
    )


def jar_candidate(settings, *, jars, environment_version="5"):
    """Explicit JAR packaging path for current serverless support; no guessed conversion."""
    from .orchestration import TRIGGERS, validate_task_graph

    if not jars or any(
        not re.fullmatch(r"/Volumes/[A-Za-z0-9_./-]+\.jar", path) for path in jars
    ):
        raise ValueError("Explicit Unity Catalog Volume JAR paths required")
    if not str(environment_version).isdecimal() or int(environment_version) < 4:
        raise ValueError("Serverless JAR environment must be version 4 or higher")
    result = copy.deepcopy(settings)
    tasks = result.get("tasks", [])
    validate_task_graph(tasks)
    if not tasks or any(not t.get("spark_jar_task") for t in tasks):
        raise ValueError("JAR-only job required for this explicit packaging proposal")
    for field in TRIGGERS:
        result.pop(field, None)
    # Original runtime/cluster blockers are surfaced separately; these artifacts
    # must be rebuilt for matching Databricks Connect/Scala/JDK versions.
    result = {
        k: v for k, v in result.items() if k in ("name", "description", "tasks", "tags")
    }
    result["name"] = "[SMCT JAR REVIEW - NO SCHEDULE] " + settings.get(
        "name", "JAR job"
    )
    for task in tasks:
        if not task["spark_jar_task"].get("main_class_name"):
            raise ValueError("Explicit JAR main class required")
        for field in (
            "new_cluster",
            "existing_cluster_id",
            "job_cluster_key",
            "libraries",
        ):
            task.pop(field, None)
        task["environment_key"] = "smct_jar"
    result.update(
        environments=[
            dict(
                environment_key="smct_jar",
                spec=dict(
                    environment_version=str(environment_version),
                    java_dependencies=sorted(set(jars)),
                ),
            )
        ],
        max_concurrent_runs=1,
    )
    return dict(
        candidate=result,
        requires_review=True,
        requirements=[
            "Rebuild JAR for serverless Databricks Connect, Scala and JDK versions",
            "Run isolated A/B verification before cutover",
        ],
    )
