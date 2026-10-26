from smct.assessment import check_code, check_job, check_cluster, score
from smct.discovery import build_job
from smct.validation import compare_rows
from smct.governance import evaluate_cutover

def test_large_job_blocked():
    f=check_job({"tasks":[{"task_key":str(i)} for i in range(11)]})
    assert any(x.limitation == "L2" for x in f)
    assert not score(f)[1]

def test_cluster_flags():
    f=check_cluster({"spark_version":"12.2.x-scala2.12","docker_image":{"url":"example"},"init_scripts":[{"dbfs":{"destination":"dbfs:/something"}}]})
    assert {x.limitation for x in f} >= {"L3","L4"}

def test_source_flags():
    f=check_code("dbutils.fs.mount('/x','/y')\nspark.sql('select cast(x as int)')", "PYTHON")
    assert {x.limitation for x in f} >= {"L6","L13"}

def test_comparison_order_independent_duplicates_detected():
    assert compare_rows([{"a":1},{"a":2}], [{"a":2},{"a":1}])["passed"]
    assert not compare_rows([{"a":1},{"a":1}], [{"a":1},{"a":2}])["passed"]

def test_safety_gate():
    assert evaluate_cutover(True,True,True,True,True).allowed
    assert not evaluate_cutover(True,True,True,False,True).allowed

def test_job_mapping():
    w=build_job(123,{"name":"example","tasks":[{"task_key":"t","existing_cluster_id":"abc"}]})
    assert w.compute_mode=="classic"
