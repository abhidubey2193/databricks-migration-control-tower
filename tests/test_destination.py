import pytest
from smct.destination import resolve_destination

class Result:
    def __init__(self,rows): self.rows=rows
    def first(self): return self.rows[0]
    def collect(self): return self.rows

class Spark:
    def __init__(self,current='workspace',catalogs=('workspace','system')):
        self.current=current;self.catalogs=catalogs;self.queries=[]
    def sql(self,query):
        self.queries.append(query)
        if query=='SELECT current_catalog()': return Result([(self.current,)])
        if query=='SHOW CATALOGS': return Result([(c,) for c in self.catalogs])
        raise AssertionError(query)

@pytest.mark.parametrize('requested',['','auto',' AUTO '])
def test_auto_uses_actual_current_catalog(requested):
    spark=Spark()
    assert resolve_destination(spark,requested)==('workspace','serverless_migration')
    assert not any('main' in q or 'CREATE' in q for q in spark.queries)

def test_explicit_valid_catalog_preserved():
    spark=Spark(catalogs=('workspace','sandbox'))
    assert resolve_destination(spark,'sandbox','evidence')==('sandbox','evidence')
    assert spark.queries==['SHOW CATALOGS']

def test_missing_main_lists_catalogs_without_silent_fallback():
    with pytest.raises(ValueError,match="Set the catalog widget to 'auto'.*workspace"):
        resolve_destination(Spark(),'main')

@pytest.mark.parametrize('catalog',['system','samples','hive_metastore'])
def test_read_only_or_non_uc_default_rejected(catalog):
    with pytest.raises(ValueError,match='not an appropriate'):
        resolve_destination(Spark(current=catalog,catalogs=(catalog,'workspace')),'auto')

def test_injected_schema_rejected_before_query():
    spark=Spark()
    with pytest.raises(ValueError): resolve_destination(spark,'workspace','s; DROP SCHEMA x')
    assert not spark.queries
