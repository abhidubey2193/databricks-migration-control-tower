"""Compare explicit isolated Delta output tables via SQL warehouse. Never launches production workloads."""
import re
from .validation import compare_rows

IDENT=re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')

def safe_table(name):
    parts=name.split('.')
    if len(parts)!=3 or not all(IDENT.fullmatch(x) for x in parts):
        raise ValueError('Use plain three-part catalog.schema.table names only')
    return '.'.join('`'+x+'`' for x in parts)

def warehouse_compare(connection,classic_table,serverless_table,max_rows=10000,ignore_columns=()):
    if max_rows<1 or max_rows>100000: raise ValueError('max_rows must be 1..100000')
    output=[]
    for name in (classic_table,serverless_table):
        with connection.cursor() as cur:
            cur.execute(f'SELECT * FROM {safe_table(name)} LIMIT {max_rows+1}')
            columns=[col[0] for col in cur.description]
            rows=cur.fetchall()
        if len(rows)>max_rows: raise ValueError(f'{name} exceeds {max_rows} rows; no PASS recorded')
        output.append([dict(zip(columns,row)) for row in rows])
    result=compare_rows(output[0],output[1],ignore_columns)
    result.update({'classic_table':classic_table,'serverless_table':serverless_table,'method':'bounded full-table record comparison','max_rows':max_rows,'isolated_outputs_required':True})
    return result

def connect_and_compare(host,http_path,classic,serverless,max_rows=10000):
    from databricks import sql
    from databricks.sdk.core import Config
    config=Config()
    from databricks.sdk import WorkspaceClient
    w=WorkspaceClient(config=config)
    with sql.connect(server_hostname=host or config.host.replace('https://',''),http_path=http_path,credentials_provider=lambda: w.config.authenticate) as connection:
        return warehouse_compare(connection,classic,serverless,max_rows)
