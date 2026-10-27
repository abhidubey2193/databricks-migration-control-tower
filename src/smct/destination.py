"""Resolve a writable namespace selection without assuming a main catalog exists."""
from .ab import IDENT


def resolve_destination(spark, requested_catalog='', requested_schema='serverless_migration'):
    catalog=requested_catalog.strip()
    schema=requested_schema.strip()
    if not catalog or catalog.lower()=='auto':
        catalog=spark.sql('SELECT current_catalog()').first()[0]
    if not isinstance(catalog,str) or not IDENT.fullmatch(catalog) or not IDENT.fullmatch(schema):
        raise ValueError('Catalog and schema must use plain letters, numbers and underscores, starting with a letter or underscore')
    available=sorted(row[0] for row in spark.sql('SHOW CATALOGS').collect())
    if catalog not in available:
        raise ValueError(f"Catalog {catalog!r} is unavailable. Set the catalog widget to 'auto' to use the current catalog, or choose an accessible catalog: {', '.join(available)}")
    if catalog.lower() in ('system','samples','hive_metastore'):
        raise ValueError(f"Catalog {catalog!r} is not an appropriate Unity Catalog evidence destination. Select an existing writable Unity Catalog catalog: {', '.join(available)}")
    return catalog,schema
