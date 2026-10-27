"""Exact, duplicate-aware comparison. Synthetic evidence is never live execution."""
import base64
import hashlib
import json
import math
from collections import Counter
from datetime import date, datetime
from decimal import Decimal


def _value(value):
    if value is None: return ['null']
    if isinstance(value, bool): return ['bool', value]
    if isinstance(value, int): return ['int', str(value)]
    if isinstance(value, float):
        if not math.isfinite(value): raise ValueError('Non-finite values require explicit review')
        return ['float', value.hex()]
    if isinstance(value, Decimal): return ['decimal', str(value)]
    if isinstance(value, datetime): return ['datetime', value.isoformat()]
    if isinstance(value, date): return ['date', value.isoformat()]
    if isinstance(value, str): return ['string', value]
    if isinstance(value, bytes): return ['binary', base64.b64encode(value).decode()]
    if isinstance(value, (list, tuple)): return ['array', [_value(v) for v in value]]
    if isinstance(value, dict):
        if not all(isinstance(k, str) for k in value): raise ValueError('Object keys must be strings')
        return ['object', [[k, _value(v)] for k, v in sorted(value.items())]]
    raise ValueError(f'Unsupported comparison type: {type(value).__name__}')


def _record(row, ignore_columns=()):
    return json.dumps(_value({k:v for k,v in row.items() if k not in ignore_columns}), separators=(',', ':'))


def fingerprint(rows, ignore_columns=()):
    hashes=Counter(hashlib.sha256(_record(row, ignore_columns).encode()).hexdigest() for row in rows)
    columns=sorted({k for row in rows for k in row if k not in ignore_columns})
    return {'row_count':len(rows), 'columns':columns, 'record_hashes':hashes}


def compare_rows(classic, serverless, ignore_columns=(), *, classic_schema=None, serverless_schema=None, key_columns=()):
    before=fingerprint(classic, ignore_columns)
    after=fingerprint(serverless, ignore_columns)
    missing=sum((before['record_hashes']-after['record_hashes']).values())
    extra=sum((after['record_hashes']-before['record_hashes']).values())
    columns_equal=before['columns']==after['columns']
    typed_schema_compared=classic_schema is not None and serverless_schema is not None
    schema_equal=classic_schema==serverless_schema if typed_schema_compared else columns_equal
    columns=sorted(set(before['columns'])|set(after['columns']))
    def nulls(rows): return {c:sum(r.get(c) is None for r in rows) for c in columns}
    result={'passed':columns_equal and schema_equal and not missing and not extra,
            'classic_rows':len(classic), 'serverless_rows':len(serverless), 'columns_equal':columns_equal,
            'schema_equal':schema_equal, 'typed_schema_compared':typed_schema_compared,
            'classic_schema':classic_schema, 'serverless_schema':serverless_schema,
            'missing_records':missing, 'extra_records':extra,
            'classic_null_counts':nulls(classic), 'serverless_null_counts':nulls(serverless),
            'thresholds':{'missing_records':0,'extra_records':0}, 'ignored_columns':list(ignore_columns),
            'comparison_version':2}
    if key_columns:
        if set(key_columns)&set(ignore_columns): raise ValueError('Keys cannot be ignored')
        def index(rows):
            groups={}
            for r in rows:
                if not all(k in r for k in key_columns): raise ValueError('Key column missing')
                key=_record({k:r[k] for k in key_columns})
                groups.setdefault(key, Counter())[_record(r, ignore_columns)]+=1
            return groups
        left,right=index(classic),index(serverless)
        result['key_columns']=list(key_columns)
        result['changed_key_groups']=sum(left.get(k)!=right.get(k) for k in left.keys()|right.keys())
        result['duplicate_key_groups']={'classic':sum(sum(v.values())>1 for v in left.values()),'serverless':sum(sum(v.values())>1 for v in right.values())}
    return result
