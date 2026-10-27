"""Cooperative Delta lease on one pre-created row, renewed before journal writes.

Delta UPDATE conflicts serialize lease ownership. Requires restricted table ACLs;
workspace administrators can bypass it, just as they can edit Jobs directly.
"""

import uuid
from .ab import safe_table


class DeltaLease:
    def __init__(self, spark, table, ttl_seconds=900):
        if type(ttl_seconds) is not int or not 60 <= ttl_seconds <= 3600:
            raise ValueError("Lease TTL must be 60..3600 seconds")
        self.spark, self.table, self.ttl = spark, safe_table(table), ttl_seconds
        self.owner = uuid.uuid4().hex
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {self.table} USING DELTA AS SELECT CAST(NULL AS STRING) AS owner, TIMESTAMP '1970-01-01 00:00:00' AS lease_until"
        )
        if spark.table(self.table).count() != 1:
            raise RuntimeError(
                "Lease table must contain exactly one row; stop and repair"
            )

    def acquire(self):
        self.spark.sql(
            f"UPDATE {self.table} SET owner='{self.owner}', lease_until=current_timestamp() + INTERVAL {self.ttl} SECONDS WHERE lease_until < current_timestamp() OR owner='{self.owner}'"
        )
        self.assert_owned()
        return self

    def assert_owned(self):
        rows = self.spark.sql(
            f"SELECT owner, lease_until > current_timestamp() AS live FROM {self.table}"
        ).collect()
        if len(rows) != 1 or rows[0]["owner"] != self.owner or not rows[0]["live"]:
            raise RuntimeError(
                "Another controller holds the lease or this lease expired; stop and retry later"
            )

    def renew(self):
        self.assert_owned()
        self.spark.sql(
            f"UPDATE {self.table} SET lease_until=current_timestamp() + INTERVAL {self.ttl} SECONDS WHERE owner='{self.owner}' AND lease_until > current_timestamp()"
        )
        self.assert_owned()

    def release(self):
        self.spark.sql(
            f"UPDATE {self.table} SET owner=NULL, lease_until=TIMESTAMP '1970-01-01 00:00:00' WHERE owner='{self.owner}'"
        )

    def __enter__(self):
        return self.acquire()

    def __exit__(self, kind, value, traceback):
        self.release()
