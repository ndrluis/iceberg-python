# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


import pytest
from pyspark.sql import SparkSession

from pyiceberg.catalog import Catalog
from pyiceberg.schema import Schema
from pyiceberg.table.procedures import ExpireSnashots
from pyiceberg.types import (
    LongType,
    NestedField,
    StringType,
)

TABLE_SCHEMA = Schema(
    NestedField(field_id=1, name="id", field_type=LongType(), required=False),
    NestedField(field_id=2, name="name", field_type=StringType(), required=False),
)


def create_table(spark: SparkSession, identifier: str, sql: str):
    spark.sql(f"DROP TABLE IF EXISTS {identifier}")
    spark.sql(sql.format(identifier))


@pytest.mark.integration
def test_expire_snapshots_in_empty_table(
    spark: SparkSession,
    session_catalog: Catalog,
    format_version: int,
) -> None:
    table = create_table()
    identifier = "default.empty_table_expire_snapshots"

    create_table(spark, "CREATE TABLE {} (id int, data string)", identifier)

    table = session_catalog.load_table(identifier)

    ExpireSnashots(table).collect().commit()


@pytest.mark.integration
def test_expire_snapshots_using_positional_args(
    spark: SparkSession,
    session_catalog: Catalog,
    format_version: int,
):
    identifier = "default.test_table_expire_snapshots"

    create_table(spark, "CREATE TABLE {} (id int, data string)", identifier)

    spark.sql(f"INSERT INTO TABLE {identifier} VALUES (1, 'a')")

    table = session_catalog.load_table(identifier)

    spark.sql(f"INSERT INTO TABLE {identifier} VALUES (2, 'b')")

    table.refresh()

    second_snapshot = table.current_snapshot()
    second_snapshot_timestamp = second_snapshot.timestamp_ms

    assert len(table.snapshots()) == 2

    # expire without retainLast param
    action = (
        ExpireSnashots(
            table,
            {"expire_older_than": second_snapshot_timestamp},
        )
        .collect()
        .execute()
    )

    table.refresh()

    assert len(table.snapshots()) == 1

    assert action.summary == {
        "data_files_count": 0,
        "position_delete_files_count": 0,
        "equality_delete_files_count": 0,
        "manifests_count": 0,
        "statistics_files_count": 1,
        "other_files_count": 0,
    }

    spark.sql("INSERT OVERWRITE {identifier} VALUES (3, 'c')")
    spark.sql("INSERT INTO TABLE {identifier} VALUES (4, 'd')")

    table.refresh()

    assert len(table.snapshots()) == 3

    from datetime import datetime

    current_timestamp = datetime.utcnow()

    # expire with retainLast param
    action = (
        ExpireSnashots(
            table,
            {"expire_older_than": current_timestamp},
        )
        .collect()
        .execute()
    )

    table.refresh()

    assert len(table.snapshots()) == 1

    assert action.summary == {
        "data_files_count": 2,
        "position_delete_files_count": 0,
        "equality_delete_files_count": 0,
        "manifests_count": 2,
        "statistics_files_count": 1,
        "other_files_count": 0,
    }
