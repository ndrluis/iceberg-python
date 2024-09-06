from pyiceberg.manifest import DataFile
from pyiceberg.schema import Schema, NestedField, IntegerType, StringType
from pyiceberg.partitioning import PartitionSpec, PartitionField, BucketTransform



FILE_A = DataFile(
    file_path="/path/to/data-a.parquet",
    file_size_in_bytes=10,
    partitions={"data_bucket": "0"},
    record_count=1,
)

FILE_B = DataFile(
    file_path="/path/to/data-b.parquet",
    file_size_in_bytes=10,
    partitions={"data_bucket": "1"},
    record_count=1,
    split_offsets=[1],
)

SCHEMA = Schema(
    NestedField("id", IntegerType(), nullable=False),
    NestedField("data", StringType(), nullable=False),
)

SPEC = PartitionSpec(PartitionField(2, 1000, BucketTransform(16), "data_bucket"))


def test_expire_older_than():
    table =
