from pyiceberg.manifest import DataFile

DataFile(file_path="/path/to/data-a.parquet", file_size_in_bytes=10, partitions={"id": "123"})
