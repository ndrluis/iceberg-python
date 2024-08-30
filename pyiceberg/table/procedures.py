from typing import TYPE_CHECKING, Any, Callable, Dict

from pydantic import Field

from pyiceberg.manifest import DataFileContent
from pyiceberg.table import Table
from pyiceberg.table.refs import SnapshotRef
from pyiceberg.typedef import IcebergBaseModel

if TYPE_CHECKING:
    pass


class DeleteSummary(IcebergBaseModel):
    MANIFEST = "MANIFEST"
    MANIFEST_LIST = "MANIFEST_LIST"
    STATISTICS_FILES = "STATISTICS_FILES"
    OTHERS = "OTHERS"

    data_files_count: int = Field(default=0)
    position_delete_files_count: int = Field(default=0)
    equality_delete_files_count: int = Field(default=0)
    manifests_count: int = Field(default=0)
    manifest_lists_count: int = Field(default=0)
    statistics_files_count: int = Field(default=0)
    other_files_count: int = Field(default=0)

    def deleted_files(self, file_type: str, count: int):
        if file_type == DataFileContent.DATA.name:
            self.data_files_count += count
        elif file_type == DataFileContent.POSITION_DELETE.name:
            self.position_delete_files_count += count
        elif file_type == DataFileContent.EQUALITY_DELETE.name:
            self.equality_delete_files_count += count
        elif file_type == self.MANIFEST:
            self.manifests_count += count
        elif file_type == self.MANIFEST_LIST:
            self.manifest_lists_count += count
        elif file_type == self.STATISTICS_FILES:
            self.statistics_files_count += count
        elif file_type == self.OTHERS:
            self.other_files_count += count
        else:
            raise ValueError(f"Unknown file type: {file_type}")


class ExpireSnashots:
    # Expire snapshots older than 3 days (timestampMillis a long timestamp)
    DEFAULT_OPTS = {
        "expire_older_than": 3 * 24 * 60 * 60 * 1000,
        "retain_last": True,
    }

    expired_snapshots = []

    summary = DeleteSummary()

    def __init__(self, table: Table, opts: Dict[str, Any], delete_with: Callable[[str], None] = None):
        self.table = table
        self.opts = {**self.DEFAULT_OPTS, **opts}
        self.delete_with = delete_with
        self.default_max_ref_age_ms = 0
        self.now = 0

    def collect(self):
        if len(self.expired_snapshots) > 0:
            self.expired_snapshots = []

        ids_to_retain = set()
        retained_refs = self.table.metadata.refs

        compute_retained_refs = self.compute_retained_refs(retained_refs)

        return self

    def compute_retained_refs(self, refs: Dict[str, SnapshotRef]):
        retained_refs: Dict[str, SnapshotRef] = {}

        for name, ref in refs.items():
            if name == SnapshotRef.MAIN_BRANCH:
                retained_refs[name] = ref
                continue

            snapshot = self.table.snapshot_by_id(ref.snapshot_id)

            max_ref_age_ms = ref.max_ref_age_ms or self.default_max_ref_age_ms

            if snapshot is not None:
                ref_age_ms = self.now - snapshot.timestamp_ms

                if ref_age_ms <= max_ref_age_ms:
                    retained_refs[name] = ref
            else:
                logger.warning(f"Snapshot {ref.snapshot_id} not found for reference {name}")

        return retained_refs

    def execute(self):
        return self

    def delete_files(self):
        self.summary.deleted_files(DataFileContent.DATA.name, 0)
