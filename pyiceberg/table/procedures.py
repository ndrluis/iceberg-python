from typing import TYPE_CHECKING, Any, Callable, Dict, List

from pydantic import Field

from pyiceberg.manifest import DataFileContent
from pyiceberg.table import Table
from pyiceberg.table.refs import SnapshotRef, SnapshotRefType
from pyiceberg.typedef import IcebergBaseModel
from pyiceberg.snapshot import ancestors_of
import time
import logging

if TYPE_CHECKING:
    pass


logger = logging.getLogger(__name__)


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
        # now in java
        self.procedure_started_at_ms = int(time.time() * 1000)

    def collect(self):
        if len(self.expired_snapshots) > 0:
            self.expired_snapshots = []

        ids_to_retain = set()
        retained_id_to_refs: Dict[int, List[str]] = {}
        refs: Dict[str, SnapshotRef] = self.table.metadata.refs

        retained_refs = self.compute_retained_refs(refs)

        for name, ref in retained_refs.items():
            snapshot_id: int = ref.snapshot_id

            if snapshot_id not in retained_id_to_refs:
                retained_id_to_refs[snapshot_id] = []

            retained_id_to_refs[snapshot_id] = name
            ids_to_retain = ids_to_retain | {snapshot_id}

        ids_to_retain = ids_to_retain | self.compute_all_branch_snapshots_to_retain(refs)

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
                ref_age_ms = self.procedure_started_at_ms - snapshot.timestamp_ms

                if ref_age_ms <= max_ref_age_ms:
                    retained_refs[name] = ref
            else:
                logger.warning(f"Snapshot {ref.snapshot_id} not found for reference {name}")

        return retained_refs

    def compute_all_branch_snapshots_to_retain(self, refs: Dict[str, SnapshotRef]):
        ids_to_retain = set()

        for _, ref in refs.items():
            if ref.snapshot_ref_type == SnapshotRefType.BRANCH:
                if ref.max_snapshot_age_ms is not None:
                    expire_snapshots_orlder_than = self.procedure_started_at_ms - ref.max_snapshot_age_ms
                else:
                    expire_snapshots_orlder_than = self.default_expire_older_than

                min_snapshots_to_keep = ref.min_snapshots_to_keep or self.default_min_num_snapshots

                ids_to_retain = ids_to_retain | self.compute_branch_snapshots_to_retain(
                    ref.snapshot_id,
                    expire_snapshots_orlder_than,
                    min_snapshots_to_keep,
                )

        return ids_to_retain

    def compute_branch_snapshots_to_retain(
        self,
        snapshot_id: int,
        expire_snapshots_older_than: int,
        min_snapshots_to_keep: int,
    ):
        ids_to_retain = set()

        for ancestor in ancestors_of(self.table.snapshot_by_id(snapshot_id)):
            if len(ids_to_retain) < min_snapshots_to_keep or ancestor.timestamp_ms >= expire_snapshots_older_than:
                ids_to_retain = ids_to_retain | {ancestor.snapshot_id}
            else:
                return ids_to_retain

        return ids_to_retain

    def execute(self):
        return self

    def delete_files(self):
        self.summary.deleted_files(DataFileContent.DATA.name, 0)
