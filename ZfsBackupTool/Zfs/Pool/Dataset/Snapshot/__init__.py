from typing import Optional

from ZfsBackupTool.Constants import INITIAL_SNAPSHOT_POSTFIX, SNAPSHOT_PREFIX_POSTFIX_SEPARATOR


class Snapshot(object):
    def __init__(self, pool_name: str, dataset_name: str, snapshot_name: str):
        self.pool_name = pool_name
        self.dataset_name = dataset_name
        self.snapshot_name = snapshot_name
        self.dataset_path = "{}/{}".format(pool_name, dataset_name)
        self.zfs_path = "{}/{}@{}".format(pool_name, dataset_name, snapshot_name)
        self._incremental_base: Optional['Snapshot'] = None

    def __str__(self):
        return "Snapshot({})".format(self.zfs_path)

    def __eq__(self, other: 'Snapshot'):
        # check the snapshot paths and other attributes
        return self.zfs_path == other.zfs_path

    def copy(self):
        return Snapshot(self.pool_name, self.dataset_name, self.snapshot_name)

    def view(self):
        return Snapshot(self.pool_name, self.dataset_name, self.snapshot_name)

    def print(self):
        print("    Snapshot: {} ({})".format(self.snapshot_name, self.zfs_path))

    @classmethod
    def merge(cls, pool_name: str, dataset_name: str, *others: 'Snapshot'):
        # build a set with all snapshot names
        snapshot_names = set(snapshot.snapshot_name for snapshot in others)
        # verify all datasets have the same name
        if len(snapshot_names) > 1:
            raise ValueError("Snapshots must have the same name to be merged")

        snapshot_name = snapshot_names.pop()

        new_merged_snapshot = cls(pool_name, dataset_name, snapshot_name)
        return new_merged_snapshot

    def has_increment_base(self) -> bool:
        return self._incremental_base is not None

    def set_incremental_base(self, base: 'Snapshot'):
        self._incremental_base = base

    def get_incremental_base(self) -> 'Snapshot':
        if not self._incremental_base:
            raise ValueError("Snapshot has no incremental base")
        return self._incremental_base


class IncrementalSnapshot(Snapshot):
    def __init__(self, pool_name: str, dataset_name: str, snapshot_name: str, previous_snapshot: Snapshot):
        super().__init__(pool_name, dataset_name, snapshot_name)
        self.previous_snapshot = previous_snapshot

    def __str__(self):
        return "IncrementalSnapshot({})".format(self.zfs_path)

    def __eq__(self, other: 'IncrementalSnapshot'):
        # check the snapshot paths and other attributes
        return self.zfs_path == other.zfs_path

    def copy(self):
        return IncrementalSnapshot(self.pool_name, self.dataset_name, self.snapshot_name, self.previous_snapshot_name)

    def view(self):
        return IncrementalSnapshot(self.pool_name, self.dataset_name, self.snapshot_name, self.previous_snapshot_name)

    def print(self):
        print("    Incremental Snapshot: {} ({})".format(self.snapshot_name, self.zfs_path))

    @classmethod
    def merge(cls, pool_name: str, dataset_name: str, *others: 'IncrementalSnapshot'):
        # build a set with all snapshot names
        snapshot_names = set(snapshot.snapshot_name for snapshot in others)
        # verify all snapshots have the same name
        if len(snapshot_names) > 1:
            raise ValueError("Snapshots must have the same name to be merged")

        previous_snapshot = others[0].previous_snapshot
        previous_snapshot_names = set(snapshot.previous_snapshot.snapshot_name for snapshot in others)
        # verify all previous snapshots have the same name
        if len(previous_snapshot_names) > 1:
            raise ValueError("Snapshots must have the same previous snapshot name to be merged")

        snapshot_name = snapshot_names.pop()

        new_merged_snapshot = cls(pool_name, dataset_name, snapshot_name, others[0].previous_snapshot)
        return new_merged_snapshot
