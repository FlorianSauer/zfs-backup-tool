from datetime import datetime
from typing import Optional


class Snapshot(object):
    def __init__(self, pool_name: str, dataset_name: str, snapshot_name: str):
        self.pool_name = pool_name
        self.dataset_name = dataset_name
        self.snapshot_name = snapshot_name
        self.dataset_zfs_path = "{}/{}".format(pool_name, dataset_name)
        self.zfs_path = "{}/{}@{}".format(pool_name, dataset_name, snapshot_name)
        self._incremental_base: Optional['Snapshot'] = None
        self._creation_time: Optional[datetime] = None

    def __str__(self):
        if self._incremental_base:
            return "Snapshot({}) -incr-> {}".format(self.zfs_path, self._incremental_base.snapshot_name)
        return "Snapshot({})".format(self.zfs_path)

    def __eq__(self, other):
        if not isinstance(other, Snapshot):
            return False
        # check the snapshot paths and other attributes
        return self.zfs_path == other.zfs_path

    def __hash__(self):
        return hash(self.zfs_path)

    def copy(self):
        """
        This method creates a new Snapshot object with the same pool name, dataset name, and snapshot name
        as the current instance. However, the incremental base is not set in the new instance.
        """
        snapshot_copy = Snapshot(self.pool_name, self.dataset_name, self.snapshot_name)
        if self._creation_time:
            snapshot_copy._creation_time = self._creation_time
        return snapshot_copy

    def prefixed_view(self, prefix: str, deshift: bool = False) -> 'Snapshot':
        """
        Creates a full copy of the current DataSet instance including all sub-references.
        Sub-references are also copied and not just referenced.
        All zfs paths are prefixed with the given prefix. This can be used to 'shift' the snapshot to a different
        location in the zfs hierarchy.
        """
        if deshift:
            prefixed_zfs_path = self.dataset_zfs_path.replace(prefix, '', 1)
        else:
            prefixed_zfs_path = prefix + self.dataset_zfs_path
        pool_name = prefixed_zfs_path.split("/", 1)[0]
        dataset_name = prefixed_zfs_path.split("/", 1)[1]

        view_snapshot = Snapshot(pool_name, dataset_name, self.snapshot_name)
        if self._incremental_base:
            view_snapshot._incremental_base = self._incremental_base.prefixed_view(prefix, deshift)
        if self._creation_time:
            view_snapshot._creation_time = self._creation_time
        return view_snapshot

    def view(self):
        """
        Creates a full copy of the current Snapshot instance including all sub-references.
        Sub-references are also copied and not just referenced.
        """
        return self.prefixed_view('')

    def print(self, with_incremental_base: bool = True):
        if self.has_incremental_base() and with_incremental_base:
            print("    Snapshot: {} ({}) -incr-> {}".format(self.snapshot_name, self.zfs_path,
                                                       self._incremental_base.snapshot_name))
        else:
            print("    Snapshot: {} ({})".format(self.snapshot_name, self.zfs_path))

    @classmethod
    def merge(cls, pool_name: str, dataset_name: str, *others: 'Snapshot'):
        # verify all snapshots have the same pool and dataset name
        if any(snapshot.pool_name != pool_name for snapshot in others):
            raise ValueError("Snapshots must have the same pool name to be merged")
        if any(snapshot.dataset_name != dataset_name for snapshot in others):
            raise ValueError("Snapshots must have the same dataset name to be merged")
        # build a set with all snapshot names
        snapshot_names = set(snapshot.snapshot_name for snapshot in others)
        # verify all datasets have the same name
        if len(snapshot_names) > 1:
            raise ValueError("Snapshots must have the same name to be merged")

        snapshot_name = snapshot_names.pop()

        new_merged_snapshot = cls(pool_name, dataset_name, snapshot_name)

        incremental_bases = [snapshot.get_incremental_base() for snapshot in others if snapshot.has_incremental_base()]
        if incremental_bases:
            new_merged_snapshot.set_incremental_base(cls.merge(pool_name, dataset_name, *incremental_bases))

        creation_times = [snapshot.get_creation_time() for snapshot in others if snapshot.has_creation_time()]
        if creation_times:
            new_merged_snapshot.set_creation_time(min(creation_times))
        return new_merged_snapshot

    def has_incremental_base(self) -> bool:
        return self._incremental_base is not None

    def set_incremental_base(self, base: Optional['Snapshot']):
        self._incremental_base = base

    def get_incremental_base(self) -> 'Snapshot':
        if not self._incremental_base:
            raise ValueError("Snapshot has no incremental base")
        return self._incremental_base

    def has_creation_time(self) -> bool:
        return self._creation_time is not None

    def set_creation_time(self, creation_time: datetime):
        self._creation_time = creation_time

    def get_creation_time(self) -> datetime:
        if not self._creation_time:
            raise ValueError("Snapshot has no creation time")
        return self._creation_time
