from typing import Dict, Iterator, List, Iterable

from ZfsBackupTool.Constants import SNAPSHOT_PREFIX_POSTFIX_SEPARATOR, INITIAL_SNAPSHOT_POSTFIX
from .Snapshot import Snapshot


class DataSet(object):
    def __init__(self, pool_name: str, dataset_name):
        self.pool_name = pool_name
        self.dataset_name = dataset_name
        self.zfs_path = "{}/{}".format(pool_name, dataset_name)
        self.snapshots: Dict[str, Snapshot] = {}

    def __str__(self):
        return "DataSet({})".format(self.zfs_path)

    def __iter__(self) -> Iterator[Snapshot]:
        for snapshot in self.sort_snapshots(self.snapshots.values()):
            yield snapshot

    def __contains__(self, item: Snapshot):
        return item.zfs_path in self.snapshots

    def __eq__(self, other):
        if not isinstance(other, DataSet):
            return False
        # our snapshots and the other snapshots must be the same
        if set(self.snapshots.keys()) != set(other.snapshots.keys()):
            return False
        # the snapshots itself must also be the same
        for snapshot in self:
            if snapshot != other.snapshots[snapshot.zfs_path]:
                return False
        # finally check the dataset paths and other attributes
        return self.zfs_path == other.zfs_path

    def copy(self):
        """
        This method creates a new DataSet object with the same pool name and dataset name as the current instance.
        However, the snapshots are not copied to the new instance.
        """
        return DataSet(self.pool_name, self.dataset_name)

    def view(self):
        """
        Creates a full copy of the current DataSet instance including all sub-references.
        Sub-references are also copied and not just referenced.
        """
        view_dataset = DataSet(self.pool_name, self.dataset_name)
        for snapshot in self.snapshots.values():
            view_dataset.add_snapshot(snapshot.view())
        return view_dataset

    def resolve_snapshot_name(self, snapshot_name: str) -> str:
        return "{}@{}".format(self.zfs_path, snapshot_name)

    def add_snapshot(self, snapshot: Snapshot):
        if snapshot.zfs_path in self.snapshots:
            raise ValueError(
                "Dataset '{}' already added to the pool '{}'".format(snapshot.snapshot_name, self.zfs_path))
        self.snapshots[snapshot.zfs_path] = snapshot

    def remove_snapshot(self, snapshot: Snapshot) -> Snapshot:
        if snapshot.zfs_path not in self.snapshots:
            raise ValueError(
                "Dataset '{}' not found in the pool '{}'".format(snapshot.snapshot_name, self.zfs_path))
        return self.snapshots.pop(snapshot.zfs_path)

    def iter_snapshots(self) -> Iterator[Snapshot]:
        for snapshot in self:
            yield snapshot

    def resolve_zfs_path(self, zfs_path: str) -> Snapshot:
        if zfs_path.startswith(self.zfs_path):
            snapshot_name = zfs_path.split("@")[1]
            return self.snapshots[snapshot_name]
        raise ValueError("Snapshot '{}' not found in the dataset '{}'".format(zfs_path, self.zfs_path))

    def get_snapshot_by_name(self, snapshot_name: str) -> Snapshot:
        if snapshot_name.startswith(self.zfs_path):
            return self.snapshots[snapshot_name]
        return self.snapshots[self.resolve_snapshot_name(snapshot_name)]

    def print(self):
        print("  Dataset: {} ({})".format(self.dataset_name, self.zfs_path))
        for snapshot in self:
            snapshot.print()

    @classmethod
    def merge(cls, pool_name: str, *others: 'DataSet'):
        # build a set with all datasets names
        dataset_names = set(dataset.dataset_name for dataset in others)
        # verify all datasets have the same name
        if len(dataset_names) > 1:
            raise ValueError("Datasets must have the same name to be merged")

        dataset_name = dataset_names.pop()

        new_merged_dataset = cls(pool_name, dataset_name)
        all_snapshots: Dict[str, List[Snapshot]] = {}

        # fill the all_snapshots dict with all snapshots from all datasets
        for dataset in others:
            for snapshot in dataset.snapshots.values():
                if snapshot.zfs_path in all_snapshots:
                    all_snapshots[snapshot.zfs_path].append(snapshot)
                else:
                    all_snapshots[snapshot.zfs_path] = [snapshot]

        for snapshot_path, mergable_snapshots in all_snapshots.items():
            new_merged_snapshot = Snapshot.merge(pool_name, dataset_name, *mergable_snapshots)
            new_merged_dataset.add_snapshot(new_merged_snapshot)

        return new_merged_dataset

    @classmethod
    def sort_snapshots(cls, snapshots: Iterable[Snapshot]) -> List[Snapshot]:
        snapshots = list(snapshots)
        initial_snapshots = sorted([s for s in snapshots
                                    if s.snapshot_name.endswith(SNAPSHOT_PREFIX_POSTFIX_SEPARATOR
                                                                + INITIAL_SNAPSHOT_POSTFIX)],
                                   key=lambda s: s.zfs_path)
        non_initial_snapshots = sorted([s for s in snapshots
                                        if not s.snapshot_name.endswith(SNAPSHOT_PREFIX_POSTFIX_SEPARATOR
                                                                        + INITIAL_SNAPSHOT_POSTFIX)],
                                       key=lambda s: s.zfs_path)

        return initial_snapshots + non_initial_snapshots

    def difference(self, *other_datasets: 'DataSet') -> 'DataSet':
        """
        Return the difference of two or more datasets as a new dataset.

        (i.e. all snapshots that are in this dataset but not the others.)
        """
        base_dataset_snapshots = set(self.snapshots.keys())

        difference_snapshots = base_dataset_snapshots.difference(*(dataset.snapshots.keys()
                                                                   for dataset in other_datasets))

        difference_dataset = self.view()
        for snapshot in difference_dataset:
            if snapshot.zfs_path not in difference_snapshots:
                difference_dataset.remove_snapshot(snapshot)
        return difference_dataset

    def intersection(self, *other_datasets: 'DataSet') -> 'DataSet':
        """
        Return the intersection of two or more datasets as a new dataset.

        (i. e. all snapshots that are in both datasets.)
        """
        base_dataset_snapshots = set(self.snapshots.keys())

        intersection_snapshots = base_dataset_snapshots.intersection(*(dataset.snapshots.keys()
                                                                       for dataset in other_datasets))

        difference_dataset = self.view()
        for snapshot in difference_dataset:
            if snapshot.zfs_path not in intersection_snapshots:
                difference_dataset.remove_snapshot(snapshot)
        return difference_dataset

    def is_incremental(self) -> bool:
        return any(snapshot.has_incremental_base() for snapshot in self.snapshots.values())

    def has_snapshots(self):
        return len(self.snapshots) > 0

    def get_incremental_children(self, parent: Snapshot) -> 'DataSet':
        """
        Get a dataset containing only the snapshots that are incremental children of the given parent snapshot.

        This method creates a view of the current dataset and removes all snapshots that are not incremental children
        of the specified parent snapshot. The resulting dataset will contain the parent snapshot and all snapshots
        that follow it in the incremental chain.

        Args:
            parent (Snapshot): The parent snapshot from which to start the incremental chain.

        Returns:
            DataSet: A new dataset containing only the snapshots that are incremental children of the parent snapshot.
        """
        thinned_out_view = self.view()
        for snapshot in list(thinned_out_view.iter_snapshots()):
            if snapshot == parent:
                break
            thinned_out_view.remove_snapshot(snapshot)
        return thinned_out_view

    def build_incremental_snapshot_refs(self) -> None:
        """
        Build incremental snapshot references for all snapshots.
        """
        sorted_snapshots = self.sort_snapshots(self.snapshots.values())
        snapshot_prefixes = set()
        for snapshot in sorted_snapshots:
            snapshot_prefix = snapshot.snapshot_name.rsplit(SNAPSHOT_PREFIX_POSTFIX_SEPARATOR, 1)[0]
            snapshot_prefixes.add(snapshot_prefix)

        for snapshot_prefix in snapshot_prefixes:
            incremental_base = None
            for index, snapshot in enumerate(sorted_snapshots):
                if snapshot.snapshot_name.startswith(snapshot_prefix + SNAPSHOT_PREFIX_POSTFIX_SEPARATOR):
                    if incremental_base:
                        # verify incremental base +1 is equal to our current snapshot index
                        if incremental_base.snapshot_name.endswith(
                                SNAPSHOT_PREFIX_POSTFIX_SEPARATOR + INITIAL_SNAPSHOT_POSTFIX):
                            incremental_base_index = 0
                        else:
                            incremental_base_index = int(
                                incremental_base.snapshot_name.split(SNAPSHOT_PREFIX_POSTFIX_SEPARATOR)[-1])
                        snapshot_index = int(
                            snapshot.snapshot_name.split(SNAPSHOT_PREFIX_POSTFIX_SEPARATOR)[-1])
                        if incremental_base_index + 1 != snapshot_index:
                            continue

                        snapshot.set_incremental_base(incremental_base)
                    incremental_base = snapshot