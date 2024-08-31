from typing import Dict, List, Iterator, Iterable, Union

from .Dataset import DataSet
from .Dataset.Snapshot import Snapshot
from ..errors import ZfsResolveError


class Pool(object):
    def __init__(self, pool_name: str):
        self.pool_name = pool_name
        self.zfs_path = pool_name
        self.datasets: Dict[str, DataSet] = {}

    def __str__(self):
        return "Pool({})".format(self.pool_name)

    def __iter__(self) -> Iterator[DataSet]:
        for dataset_name in sorted(self.datasets.keys()):
            yield self.datasets[dataset_name]

    def __contains__(self, item: DataSet):
        return item.zfs_path in self.datasets

    def __eq__(self, other):
        if not isinstance(other, Pool):
            return False
        # our datasets and the other datasets must be the same
        if set(self.datasets.keys()) != set(other.datasets.keys()):
            return False
        # the datasets itself must also be the same
        for dataset in self:
            if dataset != other.datasets[dataset.zfs_path]:
                return False
        # finally check the pool names and other attributes
        return self.pool_name == other.pool_name

    def copy(self):
        """
        This method creates a new Pool object with the same pool name as the current instance.
        However, the datasets are not copied to the new instance.
        """
        return Pool(self.pool_name)

    def view(self):
        """
        Creates a full copy of the current Pool instance including all sub-references.
        Sub-references are also copied and not just referenced.
        """
        view_pool = Pool(self.pool_name)
        for dataset in self.datasets.values():
            view_pool.add_dataset(dataset.view())
        return view_pool

    def resolve_dataset_name(self, dataset_name: str) -> str:
        return "{}/{}".format(self.pool_name, dataset_name)

    def add_dataset(self, dataset: DataSet):
        if dataset.zfs_path in self.datasets:
            raise ValueError("Dataset '{}' already added to the pool '{}'".format(dataset.zfs_path, self.pool_name))
        if dataset.pool_name != self.pool_name:
            raise ValueError("Dataset '{}' must have the same pool name as the pool '{}'".format(dataset.zfs_path,
                                                                                                 self.pool_name))
        self.datasets[dataset.zfs_path] = dataset

    def remove_dataset(self, dataset: DataSet):
        if dataset.zfs_path not in self.datasets:
            raise ValueError("Dataset '{}' not found in the pool '{}'".format(dataset.zfs_path, self.pool_name))
        self.datasets.pop(dataset.zfs_path)

    def iter_datasets(self) -> Iterable[DataSet]:
        for dataset in self:
            yield dataset

    def iter_snapshots(self) -> Iterable[Snapshot]:
        for dataset in self:
            for snapshot in dataset:
                yield snapshot

    def resolve_zfs_path(self, zfs_path: str) -> Union[DataSet, Snapshot]:
        """
        Resolve a ZFS path to a dataset or snapshot object.

        :raises ZfsResolveError: If the ZFS path is not found in the pool.
        """
        if '@' in zfs_path:
            # resolve snapshot
            dataset_path, _ = zfs_path.split("@", 1)
            if dataset_path in self.datasets:
                return self.datasets[dataset_path].resolve_zfs_path(zfs_path)
        else:
            # resolve dataset
            dataset_path = zfs_path
            if dataset_path in self.datasets:
                return self.datasets[dataset_path]
        raise ZfsResolveError("Dataset '{}' not found in the pool '{}'".format(zfs_path, self.zfs_path))

    def get_dataset_by_name(self, dataset_name: str) -> DataSet:
        return self.datasets[self.resolve_dataset_name(dataset_name)]

    def print(self):
        print("Pool: {}".format(self.pool_name))
        for dataset in self:
            dataset.print()

    @classmethod
    def merge(cls, *others: 'Pool'):
        # build a set with all pools names
        pool_names = set(pool.pool_name for pool in others)
        # verify all pools have the same name
        if len(pool_names) > 1:
            raise ValueError("Pools must have the same name to be merged")

        pool_name = pool_names.pop()

        new_merged_pool = cls(pool_name)
        all_datasets: Dict[str, List[DataSet]] = {}

        # fill the all_datasets dict with all datasets from all pools
        for pool in others:
            for dataset in pool.datasets.values():
                if dataset.zfs_path in all_datasets:
                    all_datasets[dataset.zfs_path].append(dataset)
                else:
                    all_datasets[dataset.zfs_path] = [dataset]

        for dataset_path, mergable_datasets in all_datasets.items():
            new_merged_dataset = DataSet.merge(pool_name, *mergable_datasets)
            new_merged_pool.add_dataset(new_merged_dataset)

        return new_merged_pool

    def difference(self, *other_pools: 'Pool') -> 'Pool':
        """
        Return the difference of two or more pools as a new pool.

        (i.e. all datasets and snapshots that are in this pool but not the others.)
        """
        base_pool_datasets = set(self.datasets.keys())

        difference_datasets = base_pool_datasets.difference(*(pool.datasets.keys() for pool in other_pools))

        difference_pool = self.view()
        for dataset in list(difference_pool.datasets.values()):
            if dataset.zfs_path not in difference_datasets:
                # no difference for this dataset -> removable, but check snapshots
                difference_dataset = dataset.difference(*(pool.datasets[dataset.zfs_path]
                                                          for pool in other_pools
                                                          if dataset.zfs_path in pool.datasets))
                if difference_dataset.snapshots:
                    # replace the dataset with the difference dataset, which contains the difference snapshots
                    difference_pool.remove_dataset(dataset)
                    difference_pool.add_dataset(difference_dataset)
                else:
                    difference_pool.remove_dataset(dataset)
        return difference_pool

    def intersection(self, *other_pools: 'Pool') -> 'Pool':
        """
        Return the intersection of two  or more pools as a new pool.

        (i. e. all datasets and snapshots that are in both pools.)
        """
        # base_pool_datasets = set(self.datasets.keys())

        # intersection_datasets = base_pool_datasets.intersection(*(pool.datasets.keys() for pool in other_pools))

        intersection_base_pool = self.view()
        intersection_pool = self.copy()
        for pool in other_pools:
            intersection_pool = intersection_pool.copy()
            intersecting_datasets = set(intersection_base_pool.datasets.keys()).intersection(pool.datasets.keys())
            for intersecting_dataset in intersecting_datasets:
                intersection_pool.add_dataset(
                    intersection_base_pool.datasets[intersecting_dataset].intersection(
                        pool.datasets[intersecting_dataset]
                    ))
            intersection_base_pool = intersection_pool.view()
        return intersection_pool

    def has_incremental_snapshot_refs(self) -> bool:
        return any(dataset.has_incremental_snapshot_refs() for dataset in self.datasets.values())

    def has_snapshots(self):
        return any(dataset.has_snapshots() for dataset in self.datasets.values())

    def has_datasets(self):
        return len(self.datasets) > 0

    def build_incremental_snapshot_refs(self) -> None:
        """
        Build incremental snapshot references for all snapshots in this Pool.
        """
        for dataset in self.datasets.values():
            dataset.build_incremental_snapshot_refs()

    def drop_snapshots(self):
        for dataset in self.datasets.values():
            dataset.drop_snapshots()

    def drop_empty_datasets(self):
        """
        Drop all datasets which have no snapshots.
        """
        for dataset in list(self.datasets.values()):
            if not dataset.has_snapshots():
                self.remove_dataset(dataset)

    def filter_include_by_zfs_path_prefix(self, zfs_path_prefix: str) -> "Pool":
        """
        Filter out all elements in the pool, which do not match the given zfs path prefix.
        """
        new_pool = self.copy()

        for dataset in self.datasets.values():
            # add @ to the zfs path prefix to match the full potential dataset zfs path
            if (dataset.zfs_path + "@").startswith(zfs_path_prefix):
                dataset_view = dataset.filter_include_by_zfs_path_prefix(zfs_path_prefix)
                new_pool.add_dataset(dataset_view)

        return new_pool
