import logging
import os
from typing import List, Dict, Iterable, Union, Iterator

from ZfsBackupTool.ShellCommand import ShellCommand
from .Pool import Pool
from .Pool.Dataset import DataSet
from .Pool.Dataset.Snapshot import Snapshot
from .errors import ZfsResolveError, ZfsDeshiftError, ZfsAddError

__all__ = [
    'scan_zfs_pools', 'scan_filebased_zfs_pools',
    'PoolList', 'Pool', 'DataSet', 'Snapshot']

from ..Constants import (BACKUP_FILE_POSTFIX, EXPECTED_CHECKSUM_FILE_POSTFIX, CALCULATED_CHECKSUM_FILE_POSTFIX)

logger = logging.getLogger(__name__)


class PoolList(object):
    """
    Class to store multiple pools with DIFFERENT pool names in one object.
    """

    def __init__(self, *pools: Union[Pool, Iterable[Pool]]):
        self.pools: Dict[str, Pool] = {}
        for pool in pools:
            if isinstance(pool, Pool):
                if pool.pool_name in self.pools:
                    raise ValueError("Pool '{}' already added to the pool list".format(pool.pool_name))
                self.pools[pool.pool_name] = pool

            elif isinstance(pool, Iterable):
                for sub_pool in pool:
                    if sub_pool.pool_name in self.pools:
                        raise ValueError("Pool '{}' already added to the pool list".format(sub_pool.pool_name))
                    self.pools[sub_pool.pool_name] = sub_pool
            else:
                raise ValueError("Invalid pool type {}".format(type(pool)))

    def __iter__(self) -> Iterator[Pool]:
        for pool_name in sorted(self.pools.keys()):
            yield self.pools[pool_name]

    def __contains__(self, item: Pool):
        return item in self.pools.values()

    def __eq__(self, other):
        if not isinstance(other, PoolList):
            return False
        # our pools and the other pools must be the same
        if set(self.pools.keys()) != set(other.pools.keys()):
            return False
        # finally check the pools itself and other attributes
        for pool_name in self.pools.keys():
            if self.pools[pool_name] != other.pools[pool_name]:
                return False
        return True

    def copy(self):
        """
        This method creates a new PoolList object with the same pool names as the current instance.
        However, the pools are not copied to the new instance.
        """
        return PoolList(*[pool.copy() for pool in self.pools.values()])

    def prefixed_view(self, prefix: str, deshift: bool = False):
        """
        Creates a full copy of the current PoolList instance including all sub-references.
        Sub-references are also copied and not just referenced.
        All zfs paths are prefixed with the given prefix. This can be used to 'shift' the pool list to a different
        location in the zfs hierarchy.
        """
        pool_list = [pool.prefixed_view(prefix, deshift) for pool in self.pools.values()]
        if deshift:
            # check if the de-shifting created multiple pools with the same name
            pool_names = {pool.pool_name: pool for pool in pool_list}
            if len(pool_names) != len(pool_list):
                raise ZfsDeshiftError("De-shifting the pool list created multiple pools with the same name")
        return PoolList(*pool_list)

    def view(self):
        """
        Creates a full copy of the current PoolList instance including all sub-references.
        Sub-references are also copied and not just referenced.
        """
        return self.prefixed_view('')

    def add_pool(self, pool: Pool):
        if pool.pool_name in self.pools:
            raise ZfsAddError("Pool '{}' already added to the pool list".format(pool.pool_name))
        self.pools[pool.pool_name] = pool

    def remove_pool(self, pool: Pool):
        if pool.pool_name not in self.pools:
            raise ZfsResolveError("Pool '{}' not found in the pool list".format(pool.pool_name))
        self.pools.pop(pool.pool_name)

    def add_dataset(self, dataset: DataSet):
        poolname = dataset.pool_name
        if poolname not in self.pools:
            pool = Pool(poolname)
            self.pools[poolname] = pool
        else:
            pool = self.pools[poolname]
        pool.add_dataset(dataset)

    def remove_dataset(self, dataset: DataSet):
        if dataset.pool_name not in self.pools:
            raise ZfsResolveError("Pool '{}' not found in the pool list".format(dataset.pool_name))
        pool = self.pools[dataset.pool_name]
        pool.remove_dataset(dataset)

    def iter_pools(self) -> Iterable[Pool]:
        for pool in self:
            yield pool

    def iter_datasets(self) -> Iterable[DataSet]:
        for pool in self:
            for dataset in pool:
                yield dataset

    def iter_snapshots(self) -> Iterable[Snapshot]:
        for pool in self:
            for dataset in pool:
                for snapshot in dataset:
                    yield snapshot

    def resolve_zfs_path(self, zfs_path: str) -> Union[Pool, DataSet, Snapshot]:
        """
        Resolve a ZFS path to a Pool, DataSet or Snapshot object.

        :raises ZfsResolveError: If the pool name is not found in the pool list.
        """
        pool_name, _ = zfs_path.split("/", 1)
        if pool_name in self.pools:
            return self.pools[pool_name].resolve_zfs_path(zfs_path)
        raise ZfsResolveError("Pool '{}' not found in the pool list".format(pool_name))

    def print(self, with_incremental_base: bool = True):
        for pool in self:
            pool.print(with_incremental_base)

    @classmethod
    def merge(cls, *others: 'PoolList') -> 'PoolList':
        equal_pools: Dict[str, List[Pool]] = {}
        for pool_list in others:
            if isinstance(pool_list, PoolList):
                for pool in pool_list:
                    if pool.pool_name in equal_pools:
                        equal_pools[pool.pool_name].append(pool)
                    else:
                        equal_pools[pool.pool_name] = [pool]
            elif isinstance(pool_list, Iterable):
                for sub_pool in others:
                    for pool in sub_pool:
                        if pool.pool_name in equal_pools:
                            equal_pools[pool.pool_name].append(pool)
                        else:
                            equal_pools[pool.pool_name] = [pool]
            else:
                raise ValueError("Invalid pool type {}".format(type(pool_list)))

        merged_pools = []
        for pool_name, pool_list in equal_pools.items():
            merged_pool = Pool.merge(*pool_list)
            merged_pools.append(merged_pool)

        return PoolList(merged_pools)

    def difference(self, *other_pool_lists: 'PoolList') -> 'PoolList':
        """
        Return the difference of two or more pool lists as a new pool list.

        (i.e. all pools, datasets and snapshots that are in this pool list but not the others.)
        """
        other_pools = {}
        for pool_list in other_pool_lists:
            for pool in pool_list:
                if pool.pool_name not in other_pools:
                    other_pools[pool.pool_name] = [pool]
                else:
                    other_pools[pool.pool_name].append(pool)

        diff_poollist = PoolList()
        for our_pool in self.pools.values():
            if our_pool.pool_name in other_pools:
                diff_pool = our_pool.difference(*other_pools[our_pool.pool_name])
                diff_poollist.add_pool(diff_pool)
            else:
                diff_poollist.add_pool(our_pool.view())
        full_diffs = set(self.pools.keys()).difference(set(other_pools.keys()))
        for full_diff in full_diffs:
            assert full_diff not in diff_poollist.pools
            diff_poollist.add_pool(self.pools[full_diff].view())
        return diff_poollist

    def intersection(self, *other_pool_lists: 'PoolList') -> 'PoolList':
        equal_pools: Dict[str, List[Pool]] = {}
        for pool in self.pools.values():
            equal_pools[pool.pool_name] = [pool]
        for item in other_pool_lists:
            if isinstance(item, PoolList):
                for pool in item:
                    if not pool.pool_name in equal_pools:
                        equal_pools[pool.pool_name] = []
                    equal_pools[pool.pool_name].append(pool)
            elif isinstance(item, Iterable):
                sub_pool: PoolList
                for sub_pool in item:
                    for pool in sub_pool:
                        if not pool.pool_name in equal_pools:
                            equal_pools[pool.pool_name] = []
                        equal_pools[pool.pool_name].append(pool)
            else:
                raise ValueError("Invalid pool type {}".format(type(item)))

        intersection_pools = []
        for pool_name, pool_list in equal_pools.items():
            if len(pool_list) == 1:
                # skip pools with only one pool, second comparison pool would be an empty pool
                # equal example: set((1,2,3)).intersection(set()) == set()
                continue
            base_pool = pool_list.pop()
            intersection_pool = base_pool.intersection(*pool_list)
            intersection_pools.append(intersection_pool)

        return PoolList(intersection_pools)

    def has_incremental_snapshot_refs(self) -> bool:
        return any(pool.has_incremental_snapshot_refs() for pool in self.pools.values())

    def has_snapshots(self):
        return any(pool.has_snapshots() for pool in self.pools.values())

    def has_datasets(self):
        return any(pool.has_datasets() for pool in self.pools.values())

    def get_dataset_by_path(self, zfs_path: str):
        pool_name, dataset_name = zfs_path.split("/", 1)
        dataset_name = "{}/{}".format(pool_name, dataset_name.split("@", 1)[0])
        if pool_name not in self.pools:
            raise ZfsResolveError("Pool '{}' not found in the pool list".format(pool_name))
        if dataset_name not in self.pools[pool_name].datasets:
            raise ZfsResolveError("Dataset '{}' not found in the pool '{}'".format(dataset_name, pool_name))
        return self.pools[pool_name].datasets[dataset_name]

    def build_incremental_snapshot_refs(self) -> None:
        """
        Build incremental snapshot references for all snapshots in this PoolList.
        """
        for pool in self.pools.values():
            pool.build_incremental_snapshot_refs()

    def drop_snapshots(self) -> None:
        """
        Drop all snapshots from all datasets in this PoolList.
        """
        for pool in self.pools.values():
            pool.drop_snapshots()

    def drop_empty_datasets(self) -> None:
        """
        Drop all datasets that have no snapshots from all pools in this PoolList.
        """
        for pool in self.pools.values():
            pool.drop_empty_datasets()

    def filter_include_by_zfs_path_prefix(self, zfs_path_prefix: str) -> "PoolList":
        """
        Filter out all elements in the pool, which do not match the given zfs path prefix.
        """
        new_poollist = PoolList()

        for pool in self.pools.values():
            pool_view = pool.filter_include_by_zfs_path_prefix(zfs_path_prefix)
            if pool_view.has_datasets():
                new_poollist.add_pool(pool_view)

        return new_poollist


def scan_zfs_pools(shell_command: ShellCommand, include_dataset_sizes=False) -> PoolList:
    """
    Scan the local system for ZFS datasets.
    """
    # first scan for pools
    pool_names = shell_command.list_pools()
    logger.debug("Found pools: {}".format(pool_names))
    pools = [Pool(pool_name) for pool_name in pool_names]

    # iter pools, add datasets, add snapshots
    for pool in pools:
        pool_dataset_names = shell_command.list_datasets(pool.pool_name)
        logger.debug("Found datasets for pool {}: {}".format(pool.pool_name, pool_dataset_names))
        for dataset_name in pool_dataset_names:
            dataset = DataSet(pool.pool_name, dataset_name)
            if include_dataset_sizes:
                dataset.dataset_size = shell_command.get_dataset_size(dataset.zfs_path, recursive=False)
            pool.add_dataset(dataset)

            dataset_snapshot_names = shell_command.list_snapshots(dataset.zfs_path)
            logger.debug("Found snapshots for dataset {}: {}".format(dataset.zfs_path, dataset_snapshot_names))

            for snapshot_name in dataset_snapshot_names:
                snapshot = Snapshot(pool.pool_name, dataset.dataset_name, snapshot_name)
                dataset.add_snapshot(snapshot)

    return PoolList(*pools)


def scan_filebased_zfs_pools(shell_command: ShellCommand, target_pool_storage_path: str) -> PoolList:
    discovered_pools: PoolList = PoolList()

    if not shell_command.target_dir_exists(target_pool_storage_path):
        return PoolList(discovered_pools)

    _, pool_names = shell_command.target_list_directory(target_pool_storage_path)
    logger.debug("Found pools: {}".format(pool_names))
    for pool_name in pool_names:
        pool = Pool(pool_name)
        discovered_pools.add_pool(pool)

    for pool in discovered_pools:
        pool_target_path = os.path.join(target_pool_storage_path, pool.pool_name)
        # prime dataset_dirs with the dataset names
        files, dataset_names = shell_command.target_list_directory(pool_target_path)
        logger.debug("Found top level datasets for pool {}: {}".format(pool.pool_name, dataset_names))

        # analyze datasets while we have some
        while dataset_names:
            dataset_name = dataset_names.pop()
            dataset_zfs_path = pool.resolve_dataset_name(dataset_name)
            dataset_target_path = os.path.join(target_pool_storage_path, dataset_zfs_path)

            dataset_dir_file_names, dataset_dir_subdir_names = shell_command.target_list_directory(
                dataset_target_path)
            logger.debug("Found files for dataset {}: {}".format(dataset_zfs_path, dataset_dir_file_names))
            logger.debug("Found folders for dataset {}: {}".format(dataset_zfs_path, dataset_dir_subdir_names))

            # filter out checksum files
            snapshot_files = [snapshot_name
                              for snapshot_name in dataset_dir_file_names
                              if snapshot_name.endswith(BACKUP_FILE_POSTFIX)]

            # snapshot names are the ones that are not directories
            if snapshot_files:
                if dataset_zfs_path in pool.datasets:
                    dataset = pool.datasets[dataset_zfs_path]
                else:
                    dataset = DataSet(pool.pool_name, dataset_name)
                    logger.debug("Adding dataset: {}".format(dataset_name))
                    pool.add_dataset(dataset)
                for snapshot_file in snapshot_files:
                    snapshot_name = snapshot_file.replace(BACKUP_FILE_POSTFIX, "")
                    snapshot_checksum_file = snapshot_file + EXPECTED_CHECKSUM_FILE_POSTFIX
                    if snapshot_checksum_file not in dataset_dir_file_names:
                        # skip snapshots without checksum file, verification is not possible without it
                        continue
                    snapshot_calculated_checksum_file = snapshot_file + CALCULATED_CHECKSUM_FILE_POSTFIX
                    if snapshot_calculated_checksum_file not in dataset_dir_file_names:
                        # skip snapshots without a calculated checksum file, verification is still pending for this
                        # snapshot/file
                        continue
                    logger.debug("found snapshot: {}".format(snapshot_name))
                    if snapshot_name in dataset.snapshots:
                        continue
                    snapshot = Snapshot(pool.pool_name, dataset.dataset_name, snapshot_name)
                    dataset.add_snapshot(snapshot)

            # dataset names are the ones that are directories
            for dataset_sub_dir in dataset_dir_subdir_names:
                sub_dataset_name = os.path.join(dataset_name, dataset_sub_dir)
                dataset_names.append(sub_dataset_name)

    return discovered_pools
