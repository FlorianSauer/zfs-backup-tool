import os
from typing import List, Dict, Iterable, Union, Iterator

from ZfsBackupTool.ShellCommand import ShellCommand
from .Pool import Pool
from .Pool.Dataset import DataSet
from .Pool.Dataset.Snapshot import Snapshot

__all__ = ['scan_zfs_pools', 'scan_filebased_zfs_pools',
           'merge_pools', 'difference_pools', 'intersection_pools',
           'Pool', 'DataSet', 'Snapshot']

from ..Constants import (BACKUP_FILE_POSTFIX, CHECKSUM_FILE_POSTFIX)


def scan_zfs_pools(shell_command: ShellCommand) -> List[Pool]:
    """
    Scan the local system for ZFS datasets.
    """
    # first scan for pools
    pool_names = shell_command.list_pools()
    print("Found pools: ", pool_names)
    pools = [Pool(pool_name) for pool_name in pool_names]

    # iter pools, add datasets, add snapshots
    for pool in pools:
        pool_dataset_names = shell_command.list_datasets(pool.pool_name)
        print("Found datasets for pool {}: ".format(pool.pool_name), pool_dataset_names)
        for dataset_name in pool_dataset_names:
            dataset = DataSet(pool.pool_name, dataset_name)
            pool.add_dataset(dataset)

            dataset_snapshot_names = shell_command.list_snapshots(dataset.zfs_path)
            print("Found snapshots for dataset {}: ".format(dataset.zfs_path), dataset_snapshot_names)

            for snapshot_name in dataset_snapshot_names:
                snapshot = Snapshot(pool.pool_name, dataset.dataset_name, snapshot_name)
                dataset.add_snapshot(snapshot)

    return pools


def scan_filebased_zfs_pools(shell_command: ShellCommand, target_pool_storage_path: str) -> List[Pool]:
    discovered_pools = []

    if not shell_command.target_dir_exists(target_pool_storage_path):
        return discovered_pools

    _, pool_names = shell_command.target_list_directory(target_pool_storage_path)
    print("Found pools: ", pool_names)
    for pool_name in pool_names:
        pool = Pool(pool_name)
        discovered_pools.append(pool)

    for pool in discovered_pools:
        pool_target_path = os.path.join(target_pool_storage_path, pool.pool_name)
        # prime dataset_dirs with the dataset names
        files, dataset_names = shell_command.target_list_directory(pool_target_path)
        print("Found top level datasets for pool {}: ".format(pool.pool_name), dataset_names)

        # analyze datasets while we have some
        while dataset_names:
            dataset_name = dataset_names.pop()
            dataset_zfs_path = pool.resolve_dataset_name(dataset_name)
            dataset_target_path = os.path.join(target_pool_storage_path, dataset_zfs_path)

            dataset_dir_file_names, dataset_dir_subdir_names = shell_command.target_list_directory(
                dataset_target_path)
            print("Found files for dataset {}: ".format(dataset_zfs_path), dataset_dir_file_names)
            print("Found folders for dataset {}: ".format(dataset_zfs_path), dataset_dir_subdir_names)

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
                    print("Adding dataset: ", dataset_name)
                    pool.add_dataset(dataset)
                for snapshot_file in snapshot_files:
                    snapshot_name = snapshot_file.replace(BACKUP_FILE_POSTFIX, "")
                    snapshot_checksum_file = snapshot_file + CHECKSUM_FILE_POSTFIX
                    if snapshot_checksum_file not in dataset_dir_file_names:
                        # skip snapshots without checksum file, verification is not possible without it
                        continue
                    print("found snapshot: ", snapshot_name)
                    if snapshot_name in dataset.snapshots:
                        continue
                    snapshot = Snapshot(pool.pool_name, dataset.dataset_name, snapshot_name)
                    dataset.add_snapshot(snapshot)

            # dataset names are the ones that are directories
            for dataset_sub_dir in dataset_dir_subdir_names:
                sub_dataset_name = os.path.join(dataset_name, dataset_sub_dir)
                dataset_names.append(sub_dataset_name)

    return discovered_pools


def merge_pools(*pools: Union[Pool, Iterable[Pool]]) -> List[Pool]:
    """
    Combine pools with the same name into a single pool.
    """
    equal_pools: Dict[str, List[Pool]] = {}
    for pool in pools:
        if isinstance(pool, Pool):
            if pool.pool_name in equal_pools:
                equal_pools[pool.pool_name].append(pool)
            else:
                equal_pools[pool.pool_name] = [pool]
        elif isinstance(pool, Iterable):
            for sub_pool in pool:
                if sub_pool.pool_name in equal_pools:
                    equal_pools[sub_pool.pool_name].append(sub_pool)
                else:
                    equal_pools[sub_pool.pool_name] = [sub_pool]
        else:
            raise ValueError("Invalid pool type {}".format(type(pool)))

    merged_pools = []
    for pool_name, pool_list in equal_pools.items():
        merged_pool = Pool.merge(*pool_list)
        merged_pools.append(merged_pool)

    return merged_pools


def difference_pools(*pools: Union[Pool, Iterable[Pool]]) -> List[Pool]:
    """
    Calculate the difference between multiple ZFS pool objects or collections of pool objects.

    This function takes any number of arguments, each of which can be either a single Pool object or an iterable
    collection of Pool objects (e.g., list, set). It organizes the pools by their names and then calculates the
    difference between pools with the same name.
    For multiple pools with the same name, the respectively first pool is used as the base pool for the difference
    operation.
    """
    equal_pools: Dict[str, List[Pool]] = {}
    for pool in pools:
        if isinstance(pool, Pool):
            if pool.pool_name in equal_pools:
                equal_pools[pool.pool_name].append(pool)
            else:
                equal_pools[pool.pool_name] = [pool]
        elif isinstance(pool, Iterable):
            for sub_pool in pool:
                if sub_pool.pool_name in equal_pools:
                    equal_pools[sub_pool.pool_name].append(sub_pool)
                else:
                    equal_pools[sub_pool.pool_name] = [sub_pool]
        else:
            raise ValueError("Invalid pool type {}".format(type(pool)))

    differenced_pools = []
    for pool_name, pool_list in equal_pools.items():
        base_pool = pool_list.pop()
        difference_pool = base_pool.difference(*pool_list)
        differenced_pools.append(difference_pool)

    return differenced_pools


def intersection_pools(*pools: Union[Pool, Iterable[Pool]]) -> List[Pool]:
    """
    Calculate the intersection between multiple ZFS pool objects or collections of pool objects.

    This function takes any number of arguments, each of which can be either a single Pool object or an iterable
    collection of Pool objects (e.g., list, set). It organizes the pools by their names and then calculates the
    intersection between pools with the same name.
    For multiple pools with the same name, the respectively first pool is used as the base pool for the intersection
    operation.
    """
    equal_pools: Dict[str, List[Pool]] = {}
    for pool in pools:
        if isinstance(pool, Pool):
            if pool.pool_name in equal_pools:
                equal_pools[pool.pool_name].append(pool)
            else:
                equal_pools[pool.pool_name] = [pool]
        elif isinstance(pool, Iterable):
            for sub_pool in pool:
                if sub_pool.pool_name in equal_pools:
                    equal_pools[sub_pool.pool_name].append(sub_pool)
                else:
                    equal_pools[sub_pool.pool_name] = [sub_pool]
        else:
            raise ValueError("Invalid pool type {}".format(type(pool)))

    intersected_pools = []
    for pool_name, pool_list in equal_pools.items():
        base_pool = pool_list.pop()
        intersection_pool = base_pool.intersection(*pool_list)
        intersected_pools.append(intersection_pool)

    return intersected_pools


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

    def __eq__(self, other: 'PoolList'):
        # our pools and the other pools must be the same
        if set(self.pools.keys()) != set(other.pools.keys()):
            return False
        # finally check the pools itself and other attributes
        for pool_name in self.pools.keys():
            if self.pools[pool_name] != other.pools[pool_name]:
                return False
        return True

    def copy(self):
        return PoolList()

    def view(self):
        return PoolList(*[pool.view() for pool in self.pools.values()])

    def add_pool(self, pool: Pool):
        if pool.pool_name in self.pools:
            raise ValueError("Pool '{}' already added to the pool list".format(pool.pool_name))
        self.pools[pool.pool_name] = pool

    def remove_pool(self, pool: Pool):
        if pool.pool_name not in self.pools:
            raise ValueError("Pool '{}' not found in the pool list".format(pool.pool_name))
        self.pools.pop(pool.pool_name)

    def print(self):
        for pool in self:
            pool.print()

    @classmethod
    def merge(cls, *others: 'PoolList') -> 'PoolList':
        return cls(merge_pools(*(p.pools.values() for p in others)))

    def difference(self, *other_pool_lists: 'PoolList') -> 'PoolList':
        return PoolList(difference_pools(self.pools.values(), *(p.pools.values() for p in other_pool_lists)))

    def intersection(self, *other_pool_lists: 'PoolList') -> 'PoolList':
        return PoolList(intersection_pools(self.pools.values(), *(p.pools.values() for p in other_pool_lists)))
