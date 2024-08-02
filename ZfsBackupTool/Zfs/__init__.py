import os
from typing import List, Dict, Iterable, Union, Iterator

from ZfsBackupTool.ShellCommand import ShellCommand
from .Pool import Pool
from .Pool.Dataset import DataSet
from .Pool.Dataset.Snapshot import Snapshot

__all__ = ['build_incremental_snapshot_refs',
           'scan_zfs_pools', 'scan_filebased_zfs_pools',
           'merge_pools', 'difference_pools', 'intersection_pools',
           'Pool', 'DataSet', 'Snapshot']

from ..Constants import (BACKUP_FILE_POSTFIX, CHECKSUM_FILE_POSTFIX, SNAPSHOT_PREFIX_POSTFIX_SEPARATOR)


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


def difference_pools(base_pools: Union[Pool, Iterable[Pool]], *other_pools: Union[Pool, Iterable[Pool]],
                     ignore_missing_base=False) -> List[Pool]:
    """
    Calculate the difference between multiple ZFS pool objects or collections of pool objects.

    This function takes any number of arguments, each of which can be either a single Pool object or an iterable
    collection of Pool objects (e.g., list, set). It organizes the pools by their names and then calculates the
    difference between pools with the same name.
    """
    equal_pools: Dict[str, List[Pool]] = {}
    if isinstance(base_pools, Pool):
        equal_pools[base_pools.pool_name] = [base_pools]
    elif isinstance(base_pools, Iterable):
        for sub_pool in base_pools:
            equal_pools[sub_pool.pool_name] = [sub_pool]
    else:
        raise ValueError("Invalid pool type {}".format(type(base_pools)))

    for pool in other_pools:
        if isinstance(pool, Pool):
            if pool.pool_name not in equal_pools:
                if not ignore_missing_base:
                    raise ValueError("Pool '{}' not found in the base pools".format(pool.pool_name))
                else:
                    # ignore missing base pool, this simulates the following:
                    # >>> set().difference(set((1,2,3)))
                    # set()
                    pass
            else:
                equal_pools[pool.pool_name].append(pool)
        elif isinstance(pool, Iterable):
            for sub_pool in pool:
                if sub_pool.pool_name not in equal_pools:
                    if not ignore_missing_base:
                        raise ValueError("Pool '{}' not found in the base pools".format(sub_pool.pool_name))
                    else:
                        # ignore missing base pool, this simulates the following:
                        # >>> set().difference(set((1,2,3)))
                        # set()
                        pass
                else:
                    equal_pools[sub_pool.pool_name].append(sub_pool)
        else:
            raise ValueError("Invalid pool type {}".format(type(pool)))

    differenced_pools = []
    for pool_name, pool_list in equal_pools.items():
        base_pool = pool_list.pop()
        difference_pool = base_pool.difference(*pool_list)
        differenced_pools.append(difference_pool)

    return differenced_pools


def intersection_poolsold(base_pools: Union[Pool, Iterable[Pool]], *other_pools: Union[Pool, Iterable[Pool]],
                       ignore_missing_base=False) -> List[Pool]:
    """
    Calculate the intersection between multiple ZFS pool objects or collections of pool objects.

    This function takes any number of arguments, each of which can be either a single Pool object or an iterable
    collection of Pool objects (e.g., list, set). It organizes the pools by their names and then calculates the
    intersection between pools with the same name.
    """
    equal_pools: Dict[str, List[Pool]] = {}
    if isinstance(base_pools, Pool):
        equal_pools[base_pools.pool_name] = [base_pools]
    elif isinstance(base_pools, Iterable):
        for sub_pool in base_pools:
            equal_pools[sub_pool.pool_name] = [sub_pool]
    else:
        raise ValueError("Invalid pool type {}".format(type(base_pools)))

    for pool in other_pools:
        if isinstance(pool, Pool):
            if pool.pool_name not in equal_pools:
                if not ignore_missing_base:
                    raise ValueError("Pool '{}' not found in the base pools".format(pool.pool_name))
                else:
                    pass  # ignore missing base pool
            else:
                equal_pools[pool.pool_name].append(pool)
        elif isinstance(pool, Iterable):
            for sub_pool in pool:
                if sub_pool.pool_name not in equal_pools:
                    if not ignore_missing_base:
                        raise ValueError("Pool '{}' not found in the base pools".format(sub_pool.pool_name))
                    else:
                        pass  # ignore missing base pool
                else:
                    equal_pools[sub_pool.pool_name].append(sub_pool)
        else:
            raise ValueError("Invalid pool type {}".format(type(pool)))

    differenced_pools = []
    for pool_name, pool_list in equal_pools.items():
        base_pool = pool_list.pop()
        difference_pool = base_pool.intersection(*pool_list)
        differenced_pools.append(difference_pool)

    return differenced_pools

def intersection_pools(*pools: Union[Pool, Iterable[Pool]]) -> List[Pool]:
    """
    Calculate the intersection between multiple ZFS pool objects or collections of pool objects.

    This function takes any number of arguments, each of which can be either a single Pool object or an iterable
    collection of Pool objects (e.g., list, set). It organizes the pools by their names and then calculates the
    intersection between pools with the same name.
    """
    # for item in pools:
    #     if isinstance(item, Pool):
    #         print("item is pool")
    #         item.print()
    #     elif isinstance(item, Iterable):
    #         for sub_pool in item:
    #             print("item is sub pool")
    #             sub_pool.print()
    equal_pools: Dict[str, List[Pool]] = {}
    for item in pools:
        if isinstance(item, Pool):
            if not item.pool_name in equal_pools:
                equal_pools[item.pool_name] = []
            equal_pools[item.pool_name].append(item)
        elif isinstance(item, Iterable):
            sub_pool: Pool
            for sub_pool in item:
                if not sub_pool.pool_name in equal_pools:
                    equal_pools[sub_pool.pool_name] = []
                equal_pools[sub_pool.pool_name].append(sub_pool)
        else:
            raise ValueError("Invalid pool type {}".format(type(item)))

    intersection_pools = []
    for pool_name, pool_list in equal_pools.items():
        if len(pool_list) == 1:
            # skip pools with only one pool, second comparison pool would be an empty pool
            # equal example: set((1,2,3)).intersection(set()) == set()
            continue
        base_pool = pool_list.pop()
        # print("using base pool: ")
        # base_pool.print()
        intersection_pool = base_pool.intersection(*pool_list)
        intersection_pools.append(intersection_pool)

    return intersection_pools


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
        pool_name, _ = zfs_path.split("/", 1)
        if pool_name in self.pools:
            return self.pools[pool_name].resolve_zfs_path(zfs_path)
        raise ValueError("Pool '{}' not found in the pool list".format(pool_name))

    def print(self):
        for pool in self:
            pool.print()

    @classmethod
    def merge(cls, *others: 'PoolList') -> 'PoolList':
        return cls(merge_pools(*(p.pools.values() for p in others)))

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
        return PoolList(intersection_pools(list(self.pools.values()),
                                           *(list(p.pools.values()) for p in other_pool_lists)))

    def has_snapshots(self):
        return any(pool.has_snapshots() for pool in self.pools.values())


def build_incremental_snapshot_refs(pool_list: PoolList) -> None:
    """
    Build incremental snapshot references for all snapshots in the pool list.
    """
    for pool in pool_list:
        for dataset in pool:
            sorted_snapshots = DataSet.sort_snapshots(dataset.snapshots.values())
            snapshot_prefixes = set()
            for snapshot in sorted_snapshots:
                snapshot_prefix = snapshot.snapshot_name.rsplit(SNAPSHOT_PREFIX_POSTFIX_SEPARATOR, 1)[0]
                snapshot_prefixes.add(snapshot_prefix)

            for snapshot_prefix in snapshot_prefixes:
                incremental_base = None
                for snapshot in sorted_snapshots:
                    if snapshot.snapshot_name.startswith(snapshot_prefix + SNAPSHOT_PREFIX_POSTFIX_SEPARATOR):
                        if incremental_base:
                            snapshot.set_incremental_base(incremental_base)
                        incremental_base = snapshot


def scan_zfs_pools(shell_command: ShellCommand) -> PoolList:
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

    return PoolList(*pools)


def scan_filebased_zfs_pools(shell_command: ShellCommand, target_pool_storage_path: str) -> PoolList:
    discovered_pools: PoolList = PoolList()

    if not shell_command.target_dir_exists(target_pool_storage_path):
        return PoolList(discovered_pools)

    _, pool_names = shell_command.target_list_directory(target_pool_storage_path)
    print("Found pools: ", pool_names)
    for pool_name in pool_names:
        pool = Pool(pool_name)
        discovered_pools.add_pool(pool)

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
