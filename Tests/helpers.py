import random
from typing import Union

from ZfsBackupTool.Zfs import DataSet, Snapshot, Pool, PoolList


def make_dataset(pool_name: str, dataset_name: str, snapshot_count: int) -> DataSet:
    dataset = DataSet(pool_name, dataset_name)
    for i in range(snapshot_count):
        dataset.add_snapshot(Snapshot(pool_name, dataset_name, "snapshot_{}".format(i)))
    return dataset


def make_pool(pool_name: str, dataset_count: int, snapshot_count: int) -> Pool:
    pool = Pool(pool_name)
    for i in range(dataset_count):
        pool.add_dataset(make_dataset(pool_name, "dataset_{}".format(i), snapshot_count))
    return pool


def make_poollist(pool_count: int, dataset_count: int, snapshot_count: int) -> PoolList:
    poollist = PoolList()
    for i in range(pool_count):
        poollist.add_pool(make_pool("pool_{}".format(i), dataset_count, snapshot_count))
    return poollist


def pop_random_snapshot(obj: Union[PoolList, Pool, DataSet]) -> Snapshot:
    if isinstance(obj, PoolList):
        random_pool = random.choice(list(obj))
        random_dataset = random.choice(list(random_pool))
        random_snapshot = random.choice(list(random_dataset))
        random_dataset.remove_snapshot(random_snapshot)
    elif isinstance(obj, Pool):
        random_dataset = random.choice(list(obj))
        random_snapshot = random.choice(list(random_dataset))
        random_dataset.remove_snapshot(random_snapshot)
    elif isinstance(obj, DataSet):
        random_snapshot = random.choice(list(obj))
        obj.remove_snapshot(random_snapshot)
    else:
        raise ValueError("Unknown object type")
    return random_snapshot
