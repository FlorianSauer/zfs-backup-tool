from typing import Dict, Tuple, Optional, List

from ..Constants import SNAPSHOT_PREFIX_POSTFIX_SEPARATOR, INITIAL_SNAPSHOT_POSTFIX
from ..ShellCommand import SshHost
from ..Zfs import DataSet, Snapshot, PoolList, ZfsResolveError


class PlanningException(Exception):
    pass


def get_next_backup_snapshot_for_dataset(dataset: DataSet, snapshot_prefix: str) -> Snapshot:
    """
    Get the next needed snapshots for a dataset.
    """
    backup_snapshots = []
    for snapshot_name in sorted(dataset.snapshots.keys()):
        snapshot = dataset.snapshots[snapshot_name]
        if snapshot.snapshot_name.startswith(snapshot_prefix + SNAPSHOT_PREFIX_POSTFIX_SEPARATOR):
            backup_snapshots.append(snapshot)

    if not backup_snapshots:
        return Snapshot(dataset.pool_name, dataset.dataset_name,
                        "{}{}{}".format(snapshot_prefix,
                                        SNAPSHOT_PREFIX_POSTFIX_SEPARATOR,
                                        INITIAL_SNAPSHOT_POSTFIX))

    # the initial snapshot can be the last one due to the sorting
    # we need to correct the snapshot order by moving the initial snapshot to the beginning
    backup_snapshots = DataSet.sort_snapshots(backup_snapshots)

    last_snapshot = backup_snapshots[-1]
    last_snapshot_number = last_snapshot.snapshot_name.split(SNAPSHOT_PREFIX_POSTFIX_SEPARATOR)[-1]
    # number can also be "initial"
    if last_snapshot_number == INITIAL_SNAPSHOT_POSTFIX:
        next_snapshot_number = 1
    else:
        next_snapshot_number = int(last_snapshot_number) + 1

    new_backup_snapshot = Snapshot(dataset.pool_name, dataset.dataset_name,
                                   snapshot_prefix + SNAPSHOT_PREFIX_POSTFIX_SEPARATOR + str(next_snapshot_number))

    new_backup_snapshot.set_incremental_base(last_snapshot)
    return new_backup_snapshot


def make_next_backup_view(pools: PoolList, snapshot_prefix: str,
                          zfs_path_filter: Optional[str] = None,
                          skip_view: Optional[PoolList] = None) -> PoolList:
    """
    Create a new Pool view with the next needed snapshots.
    """
    backup_view = pools.view()
    # drop all snapshots for view
    backup_view.drop_snapshots()
    for dataset in pools.iter_datasets():
        backup_snapshot = get_next_backup_snapshot_for_dataset(dataset, snapshot_prefix)
        if zfs_path_filter and not backup_snapshot.zfs_path.startswith(zfs_path_filter):
            # we need to skip the snapshot which does not match the filter
            print("Skipping next backup snapshot for dataset: {}".format(backup_snapshot.dataset_zfs_path))
            continue
        if skip_view:
            # check if the snapshots dataset exists in the skip view and has snapshots
            try:
                skip_dataset = skip_view.get_dataset_by_path(backup_snapshot.dataset_zfs_path)
            except ZfsResolveError:
                # snapshot not found in skip view, add it to the backup views dataset
                pass
            else:
                if skip_dataset.has_snapshots():
                    # we need to skip the snapshot which is already in the skip_view
                    print("Skipping next backup snapshot for dataset: {}".format(backup_snapshot.dataset_zfs_path))
                    continue

        backup_view.get_dataset_by_path(dataset.zfs_path).add_snapshot(backup_snapshot)

    # drop all empty datasets from the backup view
    backup_view.drop_empty_datasets()

    return backup_view


def map_snapshots_to_data_sources(logic_pools: PoolList, data_sources: Dict[Tuple[Optional[SshHost], str], PoolList]
                                  ) -> List[Tuple[Snapshot, List[Tuple[Optional[SshHost], str]]]]:
    """
    Takes a list of logic pools and a dict of data sources.
    Maps all snapshots from the logic pools to the data sources.
    """
    # we only need the snapshot objects in the correct order and the host-target-path-mappings from where to fetch
    # the snapshot data from

    snapshot_restoresource_mapping: List[Tuple[Snapshot, List[Tuple[Optional[SshHost], str]]]] = []
    for dataset in logic_pools.iter_datasets():
        snapshot_sources: Dict[Snapshot, List[Tuple[Optional[SshHost], str]]] = {}
        for (host, target_path), remote_pools in data_sources.items():
            try:
                availabe_dataset = remote_pools.get_dataset_by_path(dataset.zfs_path)
            except KeyError:
                continue
            for snapshot in availabe_dataset.intersection(dataset):
                if snapshot not in snapshot_sources:
                    snapshot_sources[snapshot] = []
                snapshot_sources[snapshot].append((host, target_path))
        for snapshot in dataset.iter_snapshots():
            if snapshot not in snapshot_sources:
                print("Snapshot is missing on remote side")
                print("This would fail the restore process")
                print("We need to repair the remote side first")
                print("Missing snapshot:")
                snapshot.print()
                raise PlanningException("Missing snapshot on remote side")
            snapshot_restoresource_mapping.append((snapshot, snapshot_sources[snapshot]))
    return snapshot_restoresource_mapping
