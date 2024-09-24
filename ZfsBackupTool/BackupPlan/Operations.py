from typing import Dict, Tuple, Optional, List

from ..Constants import SNAPSHOT_PREFIX_POSTFIX_SEPARATOR, INITIAL_SNAPSHOT_POSTFIX
from ..ShellCommand import SshHost
from ..Zfs import DataSet, Snapshot, PoolList, ZfsResolveError
from ..Zfs.errors import ZfsParseError


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
                          skip_view: Optional[PoolList] = None) -> PoolList:
    """
    Create a new Pool view with the next needed snapshots.
    """
    backup_view = pools.view()
    # drop all snapshots for view
    backup_view.drop_snapshots()
    for dataset in pools.iter_datasets():
        backup_snapshot = get_next_backup_snapshot_for_dataset(dataset, snapshot_prefix)
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
            except ZfsResolveError:
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


def find_snapshot_holes_of_dataset(dataset: DataSet, snapshot_prefix: str) -> DataSet:
    """
    Find holes in the snapshot chain of a dataset.
    """
    available_snapshot_indexes = []
    for snapshot in dataset.iter_snapshots():
        try:
            snapshot_name, snapshot_index = DataSet.parse_backup_snapshot(snapshot.snapshot_name)
        except ZfsParseError:
            continue
        if snapshot_name != snapshot_prefix:
            continue
        available_snapshot_indexes.append(snapshot_index)

    if not available_snapshot_indexes:
        return dataset.copy()

    # get min and max index
    min_index = min(available_snapshot_indexes)
    max_index = max(available_snapshot_indexes)

    full_indexes = set(range(min_index, max_index + 1))
    missing_indexes = full_indexes.difference(available_snapshot_indexes)

    mockup_snapshots = [Snapshot(dataset.pool_name, dataset.dataset_name,
                                 "{}{}{}".format(
                                     snapshot_prefix, SNAPSHOT_PREFIX_POSTFIX_SEPARATOR, str(index)))
                        for index in missing_indexes]

    holes_counter_dataset = dataset.copy()
    for snapshot in mockup_snapshots:
        holes_counter_dataset.add_snapshot(snapshot)

    # make view of the dataset
    calculation_base_dataset = dataset.view()
    merged_dataset = DataSet.merge(dataset.pool_name, calculation_base_dataset, holes_counter_dataset)

    # build incremental snapshot refs
    merged_dataset.build_incremental_snapshot_refs()

    # diff out the holes again and return them
    return merged_dataset.intersection(holes_counter_dataset)


def find_repairable_snapshots(source_pools: PoolList, target_pools: PoolList, incremental_only=False):
    # both source and target can be shifted previously
    # for our purposes we do not care about this, either restore or backup is ok
    # the only thing we are interested in, is if we want to do a full or incremental repair
    # full restores all missing items, incremental only the last missing

    repair_diff = source_pools.difference(target_pools)

    # if we only need the incremental snapshots, filter out all snapshots before the last snapshot
    # also check if the target has the dataset and also has newer snapshots than the last repair snapshot
    # for this however, we have to unify the snapshots of the datasets, so we can compare them properly
    if incremental_only:
        for dataset in repair_diff.iter_datasets():
            other_snapshots = list(dataset.iter_snapshots())[:-1]
            last_snapshot = list(dataset.iter_snapshots())[-1]
            for pre_incremental_snapshot in other_snapshots:
                dataset.remove_snapshot(pre_incremental_snapshot)
            try:
                target_dataset = target_pools.get_dataset_by_path(dataset.zfs_path)
            except ZfsResolveError:
                # no dataset found, no need to filter
                continue
            combined_snapshots = DataSet.merge(dataset.pool_name, dataset, target_dataset)
            combined_snapshots.build_incremental_snapshot_refs()
            logic_children = combined_snapshots.get_incremental_children(last_snapshot)
            existing_children = target_dataset.intersection(logic_children)
            if existing_children.has_snapshots():
                # target has newer snapshots, we do not need to restore the last needed snapshot - it's not needed
                # for incremental only mode
                dataset.remove_snapshot(last_snapshot)

    # we also have to include all snapshots, which might be present on the target side, but are still needed to restore
    # the dataset. we can only restore a snapshot, if no children are present. if children are present, we have to
    # delete them first.
    # so we have to include child snapshots which come after the last repair snapshot of each dataset
    full_repair_diff = repair_diff.copy()
    for dataset in repair_diff.iter_datasets():
        if not dataset.has_snapshots():
            # still add dataset to the full repair diff, even if it has no snapshots
            full_repair_diff.add_dataset(dataset)
            continue
        last_repair_snapshot = list(dataset.iter_snapshots())[-1]
        # get snapshots after the last repair snapshot from the target dataset
        try:
            target_dataset = target_pools.get_dataset_by_path(dataset.zfs_path)
        except ZfsResolveError:
            # no dataset found, already included in the repair diff
            full_repair_diff.add_dataset(dataset)
            continue
        # get all incremental children of the last repair snapshot
        incremental_children = target_dataset.get_incremental_children(last_repair_snapshot)
        # add merge it with the repair diff
        full_repair_dataset = DataSet.merge(dataset.pool_name, dataset, incremental_children)
        full_repair_diff.add_dataset(full_repair_dataset)

    full_repair_diff.build_incremental_snapshot_refs()

    return full_repair_diff


def find_conflicting_intermediate_snapshots(repair_diff: PoolList, complete_target: PoolList,
                                            skip_sortability=False) -> PoolList:
    """

    :param repair_diff:
    :param complete_target: a PoolList, which contains ALL available snapshots (snapshots created by us and by
    someone else). The snapshots MUST contain a creation date, so we can sort them in the correct order.
    :param skip_sortability: Skips the verification, that all snapshots contain a creation date. Useful if the
    complete target does not support such a thing as creation date (filesystem based zfs storage)
    :return:
    """
    if not skip_sortability:
        if not all(snapshot.has_creation_time() for snapshot in complete_target.iter_snapshots()):
            raise ValueError("All snapshots MUST have a creation date!")

    conflicting_intermediate_snapshots = PoolList()
    for dataset in repair_diff.iter_datasets():
        try:
            # target doesn't even have the dataset, nothing conflicting
            target_dataset = complete_target.get_dataset_by_path(dataset.zfs_path)
        except ZfsResolveError:
            continue
        # if the target dataset has snapshots which exist after the first repair snapshot, a restore would
        # either fail hard or would be useless.

        # Note: 'initial', '1' and '2' are snapshots made by us, so we can chain them together into a incremental
        # backup chain. there might be snapshots existing between the chains elements not made by us.

        # hard failure: initial -> 1 -> 2
        # repair:       initial -> X -> 2
        # zfs fails hard (exit 1) because we cannot restore a snapshot if another snapshot is in the
        # way.
        # soft failure: initial -> foreign.1 -> 1 -> foreign.2 -> 2
        # repair        initial -> foreign.1 -> X -> foreign.2 -> 2
        # in the incremental snapshot 1 stream (increment towards its base 'initial') the foreign.1 snapshot and
        # the '1' snapshot are included. on a restore, zfs receive would find the already existing foreign.1
        # snapshot and would report, that it already exists. the '1' snapshot gets then skipped and not restored
        # at all. however, zfs would exit with 0.

        # to avoid both cases, it would be best to find the incremental base for '1', which would be 'initial',
        # and then select all snapshots AFTER that incremental base.
        # these snapshots must then be deleted to perform a restore.
        # to avoid a resolution error, we must skip the 'initial' snapshot, because it has no base.

        first_repair_snapshot = list(dataset.iter_snapshots())[0]
        if first_repair_snapshot.snapshot_name.endswith(SNAPSHOT_PREFIX_POSTFIX_SEPARATOR + INITIAL_SNAPSHOT_POSTFIX):
            # initial snapshots have no incremental base, so we cannot find any conflicting snapshots reliably
            # to go even further, we couldn't even restore the snapshot, because zfs receive would fail
            # this case is better handled by the find_initial_conflicting_snapshots method
            continue
        incremental_base = first_repair_snapshot.get_incremental_base()
        potential_conflicting_snapshots = target_dataset.get_incremental_children(incremental_base)

        # add the found snapshots to the conflicting PoolList
        conflicting_intermediate_snapshots.add_dataset(potential_conflicting_snapshots)

    return conflicting_intermediate_snapshots


def find_initial_conflicting_snapshots(repair_diff: PoolList, complete_target: PoolList) -> PoolList:
    # same as find_conflicting_intermediate_snapshots, but now we only look at initial snapshots.
    # for our incremental snapshots, we always had at least the initial snapshot as a lookup anchor to find
    # conflicting intermediate snapshots.
    # when we restore an initial snapshot, we do not have an incremental base for it. this restore datastream
    # would force zfs receive to create a fully new dataset. if a dataset already exists at the restore target,
    # the restore process would fail.

    # we must find all initial snapshots, and then check if the complete target has a dataset with the snapshots
    # dataset name

    # we can then either rename, move, delete those datasets, or we can skip the restore process.

    hard_conflicting_datasets = PoolList()

    for snapshot in repair_diff.iter_snapshots():
        if snapshot.snapshot_name.endswith(SNAPSHOT_PREFIX_POSTFIX_SEPARATOR + INITIAL_SNAPSHOT_POSTFIX):
            try:
                target_dataset = complete_target.get_dataset_by_path(snapshot.dataset_zfs_path)
            except ZfsResolveError:
                # no dataset found, no conflict
                continue
            hard_conflicting_datasets.add_dataset(target_dataset)

    return hard_conflicting_datasets


def find_restore_chain_holes(restore_source: PoolList, snapshot_prefix: str) -> PoolList:
    """
    Find restore chain holes in the restore source.

    :param restore_source:
    :return:
    """

    holes = PoolList()
    # check, if the restore source is missing any snapshots. if this is the case, we cannot restore
    # because we cannot restore intermediate snapshots without the full chain of snapshots (A->B->C is ok,
    # A->C is not).
    for dataset in restore_source.iter_datasets():
        dataset_holes = find_snapshot_holes_of_dataset(dataset, snapshot_prefix)
        holes.add_dataset(dataset_holes)
    holes.drop_empty_datasets()
    return holes
