from ..Constants import SNAPSHOT_PREFIX_POSTFIX_SEPARATOR, INITIAL_SNAPSHOT_POSTFIX
from ..Zfs import Pool, DataSet, Snapshot


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


def make_next_backup_view(pool: Pool, snapshot_prefix: str) -> Pool:
    """
    Create a new Pool view with the next needed snapshots.
    """
    backup_snapshot_pool = pool.copy()
    for dataset in pool:
        backup_dataset = dataset.copy()
        backup_snapshot_pool.add_dataset(backup_dataset)

        backup_snapshot = get_next_backup_snapshot_for_dataset(dataset, snapshot_prefix)
        backup_dataset.add_snapshot(backup_snapshot)

    return backup_snapshot_pool