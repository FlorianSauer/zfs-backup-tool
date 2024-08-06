import os
import sys
from typing import Dict, Tuple, Optional, Set, List

from ..Constants import TARGET_STORAGE_SUBDIRECTORY, BACKUP_FILE_POSTFIX, EXPECTED_CHECKSUM_FILE_POSTFIX, \
    CHECKSUM_FILE_POSTFIX
from ..ShellCommand import ShellCommand, SshHost
from ..ShellCommand.Base import CommandExecutionError
from ..Zfs import Pool, Snapshot, PoolList


class BackupPlan(object):
    def __init__(self, shell_command: ShellCommand,
                 include_intermediate_snapshots: bool = False,
                 dry_run: bool = False,
                 # snapshot_prefix: str
                 ):
        self.shell_command = shell_command
        self.include_intermediate_snapshots = include_intermediate_snapshots
        self.dry_run = dry_run
        # self.snapshot_prefix = snapshot_prefix

    def create_snapshots(self, *pools: Pool):
        for pool in pools:
            for dataset in pool:
                for snapshot in dataset:
                    if self.dry_run:
                        print("Would have created snapshot: ", snapshot)
                    else:
                        self.shell_command.create_snapshot(dataset.zfs_path, snapshot.snapshot_name)

    def verify_pool(self, pool: Pool):
        # checks if the pool exists on the target pool
        # if not self.shell_command.target_dir_exists()
        # checks if all snapshots exist on the target pool and verifies them
        if not self.shell_command.target_dir_exists():
            return False

    def verify_snapshot(self, snapshot: Snapshot):
        # checks if the snapshot file exists on the target pool
        # if not self.shell_command.target_file_exists()
        # checks if the snapshot file has a checksum file beside it
        # checks if the checksum was calculated and stored in a second checksum file
        pass

    def _write_snapshot_to_target(self, snapshot: Snapshot, host: Optional[SshHost], target_paths: Set[str],
                                  overwrite: bool = False):
        # repair the snapshot on the target pool
        self.shell_command.set_remote_host(host)

        source_dataset = snapshot.dataset_path
        if snapshot.has_increment_base():
            previous_snapshot = snapshot.get_incremental_base().snapshot_name
        else:
            previous_snapshot = None
        next_snapshot = snapshot.snapshot_name

        for target_path in target_paths:
            self.shell_command.target_mkdir(os.path.join(target_path, TARGET_STORAGE_SUBDIRECTORY, source_dataset))

        target_path_files = {tp: self.shell_command.target_list_directory(os.path.join(tp, TARGET_STORAGE_SUBDIRECTORY,
                                                                                       source_dataset))[0]
                             for tp in target_paths}

        skip_zfs_send = False
        skip_verification = False
        backup_checksum = None

        if not overwrite:
            remotes_have_snapshot_file = (
                next_snapshot + BACKUP_FILE_POSTFIX in target_path_files[tp]
                for tp in target_paths)
            if all(remotes_have_snapshot_file):
                remotes_have_checksum_file = (
                    next_snapshot + BACKUP_FILE_POSTFIX + CHECKSUM_FILE_POSTFIX in target_path_files[tp]
                    for tp in target_paths)
                if all(remotes_have_checksum_file):
                    print("Backup {}@{} already exists on all targets and checksums were written.".format(
                        source_dataset, next_snapshot))
                    skip_zfs_send = True
                    skip_verification = True
                else:
                    print("Unusual State detected: checksum file missing on some targets.")

                    remotes_have_temporary_checksum_file = (
                        next_snapshot + BACKUP_FILE_POSTFIX + EXPECTED_CHECKSUM_FILE_POSTFIX in target_path_files[tp]
                        for tp in target_paths
                    )
                    if any(remotes_have_temporary_checksum_file):
                        for target_path in target_paths:
                            try:
                                backup_checksum = self.shell_command.target_read_checksum_from_file(
                                    os.path.join(target_path, TARGET_STORAGE_SUBDIRECTORY, source_dataset,
                                                 next_snapshot + BACKUP_FILE_POSTFIX + EXPECTED_CHECKSUM_FILE_POSTFIX))
                            except CommandExecutionError:
                                pass
                            else:
                                print('Found checksum for backup "{}@{}" on target "{}": {}'.format(
                                    source_dataset, next_snapshot, target_path, backup_checksum))
                                break
                        if backup_checksum:
                            print('Skipping re-writing of backup "{}@{}" because it already exists on all targets and '
                                  'a checksum was found.'.format(source_dataset, next_snapshot))
                            skip_zfs_send = True
            # else:
            #     print("Unusual State detected: Backup {}@{} does not exist on any target.".format(
            #         source_dataset, next_snapshot))

        if not skip_zfs_send:
            print("Transmitting backup snapshot {}@{} to target(s): {}...".format(
                source_dataset, next_snapshot, ", ".join(sorted(target_paths))))
            if self.dry_run:
                backup_checksum = "dry-run"
                if previous_snapshot:
                    print("Would have sent incremental backup from {}@{} to {}@{}".format(
                        source_dataset, previous_snapshot, source_dataset, next_snapshot))
                else:
                    print("Would have sent full backup from {}@{}".format(source_dataset, next_snapshot))
            else:
                backup_checksum = self.shell_command.zfs_send_snapshot_to_target(
                    source_dataset,
                    previous_snapshot,
                    next_snapshot,
                    target_paths,
                    include_intermediate_snapshots=self.include_intermediate_snapshots)
            print("Created backup snapshot {}@{} with checksum {}".format(
                source_dataset, next_snapshot, backup_checksum))

            # after transmission, write checksum to temporary file on target
            # it gets replaced later by the 'final' checksum file
            for target_path in target_paths:
                if not self.dry_run:
                    self.shell_command.target_write_to_file(
                        os.path.join(target_path, TARGET_STORAGE_SUBDIRECTORY, source_dataset,
                                     next_snapshot + BACKUP_FILE_POSTFIX + EXPECTED_CHECKSUM_FILE_POSTFIX),
                        "{} ./{}".format(backup_checksum, next_snapshot + BACKUP_FILE_POSTFIX))
        else:
            assert backup_checksum

        if not skip_verification:
            print("Verifying written backups...")
            if self.dry_run:
                read_checksums = {tp: "dry-run" for tp in target_paths}
            else:
                read_checksums = self.shell_command.target_get_checksums(source_dataset, next_snapshot, target_paths)
            checksum_mismatch = False
            for target_path, read_checksum in read_checksums.items():
                if read_checksum != backup_checksum:
                    print("Checksum mismatch for backup {}@{} on target {}".format(
                        source_dataset, next_snapshot, target_path))
                    print("Expected checksum: {}".format(backup_checksum))
                    print("Read checksum: {}".format(read_checksum))
                    checksum_mismatch = True
                else:
                    print("Checksum verified for backup {}@{} on target {}".format(
                        source_dataset, next_snapshot, target_path))
            if checksum_mismatch:
                print("Aborting...")
                sys.exit(1)

            for target_path in target_paths:
                if not self.dry_run:
                    self.shell_command.target_write_to_file(
                        os.path.join(target_path, TARGET_STORAGE_SUBDIRECTORY, source_dataset,
                                     next_snapshot + BACKUP_FILE_POSTFIX + CHECKSUM_FILE_POSTFIX),
                        "{} ./{}".format(backup_checksum, next_snapshot + BACKUP_FILE_POSTFIX))

        for target_path in target_paths:
            if not self.dry_run:
                self.shell_command.target_remove_file(
                    os.path.join(target_path, TARGET_STORAGE_SUBDIRECTORY, source_dataset,
                                 next_snapshot + BACKUP_FILE_POSTFIX + EXPECTED_CHECKSUM_FILE_POSTFIX))

    def _group_target_paths_by_host(self, given_pools: Dict[Tuple[Optional[SshHost], str], PoolList]
                                    ) -> Dict[Optional[SshHost], Dict[str, PoolList]]:
        host_targetpaths_pools: Dict[Optional[SshHost], Dict[str, PoolList]] = {}
        for (host, target_path), pools in given_pools.items():
            if host not in host_targetpaths_pools:
                host_targetpaths_pools[host] = {}
            if target_path not in host_targetpaths_pools[host]:
                host_targetpaths_pools[host][target_path] = pools
            else:
                host_targetpaths_pools[host][target_path] = PoolList.merge(
                    host_targetpaths_pools[host][target_path], pools)
        return host_targetpaths_pools

    def _group_target_paths(self, target_paths_pools: Dict[str, PoolList]) -> Dict[Tuple[str], PoolList]:
        pool_target_paths: Dict[Tuple[str], PoolList] = {}
        for target_path in sorted(target_paths_pools.keys()):
            pools = target_paths_pools[target_path]
            for compare_paths, compare_pools in list(pool_target_paths.items()):
                shared_pools = compare_pools.intersection(pools)
                pools_leftovers = shared_pools.difference(pools)
                compare_pools_leftovers = shared_pools.difference(compare_pools)
                if shared_pools.has_snapshots():
                    pool_target_paths.pop(compare_paths)
                    new_shared_pools_key = compare_paths + (target_path,)
                    assert new_shared_pools_key not in pool_target_paths
                    pool_target_paths[new_shared_pools_key] = shared_pools
                if compare_pools_leftovers.has_snapshots():
                    pool_target_paths[compare_paths] = compare_pools_leftovers

                # push back the difference which still needs to find a home
                pools = pools_leftovers
            if pools.has_snapshots():
                pool_target_paths[(target_path,)] = pools
        return pool_target_paths

    def repair_snapshots_old(self, repair_pools: Dict[Tuple[Optional[SshHost], str], PoolList]):
        """
        Repairs missing snapshots in the given pools on the given targets.
        Equal targets get grouped together, to reduce transmitted data.
        """
        # group target paths by host
        host_targetpaths_pools = self._group_target_paths_by_host(repair_pools)

        # iterate over all hosts, group pools from different target paths together, to repair them in one go
        for host, targetpaths_pools in host_targetpaths_pools.items():
            target_path_all_repair_pools = PoolList.merge(*targetpaths_pools.values())
            target_path_shared_repair_pools = target_path_all_repair_pools.intersection(
                *targetpaths_pools.values())
            self.shell_command.set_remote_host(host)

            # filter out all snapshots, that are missing in all target paths
            print("Fully missing snapshots:")
            fully_missing_snapshots = target_path_shared_repair_pools.view()
            for pools in targetpaths_pools.values():
                fully_missing_snapshots = pools.intersection(fully_missing_snapshots)
            fully_missing_snapshots.print()

            # iter snapshots, repair them
            for snapshot in fully_missing_snapshots.iter_snapshots():
                print("Repairing snapshot: ", snapshot.zfs_path)
                assert all(snapshot in pools.iter_snapshots() for pools in targetpaths_pools.values())
                self._write_snapshot_to_target(snapshot, host, set(targetpaths_pools.keys()))

            # filter out all snapshots, that are missing in at least one target path
            print("Partially missing snapshots:")
            for target_path, pools in targetpaths_pools.items():
                partially_missing_snapshots = target_path_shared_repair_pools.difference(pools)
                print("Target path: ", target_path)
                partially_missing_snapshots.print()
                for snapshot in partially_missing_snapshots.iter_snapshots():
                    print("Repairing snapshot: ", snapshot.zfs_path)
                    self._write_snapshot_to_target(snapshot, host, {target_path})

            print()
    def repair_snapshots(self, repair_pools: Dict[Tuple[Optional[SshHost], str], PoolList]):
        """
        Repairs all snapshots in the given pools on the given (remote) targets.
        Snapshots that need to be transferred to multiple targets get grouped together, to reduce transmitted data.
        """
        # group target paths by host
        host_targetpaths_pools = self._group_target_paths_by_host(repair_pools)

        # iterate over all hosts, group pools from different target paths together, to repair them in one go
        for host, targetpaths_pools in host_targetpaths_pools.items():
            # combine pools with equal target paths
            pool_target_paths = self._group_target_paths(targetpaths_pools)

            for target_paths, pools in pool_target_paths.items():
                print("writing snapshots to target paths: ", target_paths)
                pools.print()
                for snapshot in pools.iter_snapshots():
                    print("Repairing snapshot: ", snapshot.zfs_path)
                    self._write_snapshot_to_target(snapshot, host, set(target_paths))

            print()
    def restore_snapshots(self, restore_pools: Dict[Tuple[Optional[SshHost], str], PoolList]):
        # group target paths by host
        host_targetpaths_pools = self._group_target_paths_by_host(restore_pools)

        # iterate over all hosts, group pools from different target paths together, to repair them in one go
        for host, targetpaths_pools in host_targetpaths_pools.items():
            # combine pools with equal target paths
            pool_target_paths = self._group_target_paths(targetpaths_pools)

            for target_paths, pools in pool_target_paths.items():
                print("Reading snapshots from target paths: ", target_paths)
                pools.print()
                # for snapshot in pools.iter_snapshots():
                #     print("Repairing snapshot: ", snapshot.zfs_path)
                #     self._write_snapshot_to_target(snapshot, host, set(target_paths))

            print()

    def backup_snapshots(self, backup_pools: Dict[Tuple[Optional[SshHost], str], PoolList]):
        """
        Backups all snapshots in the given pools on the given (remote) targets.
        Snapshots that need to be transferred to multiple targets get grouped together, to reduce transmitted data.
        """
        # group target paths by host
        host_targetpaths_pools = self._group_target_paths_by_host(backup_pools)

        # iterate over all hosts, group pools from different target paths together, to repair them in one go
        for host, targetpaths_pools in host_targetpaths_pools.items():
            # combine pools with equal target paths
            pool_target_paths = self._group_target_paths(targetpaths_pools)

            for target_paths, pools in pool_target_paths.items():
                print("writing snapshots to target paths: ", target_paths)
                pools.print()
                for snapshot in pools.iter_snapshots():
                    print("writing backup snapshot: ", snapshot.zfs_path)
                    self._write_snapshot_to_target(snapshot, host, set(target_paths))

            print()
