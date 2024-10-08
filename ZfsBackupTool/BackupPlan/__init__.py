import os
import sys
from itertools import combinations
from typing import Dict, Tuple, Optional, Set, List, cast, Mapping

from ..Constants import (TARGET_STORAGE_SUBDIRECTORY, BACKUP_FILE_POSTFIX, EXPECTED_CHECKSUM_FILE_POSTFIX,
                         CALCULATED_CHECKSUM_FILE_POSTFIX, INITIAL_SNAPSHOT_POSTFIX, SNAPSHOT_PREFIX_POSTFIX_SEPARATOR)
from ..ShellCommand import ShellCommand, SshHost
from ..ShellCommand.Base import CommandExecutionError
from ..ShellCommand.ZfsCommands import ZfsCommandsError
from ..Zfs import Snapshot, PoolList, DataSet


class BackupPlan(object):
    def __init__(self, shell_command: ShellCommand,
                 include_intermediate_snapshots: bool = False,
                 dry_run: bool = False,
                 debug: bool = False,
                 ):
        self.shell_command = shell_command
        self.include_intermediate_snapshots = include_intermediate_snapshots
        self.dry_run = dry_run
        self.debug = debug

    def create_snapshots(self, pools: PoolList):
        for snapshot in pools.iter_snapshots():
            if self.dry_run:
                print("Would have created snapshot:", snapshot.zfs_path)
            else:
                print("Creating new snapshot:", snapshot.zfs_path)
                self.shell_command.create_snapshot(snapshot.dataset_zfs_path, snapshot.snapshot_name)

    def verify_snapshots(self, verify_pools: Dict[Tuple[Optional[SshHost], str], PoolList],
                         remove_invalid: bool = False, target_path_prefix_filter: Optional[str] = None,
                         zfs_path_prefix_filter: Optional[str] = None) -> Dict[Tuple[Optional[SshHost], str], PoolList]:
        # iters all snapshots, checks various things
        # - expected checksum file must exist beside the snapshot file
        # - if no expected checksum file exists, try to borrow it from another target if possible
        # - if no calculated checksum file exists, recalculate the checksum and write it to the calculated checksum file
        # - if a calculated checksum file exists, it must match the expected checksum
        #

        # apply filters before further processing
        if target_path_prefix_filter:
            verify_pools = {host_target: pools for host_target, pools in verify_pools.items()
                            if host_target[1].startswith(target_path_prefix_filter)}
        if zfs_path_prefix_filter:
            new_verify_pools = {}
            for host_target, pools in verify_pools.items():
                filtered_pools = pools.filter_include_by_zfs_path_prefix(zfs_path_prefix_filter)
                if filtered_pools.has_snapshots():
                    new_verify_pools[host_target] = filtered_pools
            verify_pools = new_verify_pools

        # group target paths by host, used for the parallel writing of backups (tee command parameters)
        host_targetpaths_pools = self._group_target_paths_by_host(verify_pools)

        invalid_host_pools = {host_target: PoolList() for host_target in verify_pools.keys()}

        # iterate over all hosts, group pools from different target paths together, to repair them in one go
        for host, targetpaths_pools in host_targetpaths_pools.items():
            # combine pools with equal target paths
            pool_target_paths = self._group_target_paths(targetpaths_pools)

            for target_paths, pools in pool_target_paths.items():
                for dataset in pools.iter_datasets():
                    invalid_datasets = {target_path: dataset.copy() for target_path in target_paths}
                    print("Verifying dataset: ", dataset.zfs_path)
                    for snapshot in dataset.iter_snapshots():
                        print("Verifying snapshot: ", snapshot.zfs_path)
                        invalid_targets = self._verify_snapshot_on_target(snapshot, host, list(target_paths),
                                                                          remove_invalid=remove_invalid,
                                                                          force_recalculate=True)
                        if invalid_targets:
                            for invalid_target in invalid_targets:
                                invalid_datasets[invalid_target].add_snapshot(snapshot.view())

                    for target_path, invalid_dataset in invalid_datasets.items():
                        if invalid_dataset.has_snapshots():
                            invalid_host_pools[(host, target_path)].add_dataset(invalid_dataset)

        return invalid_host_pools

    def _checksum_verify_helper(self, target_paths: List[str], snapshot: Snapshot,
                                expected_checksums: Dict[str, Optional[str]],
                                calculated_checksums: Dict[str, Optional[str]]
                                ) -> Dict[str, Tuple[Optional[str], Optional[str]]]:
        """
        Returns a dictionary with the target paths as keys and the expected and calculated checksums as values.
        """
        mismatching_checksums = {}
        for target_path in target_paths:
            expected_checksum = expected_checksums[target_path]
            calculated_checksum = calculated_checksums[target_path]
            if not expected_checksum:
                print("Expected checksum missing for backup {}@{} on target {}".format(
                    snapshot.dataset_zfs_path, snapshot.snapshot_name, target_path))
                mismatching_checksums[target_path] = (expected_checksum, calculated_checksum)
                continue
            if expected_checksum == calculated_checksum:
                print("Checksum verified for backup {}@{} on target {}".format(
                    snapshot.dataset_zfs_path, snapshot.snapshot_name, target_path))
                continue

            print("Checksum mismatch for backup {}@{} on target {}".format(
                snapshot.dataset_zfs_path, snapshot.snapshot_name, target_path))
            print("Expected checksum: {}".format(expected_checksum))
            print("Calculated checksum: {}".format(calculated_checksum))
            mismatching_checksums[target_path] = (expected_checksum, calculated_checksum)
        return mismatching_checksums

    def _verify_snapshot_on_target(self, snapshot: Snapshot, host: Optional[SshHost], target_paths: List[str],
                                   remove_invalid: bool = False, force_recalculate=False) -> List[str]:
        """
        :return: True if all checksums match, False if at least one checksum mismatch was found
        """
        self.shell_command.set_remote_host(host)

        # read the expected checksum from the expected checksum file
        expected_checksums: Dict[str, Optional[str]] = {}
        for target_path in target_paths:
            try:
                expected_checksum = self.shell_command.target_read_checksum_from_file(
                    os.path.join(target_path, TARGET_STORAGE_SUBDIRECTORY, snapshot.dataset_zfs_path,
                                 snapshot.snapshot_name + BACKUP_FILE_POSTFIX + EXPECTED_CHECKSUM_FILE_POSTFIX))
            except CommandExecutionError:
                # missing expected checksum file is seen as incomplete backup, so we cannot use it for verification,
                # we wouldn't know what to expect
                # if everything works correctly, this should never happen.
                # verifiable/existing snapshots should only be found by the presence of the expected checksum file
                print("Expected checksum file missing for backup {}@{} on target {}".format(
                    snapshot.dataset_zfs_path, snapshot.snapshot_name, target_path))
                print("Verification not possible.")
                expected_checksum = None
            if self.dry_run and expected_checksum:
                expected_checksums[target_path] = "dry-run"
            else:
                expected_checksums[target_path] = expected_checksum

        # read the calculated checksum from the calculated checksum file
        calculated_checksums: Dict[str, Optional[str]] = {}
        if force_recalculate:
            for target_path in target_paths:
                calculated_checksums[target_path] = None
        else:
            for target_path in target_paths:
                if not expected_checksums[target_path]:
                    # if the expected checksum is missing, we cannot calculate the checksum
                    calculated_checksums[target_path] = None
                    continue
                try:
                    calculated_checksum = self.shell_command.target_read_checksum_from_file(
                        os.path.join(target_path, TARGET_STORAGE_SUBDIRECTORY, snapshot.dataset_zfs_path,
                                     snapshot.snapshot_name + BACKUP_FILE_POSTFIX + CALCULATED_CHECKSUM_FILE_POSTFIX))
                except CommandExecutionError:
                    calculated_checksum = None
                if self.dry_run and calculated_checksum:
                    calculated_checksums[target_path] = "dry-run"
                else:
                    calculated_checksums[target_path] = calculated_checksum

        # if the calculated checksum is missing, recalculate it
        if not all(calculated_checksums.values()):
            # map the full file_path on target to the short target path
            uncalculated_paths = {tp: os.path.join(tp, TARGET_STORAGE_SUBDIRECTORY, snapshot.dataset_zfs_path,
                                                   snapshot.snapshot_name + BACKUP_FILE_POSTFIX)
                                  for tp, cs in calculated_checksums.items()
                                  if not cs  # only recalculate if the checksum is missing
                                  and expected_checksums[tp]  # we need the expected checksum for verification
                                  }
            # for missing_expected_checksum_path in [tp for tp, cs in expected_checksums.items() if not cs]:

            if self.dry_run:
                re_calculated_checksums = {tp: "dry-run" for tp in uncalculated_paths.keys()}
            else:
                re_calculated_checksums = self.shell_command.target_get_checksums(uncalculated_paths)
                # write the calculated checksum to the calculated checksum file
                for target_path, calculated_checksum in re_calculated_checksums.items():
                    calculated_checksum_file_path = os.path.join(
                        target_path, TARGET_STORAGE_SUBDIRECTORY, snapshot.dataset_zfs_path,
                        snapshot.snapshot_name + BACKUP_FILE_POSTFIX + CALCULATED_CHECKSUM_FILE_POSTFIX)
                    try:
                        self.shell_command.target_write_to_file(
                            calculated_checksum_file_path,
                            "{} ./{}".format(calculated_checksum, snapshot.snapshot_name + BACKUP_FILE_POSTFIX))
                    except CommandExecutionError as e:
                        print("Error writing calculated checksum to file {}: {}".format(
                            calculated_checksum_file_path, e))
                        continue
            for target_path, calculated_checksum in re_calculated_checksums.items():
                if calculated_checksums[target_path]:
                    raise RuntimeError("Calculated checksum already exists for backup {}@{} on target {}".format(
                        snapshot.dataset_zfs_path, snapshot.snapshot_name, target_path))
                calculated_checksums[target_path] = calculated_checksum
                print("Re-Calculated checksum for backup {}@{} on target {}: {}".format(
                    snapshot.dataset_zfs_path, snapshot.snapshot_name, target_path, calculated_checksum))

        # verify the checksums
        mismatching_checksums = self._checksum_verify_helper(target_paths, snapshot, expected_checksums,
                                                             calculated_checksums)
        if mismatching_checksums and remove_invalid:
            for target_path in mismatching_checksums.keys():
                print("Removing invalid backup {}@{} on target {}".format(
                    snapshot.dataset_zfs_path, snapshot.snapshot_name, target_path))
                remove_files = [
                    os.path.join(target_path, TARGET_STORAGE_SUBDIRECTORY, snapshot.dataset_zfs_path,
                                 snapshot.snapshot_name + BACKUP_FILE_POSTFIX),
                    os.path.join(target_path, TARGET_STORAGE_SUBDIRECTORY, snapshot.dataset_zfs_path,
                                 snapshot.snapshot_name + BACKUP_FILE_POSTFIX + EXPECTED_CHECKSUM_FILE_POSTFIX),
                    os.path.join(target_path, TARGET_STORAGE_SUBDIRECTORY, snapshot.dataset_zfs_path,
                                 snapshot.snapshot_name + BACKUP_FILE_POSTFIX + CALCULATED_CHECKSUM_FILE_POSTFIX)]
                if self.dry_run:
                    print("Would have removed files: ", ', '.join(remove_files))
                else:
                    self.shell_command.target_remove_files(remove_files)

        return list(mismatching_checksums.keys())

    def _write_snapshot_to_target(self, snapshot: Snapshot, host: Optional[SshHost], target_paths: Set[str],
                                  repair=True):
        self.shell_command.set_remote_host(host)

        # ONLY auto-repair snapshots/files, which are missing the calculated checksum file
        # verification of already existing backups is done in the verification step

        # cases in which checksums are still getting calculated:
        # - the calculated checksum file is missing BUT the expected checksum file exists (needed for verification)
        # - a backup file was written

        # cases in which checksums are getting compared:
        # - we are in repair mode and the expected+calculated checksum file exists or was generated
        # - a backup file was written

        # repair the snapshot on the target pool
        if repair:
            missing_calculated_checksum: Dict[str, Optional[str]] = {}
            for target_path in list(target_paths):
                backup_file_path = os.path.join(
                    target_path, TARGET_STORAGE_SUBDIRECTORY, snapshot.dataset_zfs_path,
                    snapshot.snapshot_name + BACKUP_FILE_POSTFIX)
                expected_checksum_file_path = os.path.join(
                    target_path, TARGET_STORAGE_SUBDIRECTORY,
                    snapshot.dataset_zfs_path,
                    snapshot.snapshot_name + BACKUP_FILE_POSTFIX + EXPECTED_CHECKSUM_FILE_POSTFIX)
                calculated_checksum_file_path = os.path.join(
                    target_path, TARGET_STORAGE_SUBDIRECTORY,
                    snapshot.dataset_zfs_path,
                    snapshot.snapshot_name + BACKUP_FILE_POSTFIX + CALCULATED_CHECKSUM_FILE_POSTFIX)
                if self.shell_command.target_file_exists(backup_file_path):
                    try:
                        expected_checksum = self.shell_command.target_read_checksum_from_file(
                            expected_checksum_file_path)
                    except CommandExecutionError:
                        continue
                    try:
                        calculated_checksum = self.shell_command.target_read_checksum_from_file(
                            calculated_checksum_file_path)
                    except CommandExecutionError:
                        if self.dry_run:
                            missing_calculated_checksum[target_path] = "dry-run"
                        else:
                            missing_calculated_checksum[target_path] = expected_checksum
                        continue

                    if expected_checksum != calculated_checksum:
                        print("Checksum mismatch for backup {}@{} on target {}".format(
                            snapshot.dataset_zfs_path, snapshot.snapshot_name, target_path))
                        print("Expected checksum: {}".format(expected_checksum))
                        print("Calculated checksum: {}".format(calculated_checksum))
                        print("Scheduled for repair.")
                    else:
                        # checksums match, we can skip the repair and also the rewrite for this target path.
                        target_paths.remove(target_path)

            if missing_calculated_checksum:
                print("Calculating missing checksums...")
                backup_files = {tp: os.path.join(tp, TARGET_STORAGE_SUBDIRECTORY, snapshot.dataset_zfs_path,
                                                 snapshot.snapshot_name + BACKUP_FILE_POSTFIX)
                                for tp in missing_calculated_checksum.keys()}
                if self.dry_run:
                    checksums = {tp: "dry-run" for tp in backup_files.keys()}
                else:
                    checksums = self.shell_command.target_get_checksums(backup_files)
                    for target_path, checksum in checksums.items():
                        calculated_checksum_file_path = os.path.join(
                            target_path, TARGET_STORAGE_SUBDIRECTORY, snapshot.dataset_zfs_path,
                            snapshot.snapshot_name + BACKUP_FILE_POSTFIX + CALCULATED_CHECKSUM_FILE_POSTFIX)
                        self.shell_command.target_write_to_file(
                            calculated_checksum_file_path,
                            "{} ./{}".format(checksum, snapshot.snapshot_name + BACKUP_FILE_POSTFIX))
                invalid_checksums = self._checksum_verify_helper(list(missing_calculated_checksum.keys()), snapshot,
                                                                 missing_calculated_checksum,
                                                                 cast(Dict[str, Optional[str]], checksums))
                if invalid_checksums:
                    for target_path, (_expected_checksum, _calculated_checksum) in invalid_checksums.items():
                        print("Checksum mismatch for backup {}@{} on target {}".format(
                            snapshot.dataset_zfs_path, snapshot.snapshot_name, target_path))
                        print("Expected checksum: {}".format(_expected_checksum))
                        print("Calculated checksum: {}".format(_calculated_checksum))
                        print("Scheduled for repair.")

                # filter out the target paths, which have a valid checksum
                for target_path in missing_calculated_checksum.keys():
                    if target_path not in invalid_checksums:
                        target_paths.remove(target_path)

        # write the snapshot to all remaining target paths
        # some target paths may have been removed in the repair step
        for target_path in target_paths:
            if not self.dry_run:
                self.shell_command.target_mkdir(
                    os.path.join(target_path, TARGET_STORAGE_SUBDIRECTORY, snapshot.dataset_zfs_path))

        if self.dry_run:
            print("Would have sent backup snapshot {}@{} to target(s): {}...".format(
                snapshot.dataset_zfs_path, snapshot.snapshot_name, ", ".join(sorted(target_paths)))
            )
            expected_checksum = "dry-run"
        else:
            if snapshot.has_incremental_base():
                previous_snapshot = snapshot.get_incremental_base().snapshot_name
            else:
                previous_snapshot = None
            expected_checksum = self.shell_command.zfs_send_snapshot_to_target(
                source_dataset=snapshot.dataset_zfs_path,
                previous_snapshot=previous_snapshot,
                next_snapshot=snapshot.snapshot_name,
                target_paths=target_paths,
                include_intermediate_snapshots=self.include_intermediate_snapshots)

            # write the expected checksum to the expected checksum file
            for target_path in target_paths:
                expected_checksum_file_path = os.path.join(
                    target_path, TARGET_STORAGE_SUBDIRECTORY, snapshot.dataset_zfs_path,
                    snapshot.snapshot_name + BACKUP_FILE_POSTFIX + EXPECTED_CHECKSUM_FILE_POSTFIX)

                self.shell_command.target_write_to_file(expected_checksum_file_path,
                                                        "{} ./{}".format(expected_checksum,
                                                                         snapshot.snapshot_name + BACKUP_FILE_POSTFIX))

        print("Created backup snapshot {}@{} with checksum {}".format(
            snapshot.dataset_zfs_path, snapshot.snapshot_name, expected_checksum))

        # verify the written backups
        print("Verifying written backups...")
        if self.dry_run:
            read_checksums: Mapping[str, Optional[str]] = {tp: "dry-run" for tp in target_paths}
        else:
            backup_files = {tp: os.path.join(tp, TARGET_STORAGE_SUBDIRECTORY, snapshot.dataset_zfs_path,
                                             snapshot.snapshot_name + BACKUP_FILE_POSTFIX)
                            for tp in target_paths}
            read_checksums = self.shell_command.target_get_checksums(backup_files)
            for target_path, checksum in read_checksums.items():
                calculated_checksum_file_path = os.path.join(
                    target_path, TARGET_STORAGE_SUBDIRECTORY, snapshot.dataset_zfs_path,
                    snapshot.snapshot_name + BACKUP_FILE_POSTFIX + CALCULATED_CHECKSUM_FILE_POSTFIX)
                self.shell_command.target_write_to_file(
                    calculated_checksum_file_path,
                    "{} ./{}".format(checksum, snapshot.snapshot_name + BACKUP_FILE_POSTFIX))

        invalid_checksums = self._checksum_verify_helper(list(target_paths), snapshot,
                                                         {tp: expected_checksum for tp in target_paths},
                                                         dict(read_checksums))

        if invalid_checksums:
            print("Aborting...")
            sys.exit(1)

    def _restore_snapshot_from_target(self, host_target_paths: List[Tuple[Optional[SshHost], str]],
                                      snapshot: Snapshot,
                                      restore_zfs_path: str,
                                      replace_parent_move_children: bool = False,
                                      wipe_replacement: bool = False):
        for i, (host, target_path) in enumerate(sorted(host_target_paths, key=lambda x: x[1])):
            self.shell_command.set_remote_host(host)

            print("Restoring backup snapshot {} from target {}...".format(
                snapshot.zfs_path, target_path))
            if snapshot.snapshot_name.endswith(SNAPSHOT_PREFIX_POSTFIX_SEPARATOR + INITIAL_SNAPSHOT_POSTFIX):
                if self.shell_command.has_dataset(restore_zfs_path):
                    if not replace_parent_move_children:
                        print("Dataset {} already has snapshots.".format(restore_zfs_path))
                        print("Cannot restore initial snapshots, if the dataset already has snapshots.")
                        print("Wiping of all present snapshots of the target dataset is needed to restore initial "
                              "backup snapshot.")
                        print("Aborting...")
                        sys.exit(1)
            if self.dry_run:
                print("Would have restored backup snapshot {} from target {}".format(
                    snapshot.zfs_path, target_path))
            else:
                try:
                    self.shell_command.zfs_recv_snapshot_from_target(target_path, snapshot.zfs_path, restore_zfs_path,
                                                                     replace_parent_move_children, wipe_replacement)
                except CommandExecutionError as e:
                    print(
                        "Error restoring {} under {} from {}: {}".format(snapshot.zfs_path, restore_zfs_path,
                                                                         target_path, e))
                    if i + 1 < len(host_target_paths):
                        print("Trying next target...")
                        continue
                    else:
                        print("all targets failed")
                        print("Aborting...")
                        sys.exit(1)
                except ZfsCommandsError as e:
                    print("Error: cannot restore snapshot {}: {}".format(snapshot.zfs_path, str(e)))
                    print("Aborting...")
                    sys.exit(1)

            print("Restored backup snapshot {} from target {}".format(
                snapshot.zfs_path, target_path))
            break

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

        # make combinations of target paths, that share the same pools
        # we also have to check for pools that only exist in one target path

        pool_target_paths: Dict[Tuple[str], PoolList] = {}
        fill_list = [None, ] * (len(target_paths_pools) - 1)
        for _compare_base_paths in combinations(fill_list + list(target_paths_pools.keys()), len(target_paths_pools)):
            # filter out the None values
            compare_base_paths: Tuple[str] = cast(Tuple[str], tuple(filter(None, _compare_base_paths)))

            if len(compare_base_paths) == 1:
                # we have only one target path, so we have to 'cut off' the elements that are not shared
                # this is done by calculating the difference of the pools of the compare_base_path and the pools of
                # the other target paths
                # the compare_base_path's pools are used as a base, so we can find the unique elements in the pools
                unique_pools = target_paths_pools[compare_base_paths[0]]
                for other_path, other_pools in target_paths_pools.items():
                    if other_path in compare_base_paths:
                        continue
                    unique_pools = unique_pools.difference(other_pools)

                pool_target_paths[compare_base_paths] = unique_pools
                continue
            else:
                # intersect all pools of the compare_base_paths - all elements that are shared between the targets pools
                path_intersections = PoolList.intersection(target_paths_pools[compare_base_paths[0]],
                                                           *(target_paths_pools[compare_base_path]
                                                             for compare_base_path in compare_base_paths[1:]))
                pool_target_paths[compare_base_paths] = path_intersections

        # filter out empty pools with no snapshots
        for target_path, pools in list(pool_target_paths.items()):
            if not pools.has_snapshots():
                pool_target_paths.pop(target_path)
        return pool_target_paths

    def repair_snapshots(self, repair_pools: Dict[Tuple[Optional[SshHost], str], PoolList]):
        """
        Repairs all snapshots in the given pools on the given (remote) targets.
        Snapshots that need to be transferred to multiple targets get grouped together, to reduce transmitted data.
        """
        # group target paths by host, used for the parallel writing of backups (tee command parameters)
        host_targetpaths_pools = self._group_target_paths_by_host(repair_pools)

        # iterate over all hosts, group pools from different target paths together, to repair them in one go
        for host, targetpaths_pools in host_targetpaths_pools.items():
            # combine pools with equal target paths
            pool_target_paths = self._group_target_paths(targetpaths_pools)

            for target_paths, pools in pool_target_paths.items():
                print("writing snapshots to target paths:", ", ".join(target_paths))
                pools.print(with_incremental_base=self.debug)
                for snapshot in pools.iter_snapshots():
                    print("Repairing snapshot: ", snapshot.zfs_path)
                    self._write_snapshot_to_target(snapshot, host, set(target_paths))

            print()

    def restore_snapshots(self, restore_snapshots: List[Tuple[Snapshot, List[Tuple[Optional[SshHost], str]]]],
                          replace_dataset: Optional[List[DataSet]] = None,
                          conflicting_snapshots: Optional[List[DataSet]] = None,
                          restore_target: Optional[str] = None, inplace: bool = False, wipe_replacement: bool = False):
        if not inplace and restore_target is None:
            raise ValueError("Restore target must be specified if not restoring inplace.")
        if restore_target:
            assert not restore_target.startswith('/')

        # we cannot recreate a pool, because this would need info about the disks and other pool related setup values
        # we can only restore datasets and snapshots
        # check if the restore_target is a dataset or a pool
        if not inplace and restore_target:
            restore_pool = restore_target.split('/', 1)[0]
            if restore_pool not in self.shell_command.list_pools():
                print("Restore targets pool '{}' does not exist.".format(restore_pool))
                print("Cannot restore a whole pool, only datasets.")
                print("Create a pool with the name '{}' first.".format(restore_pool))
                print("Aborting...")
                sys.exit(1)

        replace_dataset_names = {dataset.zfs_path for dataset in replace_dataset} if replace_dataset else set()
        conflicting_dataset_names = {dataset.zfs_path: dataset for dataset in
                                     conflicting_snapshots} if conflicting_snapshots else dict()

        for snapshot, sources in restore_snapshots:
            if inplace:
                _restore_target = snapshot.dataset_zfs_path
                shifted_snapshot = snapshot.view()
            else:
                assert restore_target
                _restore_target = os.path.join(restore_target, snapshot.dataset_zfs_path)
                shifted_snapshot = snapshot.prefixed_view(restore_target)
            if shifted_snapshot.dataset_zfs_path in conflicting_dataset_names:
                # remove all conflicting snapshots on first dataset conflict
                dataset = conflicting_dataset_names.pop(shifted_snapshot.dataset_zfs_path)
                for conflicting_snapshot in dataset.iter_snapshots():
                    print("Removing snapshot: ", conflicting_snapshot.zfs_path)
                    if self.dry_run:
                        print("Would have removed snapshot: ", conflicting_snapshot.zfs_path)
                    else:
                        self.shell_command.delete_snapshot(conflicting_snapshot.zfs_path)

            if shifted_snapshot.dataset_zfs_path in replace_dataset_names and snapshot.snapshot_name.endswith(
                    SNAPSHOT_PREFIX_POSTFIX_SEPARATOR + INITIAL_SNAPSHOT_POSTFIX):
                if wipe_replacement:
                    print("Replacing and wiping dataset '{}' after restore of snapshot '{}'".format(
                        snapshot.dataset_zfs_path, snapshot.zfs_path))
                else:
                    print("Replacing dataset '{}' after restore of snapshot '{}'".format(
                        snapshot.dataset_zfs_path, snapshot.zfs_path))
                self._restore_snapshot_from_target(sources, snapshot, _restore_target,
                                                   replace_parent_move_children=True,
                                                   wipe_replacement=wipe_replacement)
            else:
                print("Restoring snapshot '{}' into '{}'".format(snapshot.zfs_path, _restore_target))
                self._restore_snapshot_from_target(sources, snapshot, _restore_target,
                                                   replace_parent_move_children=False,
                                                   wipe_replacement=False)

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
                print("writing snapshots to target paths:", ", ".join(target_paths))
                pools.print(with_incremental_base=self.debug)
                for snapshot in pools.iter_snapshots():
                    print("writing backup snapshot:", snapshot.zfs_path)
                    self._write_snapshot_to_target(snapshot, host, set(target_paths))

    def clean_snapshots(self, local_pools: PoolList):
        # iterate over all snapshots and remove them from the target pool
        for snapshot in local_pools.iter_snapshots():
            print("Removing snapshot: ", snapshot.zfs_path)
            if self.dry_run:
                print("Would have removed snapshot: ", snapshot.zfs_path)
            else:
                self.shell_command.delete_snapshot(snapshot.zfs_path)

    def clean_remote_snapshots(self, configured_remote_pools: Dict[Tuple[Optional[SshHost], str], PoolList],
                               zfs_path_filter: Optional[str] = None):
        for (host, target_path), pools in configured_remote_pools.items():
            self.shell_command.set_remote_host(host)
            for snapshot in pools.iter_snapshots():
                if zfs_path_filter and not snapshot.zfs_path.startswith(zfs_path_filter):
                    continue
                print("Removing remote snapshot: ", snapshot.zfs_path)
                snapshot_files = []
                for filepostfix in (BACKUP_FILE_POSTFIX,
                                    BACKUP_FILE_POSTFIX + EXPECTED_CHECKSUM_FILE_POSTFIX,
                                    BACKUP_FILE_POSTFIX + CALCULATED_CHECKSUM_FILE_POSTFIX):
                    snapshot_files.append(os.path.join(target_path, TARGET_STORAGE_SUBDIRECTORY,
                                                       snapshot.dataset_zfs_path,
                                                       snapshot.snapshot_name + filepostfix))
                if self.dry_run:
                    print("Would have removed files: ", snapshot_files)
                else:
                    self.shell_command.target_remove_files(snapshot_files)

    def wipe_dataset(self, dataset: DataSet):
        if self.dry_run:
            print("Would have deleted dataset: ", dataset.zfs_path)
        else:
            print("Deleting dataset: ", dataset.zfs_path)
            self.shell_command.delete_dataset(dataset.zfs_path, with_snapshots=True)
