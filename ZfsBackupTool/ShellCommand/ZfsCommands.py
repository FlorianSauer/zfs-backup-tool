import os
import shlex
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Set, Tuple

from .Base import BaseShellCommand, CommandExecutionError
from ..Constants import (TARGET_STORAGE_SUBDIRECTORY, BACKUP_FILE_POSTFIX, TARGET_DATASET_REPLACEMENT_POSTFIX,
                         REPLACED_ORIGINAL_DATASET_POSTFIX)


class ZfsCommandsError(Exception):
    pass


class ZfsCommands(BaseShellCommand):

    def __init__(self, echo_cmd=False):
        super().__init__(echo_cmd)

    def list_pools(self) -> List[str]:
        command = "zpool list -H -o name"
        sub_process = self._execute(command, capture_output=True)
        stdout_lines = sub_process.stdout.read().decode('utf-8').splitlines() if sub_process.stdout else []
        return [line.strip() for line in stdout_lines]

    def list_datasets(self, zfs_parts_prefix: str) -> List[str]:
        """
        List all datasets under the given zfs_parts_prefix
        The zfs_parts_prefix must be a full valid zfs path to a pool or dataset.
        The returned datasets are relative to the zfs_parts_prefix and can be joined with the given zfs_parts_prefix
        to get full zfs paths.
        """
        command = "zfs list -H -r -o name"
        command += ' "{}"'.format(zfs_parts_prefix)
        sub_process = self._execute(command, capture_output=True)
        stdout_lines = sub_process.stdout.read().decode('utf-8').splitlines() if sub_process.stdout else []
        datasets = [line.strip().replace(zfs_parts_prefix + '/', '', 1) for line in stdout_lines if
                    line.strip() != zfs_parts_prefix]
        return datasets

    def list_snapshots(self, dataset: str) -> List[str]:
        command = "zfs list -H -o name -t snapshot"
        command += ' "{}"'.format(dataset)
        sub_process = self._execute(command, capture_output=True)
        stdout_lines = sub_process.stdout.read().decode('utf-8').splitlines() if sub_process.stdout else []
        return [line.strip().replace(dataset + '@', '')
                for line in stdout_lines]

    def list_snapshots_with_creation_time(self, dataset: str) -> List[Tuple[str, datetime]]:
        command = "zfs list -H -p -o name,creation -t snapshot"
        command += ' "{}"'.format(dataset)
        sub_process = self._execute(command, capture_output=True)
        stdout_lines = sub_process.stdout.read().decode('utf-8').splitlines() if sub_process.stdout else []
        return [(line.strip().split()[0].replace(dataset + '@', ''),
                 datetime.fromtimestamp(int(line.strip().split()[1])))
                for line in stdout_lines]

    def has_dataset(self, dataset: str) -> bool:
        command = "zfs list -H -o name"
        command += ' | grep -q -e "^{}$"'.format(dataset)
        try:
            sub_process = self._execute(command, capture_output=True)
        except CommandExecutionError as e:
            exit_code = e.sub_process.returncode
        else:
            exit_code = sub_process.returncode
        return exit_code == 0

    def has_snapshot(self, dataset: str, snapshot: str) -> bool:
        command = "zfs list -H -o name -t snapshot"
        command += ' | grep -q -e "^{}@{}$"'.format(dataset, snapshot)
        try:
            sub_process = self._execute(command, capture_output=True)
        except CommandExecutionError as e:
            exit_code = e.sub_process.returncode
        else:
            exit_code = sub_process.returncode
        return exit_code == 0

    def get_dataset_size(self, dataset: str, recursive: bool) -> int:
        command = 'zfs list -p -H -o refer'
        if recursive:
            command += " -r"
        command += ' "{}"'.format(dataset)
        sub_process = self._execute(command, capture_output=True)
        stdout_lines = sub_process.stdout.read().decode('utf-8').splitlines() if sub_process.stdout else []
        return int(stdout_lines[0].strip())

    def get_estimated_snapshot_size(self, source_dataset: str, previous_snapshot: Optional[str], next_snapshot: str,
                                    include_intermediate_snapshots: bool = False):
        if previous_snapshot:
            sub_process = self._execute('zfs send -n -P --raw {} "{}@{}" "{}@{}"'.format(
                "-I" if include_intermediate_snapshots else '-i',
                source_dataset, previous_snapshot, source_dataset, next_snapshot), capture_output=True
            )
        else:
            sub_process = self._execute('zfs send -n -P --raw "{}@{}"'.format(
                source_dataset, next_snapshot), capture_output=True
            )
        stdout_lines = sub_process.stdout.read().decode('utf-8').splitlines() if sub_process.stdout else []

        for line in stdout_lines:
            if line.lower().startswith("size"):
                return int(line.replace("size", "").strip())
        raise ValueError("Could not determine snapshot size")

    def create_snapshot(self, source_dataset: str, next_snapshot: str):
        command = 'zfs snapshot "{}@{}"'.format(source_dataset, next_snapshot)
        return self._execute(command, capture_output=False)

    def create_dataset(self, source_dataset: str):
        command = 'zfs create "{}"'.format(source_dataset)
        return self._execute(command, capture_output=False)

    def delete_dataset(self, dataset_zfs_path: str, with_snapshots: bool = False):
        if with_snapshots:
            command = 'zfs destroy -r "{}"'.format(dataset_zfs_path)
        else:
            command = 'zfs destroy "{}"'.format(dataset_zfs_path)
        return self._execute(command, capture_output=False)

    def delete_snapshot(self, snapshot_zfs_path: str):
        command = 'zfs destroy "{}"'.format(snapshot_zfs_path)
        return self._execute(command, capture_output=False)

    def zfs_send_snapshot_to_target(self, source_dataset: str,
                                    previous_snapshot: Optional[str], next_snapshot: str,
                                    target_paths: Set[str],
                                    include_intermediate_snapshots: bool = False) -> str:

        estimated_size = self.get_estimated_snapshot_size(source_dataset, previous_snapshot, next_snapshot,
                                                          include_intermediate_snapshots)

        # with temporary file
        with tempfile.NamedTemporaryFile() as tmp:
            if previous_snapshot:
                command = 'set -o pipefail; zfs send --raw {} "{}@{}" "{}@{}" '.format(
                    "-I" if include_intermediate_snapshots else '-i',
                    source_dataset, previous_snapshot, source_dataset, next_snapshot)
            else:
                command = 'set -o pipefail; zfs send --raw "{}@{}"'.format(source_dataset, next_snapshot)

            command += ' | tee >( sha256sum -b > "{}" )'.format(tmp.name)
            command += ' | pv {} --size {}'.format(self._PV_DEFAULT_OPTIONS, estimated_size)

            if self.remote:
                command += ' | ' + self._get_ssh_command(self.remote)
                tee_quoted_paths = ' '.join('"{}"'.format(
                    os.path.join(path, TARGET_STORAGE_SUBDIRECTORY, source_dataset,
                                 next_snapshot + BACKUP_FILE_POSTFIX))
                                            for path in sorted(target_paths))
                command += shlex.quote('tee {} > /dev/null'.format(tee_quoted_paths))
            else:
                tee_quoted_paths = ' '.join('"{}"'.format(
                    os.path.join(path, TARGET_STORAGE_SUBDIRECTORY, source_dataset,
                                 next_snapshot + BACKUP_FILE_POSTFIX))
                                            for path in sorted(target_paths))
                command += ' | tee {} > /dev/null'.format(tee_quoted_paths)

            sys.stdout.flush()
            sys.stderr.flush()
            self._execute(command, capture_output=False)
            sys.stdout.flush()
            sys.stderr.flush()
            return tmp.read().decode('utf-8').strip().split(' ')[0]

    def zfs_recv_snapshot_from_target(self, restore_source_dirpath: str,
                                      restore_source_zfs_path: str,
                                      restore_target_zfs_path: str,
                                      replace_parent_move_children=False,
                                      wipe_replacement=False) -> None:
        # we iter the second to second last part of the restore_zfs_path to create the datasets, excluding
        # the pool and the target dataset are skipped
        # pool creation is a too big hassle, maybe do this at a later point
        # target dataset MUST be excluded and only created by zfs recv
        # restore_target_zfs_path must therefore be a full dataset zfs path (not a snapshot)
        # contianing one slash (pool to dataset separator) and no @ (dataset to snapshot separator)
        # the last restore_target_zfs_path part is created by zfs recv.
        # otherwise, when an encrypted dataset is received, the unencrypted dataset would be overwritten by an
        # encrypted dataset, which is forbidden by zfs
        if '/' not in restore_target_zfs_path:
            raise ValueError("restore_target_zfs_path must contain a pool and a dataset")
        if '@' in restore_target_zfs_path:
            raise ValueError("restore_target_zfs_path must not contain a snapshot")
        # restore_zfs_path must be a full snapshot zfs path (at least one / and one @)
        # it's used for the source directory concatenation with restore_source_dirpath, which is the directory
        # containing the backup files (all under TARGET_STORAGE_SUBDIRECTORY)
        if '/' not in restore_source_zfs_path:
            raise ValueError("restore_source_zfs_path must contain a pool and a dataset")
        if '@' not in restore_source_zfs_path:
            raise ValueError("restore_source_zfs_path must contain a snapshot")
        if replace_parent_move_children and self.has_dataset(
                restore_target_zfs_path + TARGET_DATASET_REPLACEMENT_POSTFIX):
            if wipe_replacement:
                self.delete_dataset(restore_target_zfs_path + TARGET_DATASET_REPLACEMENT_POSTFIX, with_snapshots=True)
            else:
                raise ZfsCommandsError("The temporary replacement dataset {} already exists.".format(
                    restore_target_zfs_path + TARGET_DATASET_REPLACEMENT_POSTFIX))
        if replace_parent_move_children and self.has_dataset(
                restore_target_zfs_path + REPLACED_ORIGINAL_DATASET_POSTFIX):
            if wipe_replacement:
                self.delete_dataset(restore_target_zfs_path + REPLACED_ORIGINAL_DATASET_POSTFIX, with_snapshots=True)
            else:
                raise ZfsCommandsError("The replacement dataset {} already exists.".format(
                    restore_target_zfs_path + REPLACED_ORIGINAL_DATASET_POSTFIX))

        # region recreate the parent datasets of the restore_target_zfs_path
        restore_target_poolname = Path(restore_target_zfs_path).parts[0]
        # exclude pool and target dataset
        restore_target_needed_dataset_parts = Path(restore_target_zfs_path).parts[1:-1]

        re_joined_zfs_path_parts = restore_target_poolname
        for restore_dataset_part in restore_target_needed_dataset_parts:
            re_joined_zfs_path_parts = os.path.join(re_joined_zfs_path_parts, restore_dataset_part)
            if not self.has_dataset(re_joined_zfs_path_parts):
                self.create_dataset(re_joined_zfs_path_parts)

        # except of the pool and the last dataset segment, all datasets are created.
        # the last dataset segment is created by zfs recv
        # endregion

        # region children renaming

        if replace_parent_move_children:
            effective_restore_target_zfs_path = restore_target_zfs_path + TARGET_DATASET_REPLACEMENT_POSTFIX
        else:
            effective_restore_target_zfs_path = restore_target_zfs_path

        # endregion

        # region read the backup file and forward it to zfs recv
        restore_pool_dataset_zfs_path, restore_snapshot = restore_source_zfs_path.split('@', 1)
        restore_file_path = os.path.join(restore_source_dirpath, TARGET_STORAGE_SUBDIRECTORY,
                                         restore_pool_dataset_zfs_path,
                                         restore_snapshot + BACKUP_FILE_POSTFIX)

        if self.remote:
            command = self._get_ssh_command(self.remote)
            command += shlex.quote(
                'pv {} "{}"'.format(self._PV_DEFAULT_OPTIONS, restore_file_path))
        else:
            command = 'pv {} "{}"'.format(self._PV_DEFAULT_OPTIONS, restore_file_path)
        command += ' | zfs recv -F "{}"'.format(effective_restore_target_zfs_path)

        try:
            self._execute(command, capture_output=False)
        except Exception:
            if replace_parent_move_children:
                # cleanup
                self.delete_dataset(effective_restore_target_zfs_path, with_snapshots=True)
            raise
        if replace_parent_move_children:
            # move any children of an existing
            if self.has_dataset(restore_target_zfs_path):
                # list_datasets will include all children, even sub-children
                # we have to filter out the sub-children, which contain an additional slash
                children = [restore_target_zfs_path + '/' + dataset
                            for dataset in self.list_datasets(restore_target_zfs_path)
                            if '/' not in dataset]

                print("Moving children: ", children)

                for child in children:
                    self._execute('zfs rename "{}" "{}"'.format(child,
                                                                child.replace(restore_target_zfs_path,
                                                                              effective_restore_target_zfs_path, 1)),
                                  capture_output=False)
                self._execute('zfs rename "{}" "{}"'.format(restore_target_zfs_path,
                                                            restore_target_zfs_path
                                                            + REPLACED_ORIGINAL_DATASET_POSTFIX),
                              capture_output=False)
                self._execute('zfs rename "{}" "{}"'.format(effective_restore_target_zfs_path,
                                                            restore_target_zfs_path),
                              capture_output=False)
                if wipe_replacement:
                    self.delete_dataset(restore_target_zfs_path + REPLACED_ORIGINAL_DATASET_POSTFIX,
                                        with_snapshots=True)
            else:
                print("No children to move")
        # endregion
