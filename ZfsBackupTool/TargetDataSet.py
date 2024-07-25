import os.path
from typing import List, Tuple, Optional, Set

from ZfsBackupTool.CliInterface import CliInterface
from ZfsBackupTool.Constants import SNAPSHOT_PREFIX_POSTFIX_SEPARATOR, INITIAL_SNAPSHOT_POSTFIX, BACKUP_FILE_POSTFIX, \
    TARGET_STORAGE_SUBDIRECTORY
from ZfsBackupTool.DataSet import DataSet
from ZfsBackupTool.ShellCommand import ShellCommand


class TargetDataSet(CliInterface):

    def __init__(self, shell_command: ShellCommand, zfs_path: str, target_paths: List[str]):
        super().__init__(shell_command)
        self.zfs_path = zfs_path
        self.target_paths = set(target_paths)
        self._snapshots: Optional[List[str]] = None

    def add_target_path(self, target_path: str) -> None:
        self.target_paths.add(target_path)

    def get_all_target_paths(self, target_filter: Optional[str] = None) -> Set[str]:
        paths = []
        for target_path in self.target_paths:
            if target_filter and not target_path.startswith(target_filter):
                continue
            paths.append(target_path)
        return set(paths)

    def add_snapshot(self, snapshot: str) -> None:
        if self._snapshots is None:
            self._snapshots = []
        if snapshot in self._snapshots:
            raise ValueError("Snapshot already added!")
        self._snapshots.append(snapshot)

    @classmethod
    def get_recursive(cls, shell_command: ShellCommand, zfs_path: str) -> List['DataSet']:
        raise NotImplementedError
        selected_source_datasets = shell_command.get_datasets(zfs_path, recursive=True)
        return [cls(shell_command, dataset) for dataset in selected_source_datasets]

    def get_snapshots(self, refresh: bool = False) -> List[str]:
        if refresh:
            self._snapshots = None
        if self._snapshots is None:
            for target_path in self.target_paths:
                if not self.shell_command.target_dir_exists(
                        os.path.join(target_path, TARGET_STORAGE_SUBDIRECTORY, self.zfs_path)):
                    self._snapshots = []
                else:
                    files, directories = self.shell_command.target_list_directory(
                        os.path.join(target_path, TARGET_STORAGE_SUBDIRECTORY, self.zfs_path))
                    self._snapshots = [f.replace(BACKUP_FILE_POSTFIX, '')
                                       for f in files if f.endswith(BACKUP_FILE_POSTFIX)]
        return self._snapshots  # type: ignore

    @classmethod
    def filter_backup_snapshots(cls, snapshots: List[str], snapshot_prefix: str) -> List[str]:
        matching_snapshots = [snapshot for snapshot in snapshots
                              if snapshot.startswith(snapshot_prefix)]
        return matching_snapshots

    def get_backup_snapshots(self, snapshot_prefix: str) -> List[str]:
        matching_snapshots = self.filter_backup_snapshots(self.get_snapshots(), snapshot_prefix)
        return self._sort_backup_snapshots(matching_snapshots)

    def get_expected_backup_snapshots(self, snapshot_prefix: str) -> List[str]:
        highest_snapshot_number = self._get_highest_snapshot_number(snapshot_prefix)
        expected_snapshots = []
        for snapshot_number in range(0, highest_snapshot_number + 1):
            if snapshot_number == 0:
                expected_snapshot = (snapshot_prefix
                                     + SNAPSHOT_PREFIX_POSTFIX_SEPARATOR
                                     + INITIAL_SNAPSHOT_POSTFIX)
            else:
                expected_snapshot = (snapshot_prefix
                                     + SNAPSHOT_PREFIX_POSTFIX_SEPARATOR
                                     + str(snapshot_number))
            expected_snapshots.append(expected_snapshot)
        return expected_snapshots

    def _sort_backup_snapshots(self, snapshots: List[str]) -> List[str]:
        ordered_snapshots: List[str] = []
        for snapshot in sorted(snapshots):
            if snapshot.endswith(INITIAL_SNAPSHOT_POSTFIX):
                ordered_snapshots.insert(0, snapshot)
            else:
                ordered_snapshots.append(snapshot)
        return ordered_snapshots

    def create_snapshot(self, next_snapshot: str) -> None:
        self.shell_command.create_snapshot(self.zfs_path, next_snapshot)

    def delete_snapshot(self, snapshot: str) -> None:
        self.shell_command.delete_snapshot(self.zfs_path, snapshot)

    def has_initial_backup_snapshot(self, snapshot_prefix: str) -> bool:
        for snapshot in self.get_snapshots():
            if snapshot.startswith(snapshot_prefix) and snapshot.endswith(
                    SNAPSHOT_PREFIX_POSTFIX_SEPARATOR + INITIAL_SNAPSHOT_POSTFIX):
                return True
        return False

    def has_intermediate_backup_snapshots(self, snapshot_prefix: str) -> bool:
        return self._get_highest_snapshot_number(snapshot_prefix) > 0

    def _get_highest_snapshot_number(self, snapshot_prefix: str) -> int:
        highest_snapshot_number = 0  # next snapshot after initial snapshot is always 1
        for snapshot in self.get_snapshots():
            if snapshot.startswith(snapshot_prefix):
                if snapshot.endswith(SNAPSHOT_PREFIX_POSTFIX_SEPARATOR + INITIAL_SNAPSHOT_POSTFIX):
                    continue
                snapshot_number = int(snapshot.split(SNAPSHOT_PREFIX_POSTFIX_SEPARATOR)[-1])
                if snapshot_number > highest_snapshot_number:
                    highest_snapshot_number = snapshot_number
        return highest_snapshot_number

    def get_next_snapshot_name(self, snapshot_prefix: str) -> Tuple[str, str]:
        highest_snapshot_number = self._get_highest_snapshot_number(snapshot_prefix)
        # next snapshot after initial snapshot is always 1, 1 gets added later
        if highest_snapshot_number == 0:
            previous_snapshot = (snapshot_prefix
                                 + SNAPSHOT_PREFIX_POSTFIX_SEPARATOR
                                 + INITIAL_SNAPSHOT_POSTFIX)
        else:
            previous_snapshot = (snapshot_prefix
                                 + SNAPSHOT_PREFIX_POSTFIX_SEPARATOR
                                 + str(highest_snapshot_number))
        next_snapshot = (snapshot_prefix
                         + SNAPSHOT_PREFIX_POSTFIX_SEPARATOR
                         + str(highest_snapshot_number + 1))
        return previous_snapshot, next_snapshot
