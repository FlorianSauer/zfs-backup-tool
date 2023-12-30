from typing import List, Optional, Tuple, Iterable

from ZfsBackupTool.CliInterface import CliInterface
from ZfsBackupTool.Constants import SNAPSHOT_PREFIX_POSTFIX_SEPARATOR, INITIAL_SNAPSHOT_POSTFIX
from ZfsBackupTool.ShellCommand import ShellCommand


class DataSet(CliInterface):

    def __init__(self, shell_command: ShellCommand, zfs_path: str):
        super().__init__(shell_command)
        self.zfs_path = zfs_path
        self._snapshots: Optional[List[str]] = None

    def __hash__(self):
        return hash(self.zfs_path)

    def invalidate_caches(self):
        self._snapshots = None

    @classmethod
    def get_recursive(cls, shell_command: ShellCommand, zfs_path: str) -> List['DataSet']:
        selected_source_datasets = shell_command.get_datasets(zfs_path, recursive=True)
        return [cls(shell_command, dataset) for dataset in selected_source_datasets]

    def get_snapshots(self, refresh: bool = False) -> List[str]:
        if refresh:
            self._snapshots = None
        if self._snapshots is None:
            self._snapshots = self.shell_command.get_snapshots(self.zfs_path)
        return self._snapshots

    @classmethod
    def filter_backup_snapshots(cls, snapshots: List[str], snapshot_prefix: str) -> List[str]:
        matching_snapshots = [snapshot for snapshot in snapshots
                              if snapshot.startswith(snapshot_prefix)]
        return matching_snapshots

    def get_backup_snapshots(self, snapshot_prefix: str) -> List[str]:
        matching_snapshots = self.filter_backup_snapshots(self.get_snapshots(), snapshot_prefix)
        return self.sort_backup_snapshots(matching_snapshots)

    @classmethod
    def sort_backup_snapshots(cls, snapshots: Iterable[str]) -> List[str]:
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
