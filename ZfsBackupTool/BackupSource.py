from typing import List, Optional, Set

from ZfsBackupTool.CliInterface import CliInterface
from ZfsBackupTool.DataSet import DataSet
from ZfsBackupTool.ShellCommand import ShellCommand
from ZfsBackupTool.TargetGroup import TargetGroup


class BackupSource(CliInterface):
    def __init__(self, shell_command: ShellCommand,
                 name: str, zfs_source: List[str], targets: List[TargetGroup],
                 recursive: bool = False,
                 exclude: List[str] = None,
                 include: List[str] = None):
        super().__init__(shell_command)
        self.name = name
        self.zfs_source = zfs_source
        self.targets = targets
        self.recursive = recursive
        self.exclude = exclude
        self.include = include
        self._datasets: Optional[List[DataSet]] = None

    def invalidate_caches(self) -> None:
        self._datasets = None

    def get_all_target_paths(self, target_filter: Optional[str] = None) -> Set[str]:
        paths = []
        for target in self.targets:
            for target_path in target.paths:
                if target_filter and not target_path.startswith(target_filter):
                    continue
                paths.append(target_path)
        return set(paths)

    def invalid_zfs_sources(self) -> List[str]:
        invalid_zfs_sources = []
        for zfs_source in self.zfs_source:
            if not self.shell_command.has_dataset(zfs_source):
                invalid_zfs_sources.append(zfs_source)
        return invalid_zfs_sources

    def get_matching_datasets(self, refresh: bool = False) -> List[DataSet]:
        if refresh:
            self._datasets = None
        if self._datasets is None:
            self._datasets = []
            for zfs_source in self.zfs_source:
                if self.recursive:
                    self._datasets.extend(DataSet.get_recursive(self.shell_command, zfs_source))
                else:
                    self._datasets.append(DataSet(self.shell_command, zfs_source))
            self._datasets = sorted(self._datasets, key=lambda dataset: dataset.zfs_path)
        return self._datasets
