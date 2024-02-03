import os
import re
from typing import List, Optional, Set, Dict, Iterable

from ZfsBackupTool.CliInterface import CliInterface
from ZfsBackupTool.Constants import TARGET_SUBDIRECTORY
from ZfsBackupTool.DataSet import DataSet
from ZfsBackupTool.ShellCommand import ShellCommand, CommandExecutionError
from ZfsBackupTool.TargetDataSet import TargetDataSet
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

    def _matches_regex(self, include_regex, exclude_regex, zfs_path) -> bool:
        for regex in include_regex:
            if regex.match(zfs_path):
                return True
        for regex in exclude_regex:
            if regex.match(zfs_path):
                return False
        return not include_regex

    def get_matching_datasets(self, refresh: bool = False) -> List[DataSet]:
        if refresh:
            self._datasets = None
        if self._datasets is None:
            self._datasets = []
            for zfs_source in sorted(self.zfs_source):
                if self.recursive:
                    self._datasets.extend(DataSet.get_recursive(self.shell_command, zfs_source,
                                                                self.get_all_target_paths()))
                else:
                    self._datasets.append(DataSet(self.shell_command, zfs_source,
                                                  self.get_all_target_paths()))
            if self.include or self.exclude:
                include_regex = [re.compile(s) for s in self.include] if self.include else []
                exclude_regex = [re.compile(s) for s in self.exclude] if self.exclude else []
                self._datasets = [dataset for dataset in self._datasets if self._matches_regex(
                    include_regex, exclude_regex, dataset.zfs_path)]

            self._datasets = sorted(self._datasets, key=lambda dataset: dataset.zfs_path)

        return self._datasets

    def filter_matching_datasets(self, datasets: Iterable[str]) -> List[str]:
        matching_datasets = []
        for dataset in datasets:
            if self.recursive:
                if not any(dataset.startswith(s) for s in self.zfs_source):
                    continue
            else:
                if dataset not in self.zfs_source:
                    continue
            if self.include or self.exclude:
                include_regex = [re.compile(s) for s in self.include] if self.include else []
                exclude_regex = [re.compile(s) for s in self.exclude] if self.exclude else []
                if not self._matches_regex(include_regex, exclude_regex, dataset):
                    continue
            matching_datasets.append(dataset)
        return matching_datasets

    def get_matching_target_datasets(self) -> List[TargetDataSet]:
        all_target_datasets = {d.zfs_path: d for d in self.get_available_target_datasets()}
        return [all_target_datasets[dataset]
                for dataset in sorted(self.filter_matching_datasets(all_target_datasets.keys()))]

    def get_available_target_datasets(self) -> List[TargetDataSet]:
        """
        Get all datasets that are available on any linked target.

        Includes datasets, that are only available on a single target.
        Returned TargetDataSet objects will contain all targets, where its dataset exists.
        """

        collected_target_datasets: Dict[str, TargetDataSet] = {}
        for zfs_path in self.zfs_source:
            for target_group in self.targets:
                for target_path in target_group.paths:
                    target_path_prefix = os.path.join(target_path, TARGET_SUBDIRECTORY) + os.path.sep
                    target_dataset_dirs = self._get_directory_paths(os.path.join(target_path_prefix, zfs_path))
                    if not target_dataset_dirs:
                        continue
                    for target_dataset_path in target_dataset_dirs:
                        target_zfs_path = target_dataset_path.replace(target_path_prefix, '')
                        if target_zfs_path not in collected_target_datasets:
                            target_dataset = TargetDataSet(self.shell_command, target_zfs_path,
                                                           [target_path, ])
                            collected_target_datasets[target_zfs_path] = target_dataset
                        else:
                            target_dataset = collected_target_datasets[target_zfs_path]
                            target_dataset.add_target_path(target_path)

        return [collected_target_datasets[dataset] for dataset in sorted(collected_target_datasets.keys())]

    def is_initialized(self) -> bool:
        return all(target.is_initialized() for target in self.targets)

    def _get_file_paths(self, path: str, file_postfix: str, collected_files: Set[str] = None) -> Set[str]:
        if collected_files is None:
            collected_files = set()
        files, directories = self.shell_command.target_list_directory(path)
        for file in files:
            if file.endswith(file_postfix):
                # collected_files.add(
                #     os.path.join(path, file).replace(os.path.join(target, TARGET_SUBDIRECTORY) + os.path.sep, ''))
                collected_files.add(os.path.join(path, file))
        for directory in directories:
            self._get_file_paths(os.path.join(path, directory), file_postfix, collected_files)
        return collected_files

    def _get_directory_paths(self, path: str, collected_directories: Set[str] = None) -> Set[str]:
        if collected_directories is None:
            collected_directories = set()
        collected_directories.add(path)
        try:
            files, directories = self.shell_command.target_list_directory(path)
        except CommandExecutionError:
            return collected_directories
        for directory in directories:
            collected_directories.add(os.path.join(path, directory))
        for directory in directories:
            self._get_directory_paths(os.path.join(path, directory), collected_directories)
        return collected_directories
