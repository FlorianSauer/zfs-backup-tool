import os.path
import re
from typing import List, Set, Tuple, Optional, Dict

from ZfsBackupTool.Config import BackupSource
from .Constants import TARGET_STORAGE_SUBDIRECTORY, SNAPSHOT_PREFIX_POSTFIX_SEPARATOR
from .ShellCommand import ShellCommand, SshHost
from .Zfs import DataSet, scan_filebased_zfs_pools, PoolList


class BackupSetup(object):
    def __init__(self, sources: List[BackupSource], snapshot_prefix: str = None,
                 include_intermediate_snapshots: bool = False,
                 target_path_prefix: str = None):
        self.sources = {source.name: source for source in sources}
        self._sources_regex_include = {}
        self._sources_regex_exclude = {}
        for source in sources:
            self._sources_regex_include[source] = [re.compile(pattern) for pattern in source.include]
            self._sources_regex_exclude[source] = [re.compile(pattern) for pattern in source.exclude]
        self.snapshot_prefix = snapshot_prefix if snapshot_prefix is not None else "backup-snapshot"
        self.include_intermediate_snapshots = include_intermediate_snapshots

    def get_all_host_target_paths(self) -> Set[Tuple[Optional[SshHost], str]]:
        paths = []
        for source in self.sources.values():
            for target in source.targets:
                for target_path in target.target_paths:
                    paths.append((target.remote, target_path))
        return set(paths)

    def get_all_hosts(self) -> Set[Optional[SshHost]]:
        return {target.remote for source in self.sources.values() for target in source.targets}

    def dataset_matches_sources(self, dataset: DataSet) -> bool:
        for source in self.sources.values():
            if self.dataset_matches_source(dataset, source):
                return True
        return False

    def dataset_matches_source(self, dataset: DataSet, source: BackupSource) -> bool:
        dataset_zfs_path = dataset.zfs_path
        for include_pattern in self._sources_regex_include[source]:
            if include_pattern.match(dataset_zfs_path):
                return True
        for exclude_pattern in self._sources_regex_exclude[source]:
            if exclude_pattern.match(dataset_zfs_path):
                return False
        if source.recursive:
            for source_dataset in source.source_datasets:
                if dataset_zfs_path.startswith(source_dataset):
                    return True
        return dataset_zfs_path in source.source_datasets

    def filter_by_sources(self, pools: PoolList) -> Dict[BackupSource, PoolList]:
        source_pool_view_mapping: Dict[BackupSource, PoolList] = {}
        """Maps a backup source to a logical view of a pool list"""

        for source in self.sources.values():
            pools_view = pools.view()
            source_pool_view_mapping[source] = pools_view

            # iter pools, add datasets, add snapshots
            for pool_view in pools_view:
                for dataset_view in pool_view:
                    if not self.dataset_matches_source(dataset_view, source):
                        pool_view.remove_dataset(dataset_view)
                        continue

                    for snapshot_view in dataset_view:
                        if not snapshot_view.snapshot_name.startswith(
                                self.snapshot_prefix + SNAPSHOT_PREFIX_POSTFIX_SEPARATOR):
                            dataset_view.remove_snapshot(snapshot_view)
                            continue

        return source_pool_view_mapping

    def gather_target_pools(self, shell_command: ShellCommand, include_all: bool = False
                            ) -> Dict[Tuple[Optional[SshHost], str], PoolList]:
        """
        Gather all pools from the available target paths.
        The target pools may contain datasets, that are not part of the configuration.
        Filtering is only based on the configured snapshot prefix by default.

        :param shell_command: The command issuing object to use
        :param include_all: also include snapshots that do not match the snapshot prefix
        :return:
        """

        host_target_path_pool_mapping: Dict[Tuple[Optional[SshHost], str], PoolList] = {}

        host_target_path_list: Dict[Optional[SshHost], Set[str]] = {}
        for source in self.sources.values():
            for target_group in source.targets:
                if target_group.remote not in host_target_path_list:
                    host_target_path_list[target_group.remote] = set()
                for target_path in target_group.target_paths:
                    host_target_path_list[target_group.remote].add(target_path)

        for host, target_paths in host_target_path_list.items():
            shell_command.set_remote_host(host)

            # first scan for remote pools
            for target_path in target_paths:
                target_pool_storage_path = os.path.join(target_path, TARGET_STORAGE_SUBDIRECTORY)
                pools = scan_filebased_zfs_pools(shell_command, target_pool_storage_path)
                # print("Found pools: ", [pool.pool_name for pool in pools])
                host_target_path_pool_mapping[(host, target_path)] = pools

                # strip snapshots from pools that do not match the snapshot name
                if not include_all:
                    for pool in pools:
                        for dataset in pool:
                            for snapshot in dataset:
                                if not snapshot.snapshot_name.startswith(
                                        self.snapshot_prefix + SNAPSHOT_PREFIX_POSTFIX_SEPARATOR):
                                    dataset.remove_snapshot(snapshot)

        return host_target_path_pool_mapping
