from typing import List, Set

from ZfsBackupTool.BackupSource import BackupSource
from ZfsBackupTool.SshHost import SshHost


class BackupSetup(object):
    def __init__(self, sources: List[BackupSource], remote: SshHost = None, snapshot_prefix: str = None,
                 include_intermediate_snapshots: bool = False):
        self.sources = sources
        self.remote = remote
        self.snapshot_prefix = snapshot_prefix or "backup-snapshot"
        self.include_intermediate_snapshots = include_intermediate_snapshots

    def get_all_target_paths(self) -> Set[str]:
        paths = []
        for source in self.sources:
            for target in source.targets:
                paths.extend(target.paths)
        return set(paths)
