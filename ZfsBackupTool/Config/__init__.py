from typing import List

from ..ShellCommand.SshHost import SshHost


class TargetGroup(object):
    def __init__(self, name: str, target_paths: List[str], remote: SshHost = None):
        self.name = name
        self.target_paths = target_paths
        self.remote = remote


class BackupSource(object):
    def __init__(self, name: str, source_datasets: List[str], targets: List[TargetGroup], recursive: bool,
                 exclude: List[str], include: List[str]):
        self.name = name
        self.source_datasets = source_datasets
        self.targets = targets
        self.recursive = recursive
        self.exclude = exclude
        self.include = include
