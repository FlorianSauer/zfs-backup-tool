import os
from typing import List, Optional

from .CliInterface import CliInterface
from .Constants import TARGET_SUBDIRECTORY, INITIALIZED_FILE_NAME
from .ShellCommand import ShellCommand
from .SshHost import SshHost


class TargetGroup(CliInterface):

    def __init__(self, shell_command: ShellCommand, paths: List[str], name: str):
        super().__init__(shell_command)
        self.paths = paths
        self.name = name

    def is_initialized(self, remote: Optional[SshHost] = None) -> bool:
        for target in self.paths:
            if not self.shell_command.target_file_exists(os.path.join(target, TARGET_SUBDIRECTORY, INITIALIZED_FILE_NAME),
                                                         remote):
                return False
        return True
