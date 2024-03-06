import os
from typing import List

from .CliInterface import CliInterface
from .Constants import TARGET_SUBDIRECTORY, INITIALIZED_FILE_NAME
from .ShellCommand import ShellCommand


class TargetGroup(CliInterface):

    def __init__(self, shell_command: ShellCommand, paths: List[str], name: str):
        super().__init__(shell_command)
        self.paths = paths
        self.name = name

    def is_initialized(self) -> bool:
        for target in self.paths:
            if not self.shell_command.target_file_exists(
                    os.path.join(target, TARGET_SUBDIRECTORY, INITIALIZED_FILE_NAME)):
                return False
        return True
