from .Base import BaseShellCommand
from .FsCommands import FsCommands
from .SshHost import SshHost
from .ZfsCommands import ZfsCommands


class ShellCommand(ZfsCommands, FsCommands, BaseShellCommand):

    def __init__(self, echo_cmd=False):
        BaseShellCommand.__init__(self, echo_cmd)
        FsCommands.__init__(self, echo_cmd)
        ZfsCommands.__init__(self, echo_cmd)
