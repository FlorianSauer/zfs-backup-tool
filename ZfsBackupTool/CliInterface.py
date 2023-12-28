from ZfsBackupTool.ShellCommand import ShellCommand


class CliInterface(object):
    def __init__(self, shell_command: ShellCommand):
        self.shell_command = shell_command

    def invalidate_caches(self) -> None:
        pass
