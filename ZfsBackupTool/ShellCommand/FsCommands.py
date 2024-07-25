import shlex
import sys
from typing import List, Tuple

from .Base import BaseShellCommand, CommandExecutionError
from .SshHost import SshHost


class FsCommands(BaseShellCommand):

    def __init__(self, echo_cmd=False):
        super().__init__(echo_cmd)

    def target_mkdir(self, path: str):
        if self.remote:
            command = self._get_ssh_command(self.remote)
            command += shlex.quote('mkdir -p "{}"'.format(path))
        else:
            command = 'mkdir -p "{}"'.format(path)
        return self._execute(command, capture_output=False)

    def target_remove_file(self, path: str):
        if self.remote:
            command = self._get_ssh_command(self.remote)
            command += shlex.quote('rm -f "{}"'.format(path))
        else:
            command = 'rm -f "{}"'.format(path)
        return self._execute(command, capture_output=False)

    def target_write_to_file(self, path: str, content: str):
        command = "echo '{}' | ".format(content)
        if self.remote:
            command += self._get_ssh_command(self.remote)
            command += shlex.quote('cat - > "{}"'.format(path))
        else:
            command += 'cat - > "{}"'.format(path)
        return self._execute(command, capture_output=False)

    def target_dir_exists(self, path: str):
        if self.remote:
            command = self._get_ssh_command(self.remote)
            command += shlex.quote('test -d "{}"'.format(path))
        else:
            command = 'test -d "{}"'.format(path)
        try:
            sub_process = self._execute(command, capture_output=True)
        except CommandExecutionError as e:
            exit_code = e.sub_process.returncode
        else:
            exit_code = sub_process.returncode
        return exit_code == 0

    def target_file_exists(self, path: str):
        if self.remote:
            command = self._get_ssh_command(self.remote)
            command += shlex.quote('test -f "{}"'.format(path))
        else:
            command = 'test -f "{}"'.format(path)
        try:
            sub_process = self._execute(command, capture_output=True)
        except CommandExecutionError as e:
            exit_code = e.sub_process.returncode
        else:
            exit_code = sub_process.returncode
        return exit_code == 0

    def target_list_directory(self, path: str) -> Tuple[List[str], List[str]]:
        """
        Returns a tuple of files and directories in the given directory
        """
        if self.remote:
            command = self._get_ssh_command(self.remote)
            command += shlex.quote('ls -AF "{}"'.format(path))
        else:
            command = 'ls -AF "{}"'.format(path)
        sub_process = self._execute(command, capture_output=True)
        if not sub_process.stdout:
            raise ValueError("Could not list directory")
        files: List[str] = []
        directories: List[str] = []
        for line in sub_process.stdout.read().decode('utf-8').splitlines():
            line = line.strip()
            if line.endswith('/'):  # directory
                directories.append(line[:-1])
            elif line.endswith('*'):  # executable
                files.append(line[:-1])
            elif line[-1] not in ['@', '%', '|', '=', '>']:  # not other special files
                files.append(line)
        return files, directories

    def target_read_checksum_from_file(self, path: str) -> str:
        if self.remote:
            command = self._get_ssh_command(self.remote)
            command += shlex.quote('cat "{}"'.format(path))
        else:
            command = 'cat "{}"'.format(path)

        sub_process = self._execute(command, capture_output=True, capture_stderr=False)
        if not sub_process.stdout:
            raise ValueError("Could not determine checksum")
        content = sub_process.stdout.read().decode('utf-8').strip().split(' ')[0]
        return content

    def program_is_installed(self, program: str) -> bool:
        # fall back to default set remote host
        if self.remote:
            command = self._get_ssh_command(self.remote)
            command += shlex.quote('which "{}"'.format(program))
        else:
            command = 'which "{}"'.format(program)

        try:
            self._execute(command, capture_output=True)
        except CommandExecutionError as e:
            print("Program '{}' not installed".format(program))
            stderr_data = e.sub_process.stderr.read().decode('utf-8') if e.sub_process.stderr else ""
            print(stderr_data, file=sys.stderr)
            sys.exit(1)
        else:
            return True
