import os
import shlex
import sys
import threading
from subprocess import Popen
from typing import List, Tuple, Dict, Set, cast, IO

from .Base import BaseShellCommand, CommandExecutionError, PipePrinterThread
from ..Constants import TARGET_STORAGE_SUBDIRECTORY, BACKUP_FILE_POSTFIX, CALCULATED_CHECKSUM_FILE_POSTFIX


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

    def _target_get_checksum(self, source_dataset: str, next_snapshot: str, target_path: str) -> Tuple[Popen, str]:
        if self.remote:
            command = self._get_ssh_command(self.remote)
            checksum_command = 'pv {} --name "{}" --cursor "{}"'.format(
                self._PV_DEFAULT_OPTIONS,
                target_path,
                os.path.join(target_path, TARGET_STORAGE_SUBDIRECTORY, source_dataset,
                             next_snapshot + BACKUP_FILE_POSTFIX))
            checksum_command += ' | sha256sum -b > "{}"'.format(
                os.path.join(target_path, TARGET_STORAGE_SUBDIRECTORY, source_dataset,
                             next_snapshot + BACKUP_FILE_POSTFIX + CALCULATED_CHECKSUM_FILE_POSTFIX))
            command += shlex.quote(checksum_command)
        else:
            command = 'pv {} --name "{}" --cursor "{}"'.format(
                self._PV_DEFAULT_OPTIONS,
                target_path,
                os.path.join(target_path, TARGET_STORAGE_SUBDIRECTORY, source_dataset,
                             next_snapshot + BACKUP_FILE_POSTFIX))
            command += ' | sha256sum -b > "{}"'.format(
                os.path.join(target_path, TARGET_STORAGE_SUBDIRECTORY, source_dataset,
                             next_snapshot + BACKUP_FILE_POSTFIX + CALCULATED_CHECKSUM_FILE_POSTFIX))

        sub_process = self._execute(command, capture_output=True, capture_stdout=False, capture_stderr=True,
                                    no_wait=True)

        return sub_process, command

    def target_get_checksums(self, source_dataset: str, next_snapshot: str, target_paths: Set[str]):
        output_dict: Dict[str, str] = {}
        print_lock = threading.Lock()

        sys.stdout.flush()
        sys.stderr.flush()

        target_process_printer_mapping: Dict[str, Tuple[Popen, PipePrinterThread]] = {}

        for target_index, target_path in enumerate(sorted(target_paths)):
            pv_process, command = self._target_get_checksum(source_dataset, next_snapshot, target_path)
            stderr_printer = PipePrinterThread(cast(IO[bytes], pv_process.stderr), sys.stderr.buffer, target_index,
                                               print_lock)
            if target_index > 0:
                sys.stderr.write('\n\r')

            target_process_printer_mapping[target_path] = (pv_process, stderr_printer)

        sys.stdout.flush()
        sys.stderr.flush()

        # start printer threads
        for _, stderr_printer in target_process_printer_mapping.values():
            stderr_printer.start()

        try:
            # wait for all processes to finish
            for pv_process, _ in target_process_printer_mapping.values():
                pv_process.wait()
        except KeyboardInterrupt:
            # abort all printer threads
            for _, stderr_printer in target_process_printer_mapping.values():
                stderr_printer.abort()
            # kill all processes
            for pv_process, _ in target_process_printer_mapping.values():
                pv_process.kill()
            # and wait for them to terminate
            for pv_process, _ in target_process_printer_mapping.values():
                pv_process.wait()
            raise
        finally:
            sys.stderr.write('\n\r')
            sys.stdout.flush()
            sys.stderr.flush()

        for target_path, (pv_process, stderr_printer) in target_process_printer_mapping.items():
            stderr_printer.join()
            if pv_process.returncode != 0:
                raise CommandExecutionError(pv_process, "Error executing command > {} <".format(str(pv_process.args)))

            checksum_file = os.path.join(target_path, TARGET_STORAGE_SUBDIRECTORY, source_dataset,
                                         next_snapshot + BACKUP_FILE_POSTFIX + CALCULATED_CHECKSUM_FILE_POSTFIX)
            checksum = self.target_read_checksum_from_file(checksum_file)
            output_dict[target_path] = checksum

        return output_dict
