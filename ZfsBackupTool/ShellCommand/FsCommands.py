import os
import shlex
import sys
import threading
from pathlib import Path
from subprocess import Popen
from typing import List, Tuple, Dict, Set, cast, IO

from .Base import BaseShellCommand, CommandExecutionError, PipePrinterThread
from ..Constants import CALCULATED_CHECKSUM_FILE_POSTFIX


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

    def target_remove_files(self, paths: List[str]):
        if self.remote:
            command = self._get_ssh_command(self.remote)
            command += shlex.quote('rm -f {}'.format(' '.join('"{}"'.format(path) for path in paths)))
        else:
            command = 'rm -f {}'.format(' '.join('"{}"'.format(path) for path in paths))
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
            command += shlex.quote('test -d "{}" && echo exist || echo not'.format(path))
        else:
            command = 'test -d "{}" && echo exist || echo not'.format(path)
        # test is more verbose, to catch ssh errors or other unexpected shell errors
        result = self._execute(command, capture_output=True).stdout.read().decode('utf-8').strip()
        if result == 'exist':
            return True
        elif result == 'not':
            return False
        else:
            raise NotImplementedError("Unexpected result: {}".format(result))

    def target_file_exists(self, path: str):
        if self.remote:
            command = self._get_ssh_command(self.remote)
            command += shlex.quote('test -f "{}" && echo exist || echo not'.format(path))
        else:
            command = 'test -f "{}" && echo exist || echo not'.format(path)
        # test is more verbose, to catch ssh errors or other unexpected shell errors
        result = self._execute(command, capture_output=True).stdout.read().decode('utf-8').strip()
        if result == 'exist':
            return True
        elif result == 'not':
            return False
        else:
            raise NotImplementedError("Unexpected result: {}".format(result))

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
            print("Program '{}' is installed".format(program))
            return True

    def _target_get_checksum(self, file_path: str, pv_name: str) -> Tuple[Popen, str]:
        if self.remote:
            command = self._get_ssh_command(self.remote)
            checksum_command = 'pv {} --name "{}" --cursor "{}"'.format(
                self._PV_DEFAULT_OPTIONS,
                pv_name,
                file_path)
            checksum_command += ' | sha256sum -b > "{}"'.format(
                file_path + CALCULATED_CHECKSUM_FILE_POSTFIX)
            command += shlex.quote(checksum_command)
        else:
            command = 'pv {} --name "{}" --cursor "{}"'.format(
                self._PV_DEFAULT_OPTIONS,
                pv_name,
                file_path)
            command += ' | sha256sum -b > "{}"'.format(
                file_path + CALCULATED_CHECKSUM_FILE_POSTFIX)

        sub_process = self._execute(command, capture_output=True, capture_stdout=False, capture_stderr=True,
                                    no_wait=True)

        return sub_process, command

    def target_get_checksums(self, file_paths: Set[str]):
        output_dict: Dict[str, str] = {}
        print_lock = threading.Lock()

        sys.stdout.flush()
        sys.stderr.flush()

        target_process_printer_mapping: Dict[str, Tuple[Popen, PipePrinterThread]] = {}

        # find common path prefix
        common_prefix = str(os.path.commonpath(*file_paths))

        for file_index, file_path in enumerate(sorted(file_paths)):
            # build pv_name, by using the common prefix as base and ONE filepath part of the file path
            pv_name = os.path.join(common_prefix,
                                   Path(file_path.replace(common_prefix, '', 1)).parts[0])
            pv_process, command = self._target_get_checksum(file_path, pv_name)
            stderr_printer = PipePrinterThread(cast(IO[bytes], pv_process.stderr), sys.stderr.buffer, file_index,
                                               print_lock)
            if file_index > 0:
                sys.stderr.write('\n\r')

            target_process_printer_mapping[file_path] = (pv_process, stderr_printer)

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

        for file_path, (pv_process, stderr_printer) in target_process_printer_mapping.items():
            stderr_printer.join()
            if pv_process.returncode != 0:
                raise CommandExecutionError(pv_process, "Error executing command > {} <".format(str(pv_process.args)))

            checksum_file = file_path + CALCULATED_CHECKSUM_FILE_POSTFIX
            checksum = self.target_read_checksum_from_file(checksum_file)
            output_dict[file_path] = checksum

        return output_dict
