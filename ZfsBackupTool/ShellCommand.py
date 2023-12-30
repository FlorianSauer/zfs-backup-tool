import os
import shlex
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import List, Optional, Set, Dict, Tuple, cast

from ZfsBackupTool.Constants import TARGET_SUBDIRECTORY, BACKUP_FILE_POSTFIX
from ZfsBackupTool.SshHost import SshHost


class CommandExecutionError(Exception):

    def __init__(self, sub_process: subprocess.Popen, *args):
        super().__init__(*args)
        self.sub_process = sub_process


class ShellCommand(object):
    _PV_DEFAULT_OPTIONS = "--force --rate --average-rate --bytes --timer --eta"

    def __init__(self, echo_cmd=False, remote: SshHost = None):
        self.echo_cmd = echo_cmd
        self.remote = remote

    def set_remote_host(self, remote: Optional[SshHost]):
        self.remote = remote

    def _execute(self, command: str, capture_output: bool, capture_stdout=True, capture_stderr=True,
                 dev_null_output=False) -> subprocess.Popen:
        if capture_output and dev_null_output:
            raise ValueError("capture_output and dev_null_output cannot be used together")
        if self.echo_cmd:
            print("$ {}".format(command))
        if capture_output:
            if capture_stdout:
                stdout = subprocess.PIPE
            else:
                stdout = None
            if capture_stderr:
                stderr = subprocess.PIPE
            else:
                stderr = None
            sub_process = subprocess.Popen(command, shell=True, stdout=stdout, stderr=stderr,
                                           executable="/bin/bash")
        elif dev_null_output:
            sub_process = subprocess.Popen(command, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                           executable="/bin/bash")
        else:
            sub_process = subprocess.Popen(command, shell=True, executable="/bin/bash")
        sub_process.wait()
        if sub_process.returncode != 0:
            if capture_output and capture_stderr:
                stderr_data = sub_process.stderr.read().decode('utf-8') if sub_process.stderr else ""
                raise CommandExecutionError(sub_process, "Error executing command > {} <\n{}".format(
                    command, stderr_data))
            raise CommandExecutionError(sub_process, "Error executing command > {} <".format(command))
        return sub_process

    def _get_ssh_command(self, remote: SshHost):
        command = "ssh -o BatchMode=yes "
        if remote.key_path:
            command += '-i "{}"'.format(remote.key_path)
        if remote.port:
            command += "-p {}".format(remote.port)
        if remote.user:
            command += "{}@{} ".format(remote.user, remote.host)
        else:
            command += "{} ".format(remote.host)
        return command

    def target_mkdir(self, path: str):
        if self.remote:
            command = self._get_ssh_command(self.remote)
            command += shlex.quote('mkdir -p "{}"'.format(path))
        else:
            command = 'mkdir -p "{}"'.format(path)
        return self._execute(command, capture_output=False)

    def target_write_to_file(self, path: str, content: str):
        command = "echo '{}' | ".format(content)
        if self.remote:
            command += self._get_ssh_command(self.remote)
            command += shlex.quote('cat - > "{}"'.format(path))
        else:
            command += 'cat - > "{}"'.format(path)
        return self._execute(command, capture_output=False)

    def get_datasets(self, config_source_dataset: str, recursive: bool) -> List[str]:
        command = "zfs list -H -o name"
        if recursive:
            command += " -r"
        command += ' "{}"'.format(config_source_dataset)
        sub_process = self._execute(command, capture_output=True)
        stdout_lines = sub_process.stdout.read().decode('utf-8').splitlines() if sub_process.stdout else []
        return [line.strip() for line in stdout_lines]

    def has_dataset(self, dataset: str) -> bool:
        command = "zfs list -H -o name"
        command += ' | grep -q -e "{}"'.format(dataset)
        try:
            sub_process = self._execute(command, capture_output=True)
        except CommandExecutionError as e:
            exit_code = e.sub_process.returncode
        else:
            exit_code = sub_process.returncode
        return exit_code == 0

    _NONE = cast(SshHost, object())

    def program_is_installed(self, program: str, remote: Optional[SshHost] = _NONE) -> bool:
        # fall back to default set remote host
        if remote is self._NONE:
            remote = self.remote
        if remote:
            command = self._get_ssh_command(remote)
            command += shlex.quote('which "{}"'.format(program))
        else:
            command = 'which "{}"'.format(program)

        try:
            self._execute(command, capture_output=True)
        except CommandExecutionError as e:
            print("Program '{}' not installed".format(program), file=sys.stderr)
            stderr_data = e.sub_process.stderr.read().decode('utf-8') if e.sub_process.stderr else ""
            print(stderr_data, file=sys.stderr)
            sys.exit(1)
        else:
            return True

    def create_snapshot(self, source_dataset: str, next_snapshot: str):
        command = 'zfs snapshot "{}@{}"'.format(source_dataset, next_snapshot)
        return self._execute(command, capture_output=False)

    def get_snapshots(self, source_dataset: str) -> List[str]:
        command = "zfs list -H -o name -t snapshot"
        command += ' "{}"'.format(source_dataset)
        sub_process = self._execute(command, capture_output=True)
        stdout_lines = sub_process.stdout.read().decode('utf-8').splitlines() if sub_process.stdout else []
        return [line.strip().replace(source_dataset + "@", "")
                for line in stdout_lines]

    def delete_snapshot(self, source_dataset: str, snapshot: str):
        command = 'zfs destroy "{}@{}"'.format(source_dataset, snapshot)
        return self._execute(command, capture_output=False)

    def get_estimated_snapshot_size(self, source_dataset: str, previous_snapshot: Optional[str], next_snapshot: str,
                                    include_intermediate_snapshots: bool = False):
        if previous_snapshot:
            sub_process = self._execute('zfs send -n -P --raw {} "{}@{}" "{}@{}"'.format(
                "-I" if include_intermediate_snapshots else '-i',
                source_dataset, previous_snapshot, source_dataset, next_snapshot), capture_output=True
            )
        else:
            sub_process = self._execute('zfs send -n -P --raw "{}@{}"'.format(
                source_dataset, next_snapshot), capture_output=True
            )
        stdout_lines = sub_process.stdout.read().decode('utf-8').splitlines() if sub_process.stdout else []

        for line in stdout_lines:
            if line.lower().startswith("size"):
                return int(line.replace("size", "").strip())
        raise ValueError("Could not determine snapshot size")

    def zfs_send_snapshot_to_target(self, source_dataset: str,
                                    previous_snapshot: Optional[str], next_snapshot: str,
                                    target_paths: Set[str],
                                    include_intermediate_snapshots: bool = False) -> str:

        estimated_size = self.get_estimated_snapshot_size(source_dataset, previous_snapshot, next_snapshot,
                                                          include_intermediate_snapshots)

        # with temporary file
        with tempfile.NamedTemporaryFile() as tmp:
            if previous_snapshot:
                command = 'zfs send --raw {} "{}@{}" "{}@{}" '.format(
                    "-I" if include_intermediate_snapshots else '-i',
                    source_dataset, previous_snapshot, source_dataset, next_snapshot)
            else:
                command = 'zfs send --raw "{}@{}"'.format(source_dataset, next_snapshot)

            command += ' | tee >( sha256sum -b > "{}" )'.format(tmp.name)
            command += ' | pv {} --size {}'.format(self._PV_DEFAULT_OPTIONS, estimated_size)

            if self.remote:
                command += ' | ' + self._get_ssh_command(self.remote)
                tee_quoted_paths = ' '.join('"{}"'.format(
                    os.path.join(path, TARGET_SUBDIRECTORY, source_dataset, next_snapshot + BACKUP_FILE_POSTFIX))
                                            for path in sorted(target_paths))
                command += shlex.quote('tee {} > /dev/null'.format(tee_quoted_paths))
            else:
                tee_quoted_paths = ' '.join('"{}"'.format(
                    os.path.join(path, TARGET_SUBDIRECTORY, source_dataset, next_snapshot + BACKUP_FILE_POSTFIX))
                                            for path in sorted(target_paths))
                command += ' | tee {} > /dev/null'.format(tee_quoted_paths)

            self._execute(command, capture_output=False)
            return tmp.read().decode('utf-8').strip().split(' ')[0]

    def _target_get_checksum(self, output_dict: Dict[str, str], access_lock: threading.Lock,
                             source_dataset: str, next_snapshot: str, target_path: str) -> None:
        if self.remote:
            command = self._get_ssh_command(self.remote)
            checksum_command = 'pv {} --name "{}" --cursor "{}"'.format(
                self._PV_DEFAULT_OPTIONS,
                target_path,
                os.path.join(target_path, TARGET_SUBDIRECTORY, source_dataset,
                             next_snapshot + BACKUP_FILE_POSTFIX))
            checksum_command += ' | sha256sum -b'
            command += shlex.quote(checksum_command)
        else:
            command = 'pv {} --name "{}" --cursor "{}"'.format(
                self._PV_DEFAULT_OPTIONS,
                target_path,
                os.path.join(target_path, TARGET_SUBDIRECTORY, source_dataset,
                             next_snapshot + BACKUP_FILE_POSTFIX))
            command += ' | sha256sum -b'
        sub_process = self._execute(command, capture_output=True, capture_stderr=False
                                    )
        if not sub_process.stdout:
            raise ValueError("Could not determine checksum")
        checksum = sub_process.stdout.read().decode('utf-8').strip().split(' ')[0]
        with access_lock:
            output_dict[target_path] = checksum

    def target_get_checksums(self, source_dataset: str, next_snapshot: str, target_paths: Set[str]):
        threads = []
        output_dict: Dict[str, str] = {}
        access_lock = threading.Lock()
        for target_path in sorted(target_paths):
            thread = threading.Thread(target=self._target_get_checksum,
                                      args=(output_dict, access_lock, source_dataset, next_snapshot, target_path),
                                      daemon=True)
            threads.append(thread)

        for thread in threads:
            thread.start()
            time.sleep(1)
        for thread in threads:
            thread.join()

        return output_dict

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
        """Returns a tuple of files and directories in the given directory"""
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

    def zfs_recv_snapshot_from_target(self, root_path: str, source_dataset: str, snapshot: str, target: str) -> None:

        # create datasets under root path, without the last part of the dataset path.
        # the last part is created by zfs recv.
        # otherwise, when an encrypted dataset is received, the unencrypted dataset would be overwritten by an
        # encrypted dataset, which is forbidden by zfs.
        re_joined_parts = root_path
        for dataset in Path(source_dataset).parts[:-1]:
            re_joined_parts = os.path.join(re_joined_parts, dataset)
            if not self.has_dataset(re_joined_parts):
                self._execute('zfs create "{}"'.format(re_joined_parts), capture_output=False)

        backup_path = os.path.join(target, TARGET_SUBDIRECTORY, source_dataset, snapshot + BACKUP_FILE_POSTFIX)
        if self.remote:
            command = self._get_ssh_command(self.remote)
            command += shlex.quote(
                'pv {} "{}"'.format(self._PV_DEFAULT_OPTIONS, backup_path))
        else:
            command = 'pv {} "{}"'.format(self._PV_DEFAULT_OPTIONS, backup_path)
        command += ' | zfs recv -F "{}"'.format(os.path.join(root_path, source_dataset))

        self._execute(command, capture_output=False)
