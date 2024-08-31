import subprocess
import threading
from subprocess import Popen
from typing import Optional, IO, cast

from .SshHost import SshHost


class CommandExecutionError(Exception):

    def __init__(self, sub_process: Popen, *args):
        super().__init__(*args)
        self.sub_process = sub_process


class PipePrinterThread(threading.Thread):
    def __init__(self, pipe: IO[bytes], output: IO[bytes], row_index: int, write_lock: threading.Lock,
                 separator: bytes = b'\r'):
        super().__init__(daemon=True)
        self.pipe: IO[bytes] = pipe
        self.output = output
        self.row_index = row_index
        self.write_lock = write_lock
        self.separator = separator
        self._aborted = False

    def abort(self):
        self._aborted = True

    def run(self):
        buffer: bytes = b''
        while True:  # until EOF
            chunk: bytes = self.pipe.read(1)  # limit to 1 byte, to bypass buffering
            if self._aborted:
                return
            if not chunk:  # EOF
                with self.write_lock:
                    if self.row_index:
                        self.output.write('\033[{}A'.format(self.row_index).encode('utf-8'))
                    self.output.write(
                        bytes(reversed(
                            bytes(reversed(
                                buffer)
                            ).replace(b'\n', b'', 1))))
                    if self.row_index:
                        self.output.write('\033[{}B'.format(self.row_index).encode('utf-8'))
                    self.output.flush()
                break
            buffer += chunk
            while True:  # until no separator is found
                try:
                    part, buffer = buffer.split(self.separator, 1)
                except ValueError:
                    break
                else:
                    with self.write_lock:
                        if self.row_index:
                            self.output.write('\033[{}A'.format(self.row_index).encode('utf-8'))
                        self.output.write(part)
                        self.output.write(self.separator)
                        if self.row_index:
                            self.output.write('\033[{}B'.format(self.row_index).encode('utf-8'))
                        self.output.flush()


class BaseShellCommand(object):
    _PV_DEFAULT_OPTIONS = "--force --rate --average-rate --bytes --timer --eta"

    def __init__(self, echo_cmd=False):
        self.echo_cmd = echo_cmd
        self.remote = None

    def set_remote_host(self, remote: Optional[SshHost]):
        self.remote = remote

    def _execute(self, command: str, capture_output: bool, capture_stdout=True, capture_stderr=True,
                 dev_null_output=False, no_wait: bool = False) -> Popen:
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
            sub_process = subprocess.Popen(command, shell=True,
                                           stdout=stdout, stderr=stderr,
                                           stdin=subprocess.DEVNULL,
                                           executable="/bin/bash")
        elif dev_null_output:
            sub_process = subprocess.Popen(command, shell=True,
                                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                           stdin=subprocess.DEVNULL,
                                           executable="/bin/bash")
        else:
            sub_process = subprocess.Popen(command, shell=True, executable="/bin/bash")
        if not no_wait:
            sub_process.wait()
            if sub_process.returncode != 0:
                if capture_output and capture_stderr:
                    stderr_data = sub_process.stderr.read().decode('utf-8') if sub_process.stderr else ""
                    raise CommandExecutionError(sub_process, "Error executing command: {}\n{}".format(
                        str(sub_process.args), stderr_data))
                raise CommandExecutionError(sub_process, "Error executing command: {}".format(
                    str(sub_process.args)))
        return sub_process

    @classmethod
    def _get_ssh_command(cls, remote: SshHost):
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

    _NONE = cast(SshHost, object())

    def with_remote(self, remote):
        self.remote = remote
        try:
            return self
        finally:
            self.remote = None