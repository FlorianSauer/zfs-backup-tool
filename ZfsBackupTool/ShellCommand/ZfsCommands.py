from typing import List, Optional

from .Base import BaseShellCommand, CommandExecutionError


class ZfsCommands(BaseShellCommand):

    def __init__(self, echo_cmd=False):
        super().__init__(echo_cmd)

    def list_pools(self) -> List[str]:
        command = "zpool list -H -o name"
        sub_process = self._execute(command, capture_output=True)
        stdout_lines = sub_process.stdout.read().decode('utf-8').splitlines() if sub_process.stdout else []
        return [line.strip() for line in stdout_lines]

    def list_datasets(self, pool: str) -> List[str]:
        command = "zfs list -H -r -o name"
        command += ' "{}"'.format(pool)
        sub_process = self._execute(command, capture_output=True)
        stdout_lines = sub_process.stdout.read().decode('utf-8').splitlines() if sub_process.stdout else []
        datasets = [line.strip().replace(pool + '/', '') for line in stdout_lines if line.strip() != pool]
        return datasets

    def list_snapshots(self, dataset: str) -> List[str]:
        command = "zfs list -H -o name -t snapshot"
        command += ' "{}"'.format(dataset)
        sub_process = self._execute(command, capture_output=True)
        stdout_lines = sub_process.stdout.read().decode('utf-8').splitlines() if sub_process.stdout else []
        return [line.strip().replace(dataset + '@', '')
                for line in stdout_lines]

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

    def get_dataset_size(self, dataset: str, recursive: bool) -> int:
        command = 'zfs list -p -H -o refer'
        if recursive:
            command += " -r"
        command += ' "{}"'.format(dataset)
        sub_process = self._execute(command, capture_output=True)
        stdout_lines = sub_process.stdout.read().decode('utf-8').splitlines() if sub_process.stdout else []
        return int(stdout_lines[0].strip())

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

    def create_snapshot(self, source_dataset: str, next_snapshot: str):
        command = 'zfs snapshot "{}@{}"'.format(source_dataset, next_snapshot)
        return self._execute(command, capture_output=False)

    def delete_snapshot(self, source_dataset: str, snapshot: str):
        command = 'zfs destroy "{}@{}"'.format(source_dataset, snapshot)
        return self._execute(command, capture_output=False)
