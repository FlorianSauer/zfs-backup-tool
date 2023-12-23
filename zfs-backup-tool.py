import argparse
import configparser
import os.path
import shlex
import subprocess
import sys
import tempfile
import threading
import time
from os.path import expandvars
from pathlib import Path
from typing import List, Set, Optional, Tuple, Dict

# region constants
INITIAL_SNAPSHOT_POSTFIX = "initial"
SNAPSHOT_PREFIX_POSTFIX_SEPARATOR = "_"
INITIALIZED_FILE_NAME = ".initialized"
TARGET_SUBDIRECTORY = "zfs"
BACKUP_FILE_POSTFIX = ".zfs"
CHECKSUM_FILE_POSTFIX = ".sha256"


# endregion

# region setup config classes
class SshHost(object):
    def __init__(self, host: str, user: str = None, port: int = None, key_path: str = None):
        self.host = host
        self.user = user
        self.port = port
        self.key_path = key_path


class TargetGroup(object):
    def __init__(self, paths: List[str], name: str):
        self.paths = paths
        self.name = name


class BackupSource(object):
    def __init__(self, name: str, zfs_source: List[str], targets: List[TargetGroup],
                 recursive: bool = False,
                 exclude: List[str] = None,
                 include: List[str] = None):
        self.name = name
        self.zfs_source = zfs_source
        self.targets = targets
        self.recursive = recursive
        self.exclude = exclude
        self.include = include

    def get_all_target_paths(self) -> Set[str]:
        paths = []
        for target in self.targets:
            paths.extend(target.paths)
        return set(paths)


class BackupSetup(object):
    def __init__(self, sources: List[BackupSource], remote: SshHost = None, snapshot_prefix: str = None,
                 include_intermediate_snapshots: bool = False):
        self.sources = sources
        self.remote = remote
        self.snapshot_prefix = snapshot_prefix or "backup-snapshot"
        self.include_intermediate_snapshots = include_intermediate_snapshots

    def get_all_target_paths(self) -> Set[str]:
        paths = []
        for source in self.sources:
            for target in source.targets:
                paths.extend(target.paths)
        return set(paths)


# endregion

# region setup helper class for environment expanding in config file
class EnvInterpolation(configparser.BasicInterpolation):
    """Interpolation which expands environment variables in values."""

    def before_get(self, parser, section, option, value, defaults):
        value = super().before_get(parser, section, option, value, defaults)
        return expandvars(value)


# endregion

# region helper class for command execution errors
class CommandExecutionError(Exception):

    def __init__(self, sub_process: subprocess.Popen, *args):
        super().__init__(*args)
        self.sub_process = sub_process


# endregion


class ShellCommand(object):
    def __init__(self, echo_cmd=False):
        self.echo_cmd = echo_cmd

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
                raise CommandExecutionError(sub_process, "Error executing command > {} <\n{}".format(
                    command, sub_process.stderr.read().decode('utf-8')))
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

    def mkdir(self, path: str, remote: SshHost = None):
        if remote:
            command = self._get_ssh_command(remote)
            command += shlex.quote('mkdir -p "{}"'.format(path))
        else:
            command = 'mkdir -p "{}"'.format(path)
        return self._execute(command, capture_output=False)

    def write_to_file(self, path: str, content: str, remote: SshHost = None):
        command = "echo '{}' | ".format(content)
        if remote:
            command += self._get_ssh_command(remote)
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
        return [line.decode('utf-8').strip() for line in sub_process.stdout.readlines()]

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

    def program_is_installed(self, program: str, remote: SshHost = None):
        if remote:
            command = self._get_ssh_command(remote)
            command += shlex.quote('which "{}"'.format(program))
        else:
            command = 'which "{}"'.format(program)

        try:
            self._execute(command, capture_output=True)
        except CommandExecutionError as e:
            print("Program '{}' not installed".format(program), file=sys.stderr)
            print(e.sub_process.stderr.read().decode('utf-8'), file=sys.stderr)
            sys.exit(1)

    def create_snapshot(self, source_dataset: str, next_snapshot: str):
        command = 'zfs snapshot "{}@{}"'.format(source_dataset, next_snapshot)
        return self._execute(command, capture_output=False)

    def get_snapshots(self, source_dataset: str) -> List[str]:
        command = "zfs list -H -o name -t snapshot"
        command += ' "{}"'.format(source_dataset)
        sub_process = self._execute(command, capture_output=True)
        return [line.decode('utf-8').strip().replace(source_dataset + "@", "")
                for line in sub_process.stdout.readlines()]

    def delete_snapshot(self, source_dataset: str, snapshot: str):
        command = 'zfs destroy "{}@{}"'.format(source_dataset, snapshot)
        return self._execute(command, capture_output=False)

    def get_estimated_snapshot_size(self, source_dataset: str, previous_snapshot: Optional[str], next_snapshot: str,
                                    include_intermediate_snapshots: bool = False):
        if previous_snapshot:
            command_output = self._execute('zfs send -n -P --raw {} "{}@{}" "{}@{}"'.format(
                "-I" if include_intermediate_snapshots else '-i',
                source_dataset, previous_snapshot, source_dataset, next_snapshot), capture_output=True
            ).stdout.read().decode('utf-8')
        else:
            command_output = self._execute('zfs send -n -P --raw "{}@{}"'.format(
                source_dataset, next_snapshot), capture_output=True
            ).stdout.read().decode('utf-8')

        for line in command_output.splitlines():
            if line.lower().startswith("size"):
                return int(line.replace("size", "").strip())
        raise ValueError("Could not determine snapshot size")

    def zfs_send_snapshot(self, source_dataset: str, previous_snapshot: Optional[str], next_snapshot: str,
                          target_paths: Set[str],
                          remote: SshHost = None,
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
                command = 'zfs send --raw {}@{}'.format(source_dataset, next_snapshot)

            command += ' | tee >( sha256sum -b > "{}" )'.format(tmp.name)
            command += ' | pv --force --rate --average-rate --bytes --timer --eta --size {}'.format(estimated_size)

            if remote:
                command += ' | ' + self._get_ssh_command(remote)
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

    def _get_checksum(self, output_dict: Dict[str, str], access_lock: threading.Lock,
                      source_dataset: str, next_snapshot: str, target_path: str, remote: SshHost = None):
        if remote:
            command = self._get_ssh_command(remote)
            checksum_command = 'pv --force --rate --average-rate --bytes --timer --name "{}" --cursor "{}"'.format(
                target_path, os.path.join(target_path, TARGET_SUBDIRECTORY, source_dataset,
                                          next_snapshot + BACKUP_FILE_POSTFIX))
            checksum_command += ' | sha256sum -b'
            command += shlex.quote(checksum_command)
        else:
            command = 'pv --force --rate --average-rate --bytes --timer --name "{}" --cursor "{}"'.format(
                target_path, os.path.join(target_path, TARGET_SUBDIRECTORY, source_dataset,
                                          next_snapshot + BACKUP_FILE_POSTFIX))
            command += ' | sha256sum -b'
        checksum = self._execute(command, capture_output=True, capture_stderr=False
                                 ).stdout.read().decode('utf-8').strip().split(' ')[0]
        with access_lock:
            output_dict[target_path] = checksum

    def get_checksums(self, source_dataset: str, next_snapshot: str, target_paths: Set[str], remote: SshHost = None):
        threads = []
        output_dict = {}
        access_lock = threading.Lock()
        for target_path in sorted(target_paths):
            thread = threading.Thread(target=self._get_checksum, args=(output_dict, access_lock,
                                                                       source_dataset, next_snapshot, target_path,
                                                                       remote), daemon=True)
            threads.append(thread)

        for thread in threads:
            thread.start()
            time.sleep(1)
        for thread in threads:
            thread.join()

        return output_dict

    def file_exists(self, path: str, remote: SshHost = None):
        if remote:
            command = self._get_ssh_command(remote)
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

    def list_directory(self, path: str, remote: SshHost = None) -> Tuple[List[str], List[str]]:
        if remote:
            command = self._get_ssh_command(remote)
            command += shlex.quote('ls -AF "{}"'.format(path))
        else:
            command = 'ls -AF "{}"'.format(path)
        sub_process = self._execute(command, capture_output=True)
        files = []
        directories = []
        for line in sub_process.stdout.readlines():
            line = line.decode('utf-8').strip()
            if line.endswith('/'):  # directory
                directories.append(line[:-1])
            elif line.endswith('*'):  # executable
                files.append(line[:-1])
            elif line[-1] not in ['@', '%', '|', '=', '>']:  # not other special files
                files.append(line)
        return files, directories

    def zfs_recv_snapshot(self, root_path: str, source_dataset: str, snapshot: str, target: str,
                          remote: SshHost = None) -> None:

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
        if remote:
            command = self._get_ssh_command(remote)
            command += shlex.quote(
                'pv --force --rate --average-rate --bytes --timer "{}"'.format(backup_path))
        else:
            command = 'pv --force --rate --average-rate --bytes --timer "{}"'.format(backup_path)
        command += ' | zfs recv -F "{}"'.format(os.path.join(root_path, source_dataset))

        self._execute(command, capture_output=False)


class ZfsBackupTool(object):
    # region argparse setup
    cli_parser = argparse.ArgumentParser(description='ZFS Backup Tool')
    cli_parser.add_argument('-c', '--config', type=str, required=True, help='Path to config file')
    cli_parser.add_argument('--debug', action='store_true', help='Debug output')
    cli_parser.add_argument('--version', action='version', version='%(prog)s 0.1')
    cli_parser.add_argument('-y', '--yes', action='store_true', help='Answer yes to all questions')
    subparsers = cli_parser.add_subparsers(dest='subparser_name')

    init_parser = subparsers.add_parser('init',
                                        help='Initialize backup targets and verify write access. '
                                             'Useful when using freshly formatted disks.',
                                        description='Initialize backup targets and verify write access. '
                                                    'Useful when using freshly formatted disks.')
    backup_parser = subparsers.add_parser('backup', help='Perform backup.', description='Perform backup.')
    backup_parser.add_argument('--new', action='store_true',
                               help='Perform new backup. '
                                    'Deletes all existing backup snapshots before regular backup process.')
    backup_parser.add_argument('--clean', action='store_true',
                               help='Removes all backup snapshots. Does not create new backup.')
    restore_parser = subparsers.add_parser('restore', help='Perform restore into given root path.',
                                           description='Perform restore into given root path.')
    restore_parser.add_argument('restore', type=str, help='Perform restore into given root path.')
    restore_parser.add_argument('-f', '--filter',
                                help='Perform restore only for datasets starting with given filter.')
    list_parser = subparsers.add_parser('list', help='List backup snapshots stored on targets.',
                                        description='List backup snapshots stored on targets.')
    list_parser.add_argument('--local', action='store_true',
                             help='List local backup snapshots which match the defined datasets in the given config.')
    list_parser.add_argument('--all', action='store_true',
                             help='List all stored backup snapshots stored on targets. '
                                  'When used with --local, lists all snapshots under matching datasets.')
    # endregion

    def __init__(self):
        # noinspection PyTypeChecker
        self.cli_args: argparse.Namespace = None
        # noinspection PyTypeChecker
        self.config: BackupSetup = None
        # noinspection PyTypeChecker
        self.shell_command: ShellCommand = None

    def run(self):
        self.cli_args = self.cli_parser.parse_args(sys.argv[1:])
        self.config = self._load_config(self.cli_args.config)
        self.shell_command = ShellCommand(echo_cmd=self.cli_args.debug)
        self.do_check_programs_installed()
        if self.cli_args.subparser_name is None:
            self.cli_parser.print_help()
            sys.exit(1)
        if self.cli_args.subparser_name == 'list':
            self.do_list()
        if self.cli_args.subparser_name == 'init':
            self.do_init()
        if self.cli_args.subparser_name == 'backup':
            self.do_backup()
        if self.cli_args.subparser_name == 'restore':
            self.do_restore()

    def do_check_programs_installed(self):
        for program in ["ssh", "zfs", "pv", "sha256sum"]:
            self.shell_command.program_is_installed(program)
        for program in ["pv", "sha256sum"]:
            self.shell_command.program_is_installed(program, self.config.remote)

    def _get_stored_snapshots_from_targets(self) -> Dict[str, List[str]]:
        collected_snapshot_paths = set()
        for target in self.config.get_all_target_paths():
            existing_snapshot_paths = set()
            if not self.shell_command.file_exists(os.path.join(target, TARGET_SUBDIRECTORY, INITIALIZED_FILE_NAME),
                                                  self.config.remote):
                print("Target {} is not initialized".format(target))
                print("skipping...")
                sys.exit(1)
            self._get_file_paths(target, os.path.join(target, TARGET_SUBDIRECTORY), BACKUP_FILE_POSTFIX,
                                 existing_snapshot_paths)

            if not existing_snapshot_paths:
                print("No backups found on target {}".format(target))
                continue

            collected_snapshot_paths.update(existing_snapshot_paths)

        if not collected_snapshot_paths:
            print("No backups found on any target!")
            sys.exit(1)

        grouped_snapshots = self._group_snapshots_by_source(collected_snapshot_paths, BACKUP_FILE_POSTFIX)
        return grouped_snapshots

    def _filter_backup_datasets_by_config(self, datasets: List[str]) -> List[str]:
        matching_datasets = []
        for source in self.config.sources:
            for dataset in datasets:
                if source.recursive:
                    if not any(dataset.startswith(s) for s in source.zfs_source):
                        continue
                else:
                    if dataset not in source.zfs_source:
                        continue
                matching_datasets.append(dataset)
        return matching_datasets

    def do_list(self):
        if self.cli_args.local:
            datasets = []
            for source in self.config.sources:
                for config_source_dataset in source.zfs_source:
                    if not self.shell_command.has_dataset(config_source_dataset):
                        print("Source dataset {} defined in '{}' does not exist".format(
                            config_source_dataset, source.name))
                        print("Aborting...")
                        exit(1)
                    selected_source_datasets = self.shell_command.get_datasets(config_source_dataset, source.recursive)
                    datasets.extend(selected_source_datasets)
            for dataset in sorted(datasets):
                dataset_snapshots = self.shell_command.get_snapshots(dataset)
                if not self.cli_args.all:
                    dataset_snapshots = self._filter_backup_snapshots(dataset_snapshots, sort=True)
                for snapshot in dataset_snapshots:
                    print("{}@{}".format(dataset, snapshot))
        else:
            grouped_snapshots = self._get_stored_snapshots_from_targets()
            selected_datasets = list(grouped_snapshots.keys())
            if not self.cli_args.all:
                selected_datasets = self._filter_backup_datasets_by_config(selected_datasets)
            for dataset in sorted(selected_datasets):
                for snapshot in grouped_snapshots[dataset]:
                    print("{}@{}".format(dataset, snapshot))

    def do_init(self):
        if not self.cli_args.yes:
            print("Please verify mountpoints are correct:")
            for target in self.config.get_all_target_paths():
                print("  {}".format(target))
            if input("Continue? [y/N] ").lower() != 'y':
                print("Aborting...")
                return
        print("Initializing backup targets...")
        for target in self.config.get_all_target_paths():
            self.shell_command.mkdir(os.path.join(target, TARGET_SUBDIRECTORY), self.config.remote)
            self.shell_command.write_to_file(os.path.join(target, TARGET_SUBDIRECTORY, INITIALIZED_FILE_NAME),
                                             "initialized", self.config.remote)

    def _filter_backup_snapshots(self, snapshots: List[str], sort=True) -> List[str]:
        matching_snapshots = [snapshot for snapshot in snapshots
                              if snapshot.startswith(self.config.snapshot_prefix)]

        if sort:
            ordered_snapshots = []
            for snapshot in sorted(matching_snapshots):
                if snapshot.endswith(SNAPSHOT_PREFIX_POSTFIX_SEPARATOR + INITIAL_SNAPSHOT_POSTFIX):
                    ordered_snapshots.insert(0, snapshot)
                else:
                    ordered_snapshots.append(snapshot)
            return ordered_snapshots
        else:
            return matching_snapshots

    def _has_initial_backup_snapshot(self, snapshots: List[str]) -> bool:
        for snapshot in snapshots:
            if snapshot.startswith(self.config.snapshot_prefix) and snapshot.endswith(
                    SNAPSHOT_PREFIX_POSTFIX_SEPARATOR + INITIAL_SNAPSHOT_POSTFIX):
                return True
        return False

    def _get_highest_snapshot_number(self, snapshots: List[str]) -> int:
        highest_snapshot_number = 0  # next snapshot after initial snapshot is always 1
        for snapshot in snapshots:
            if snapshot.startswith(self.config.snapshot_prefix):
                if snapshot.endswith(SNAPSHOT_PREFIX_POSTFIX_SEPARATOR + INITIAL_SNAPSHOT_POSTFIX):
                    continue
                snapshot_number = int(snapshot.split(SNAPSHOT_PREFIX_POSTFIX_SEPARATOR)[-1])
                if snapshot_number > highest_snapshot_number:
                    highest_snapshot_number = snapshot_number
        return highest_snapshot_number

    def _has_intermediate_backup_snapshots(self, snapshots: List[str]) -> bool:
        return self._get_highest_snapshot_number(snapshots) > 0

    def _get_next_snapshot_name(self, snapshots: List[str]) -> Tuple[str, str]:
        highest_snapshot_number = self._get_highest_snapshot_number(snapshots)
        # next snapshot after initial snapshot is always 1, 1 gets added later
        if highest_snapshot_number == 0:
            previous_snapshot = (self.config.snapshot_prefix
                                 + SNAPSHOT_PREFIX_POSTFIX_SEPARATOR
                                 + INITIAL_SNAPSHOT_POSTFIX)
        else:
            previous_snapshot = (self.config.snapshot_prefix
                                 + SNAPSHOT_PREFIX_POSTFIX_SEPARATOR
                                 + str(highest_snapshot_number))
        next_snapshot = (self.config.snapshot_prefix
                         + SNAPSHOT_PREFIX_POSTFIX_SEPARATOR
                         + str(highest_snapshot_number + 1))
        return previous_snapshot, next_snapshot

    def _do_backup(self, target_paths: Set[str], source_dataset: str, previous_snapshot: str, next_snapshot: str):
        for target_path in target_paths:
            self.shell_command.mkdir(os.path.join(target_path, TARGET_SUBDIRECTORY, source_dataset),
                                     self.config.remote)
        backup_checksum = self.shell_command.zfs_send_snapshot(
            source_dataset, previous_snapshot, next_snapshot, target_paths,
            self.config.remote, self.config.include_intermediate_snapshots)
        print("Created backup snapshot {}@{} with checksum {}".format(
            source_dataset, next_snapshot, backup_checksum))
        print("Verifying written backups...")
        read_checksums = self.shell_command.get_checksums(
            source_dataset, next_snapshot, target_paths, self.config.remote)
        for target_path, read_checksum in read_checksums.items():
            if read_checksum != backup_checksum:
                print("Checksum mismatch for backup {}@{} on target {}".format(
                    source_dataset, next_snapshot, target_path))
                print("Expected checksum: {}".format(backup_checksum))
                print("Read checksum: {}".format(read_checksum))
                print("Aborting...")
                sys.exit(1)
            else:
                print("Checksum verified for backup {}@{} on target {}".format(
                    source_dataset, next_snapshot, target_path))

        for target_path in target_paths:
            self.shell_command.write_to_file(
                os.path.join(target_path, TARGET_SUBDIRECTORY, source_dataset,
                             next_snapshot + BACKUP_FILE_POSTFIX + CHECKSUM_FILE_POSTFIX),
                "{} ./{}".format(backup_checksum, next_snapshot + BACKUP_FILE_POSTFIX),
                self.config.remote)

    def _do_recreate_missing_backups(self, source_dataset: str, source_dataset_snapshots: List[str],
                                     target_paths: Set[str]):
        sorted_existing_backup_snapshots = self._filter_backup_snapshots(source_dataset_snapshots, sort=True)
        for i, snapshot in enumerate(sorted_existing_backup_snapshots):
            incpmlete_targets = set()
            for target_path in target_paths:
                if not self.shell_command.file_exists(
                        os.path.join(target_path, TARGET_SUBDIRECTORY, source_dataset,
                                     snapshot + BACKUP_FILE_POSTFIX + CHECKSUM_FILE_POSTFIX),
                        self.config.remote):
                    incpmlete_targets.add(target_path)
            if incpmlete_targets:
                if i == 0 and snapshot.endswith(SNAPSHOT_PREFIX_POSTFIX_SEPARATOR + INITIAL_SNAPSHOT_POSTFIX):
                    previous_snapshot = None
                    next_snapshot = snapshot
                elif i == 0 and not snapshot.endswith(SNAPSHOT_PREFIX_POSTFIX_SEPARATOR + INITIAL_SNAPSHOT_POSTFIX):
                    print("Cannot recreate missing backup {}@{} on target(s) {} because intermediate source snapshot "
                          "is missing".format(source_dataset, snapshot, ", ".join(incpmlete_targets)))
                    if not self.cli_args.yes:
                        if input("Continue? [y/N] ").lower() != 'y':
                            print("Aborting...")
                            exit(1)
                    continue
                elif i > 0:
                    previous_snapshot = sorted_existing_backup_snapshots[i - 1]
                    next_snapshot = snapshot
                else:
                    raise ValueError("Unexpected state")
                print("Recreating missing backup {}@{} on target(s) {}".format(source_dataset, snapshot,
                                                                               ", ".join(incpmlete_targets)))
                self._do_backup(incpmlete_targets, source_dataset, previous_snapshot, next_snapshot)
            else:
                print("Backup {}@{} is complete on all targets".format(source_dataset, snapshot))

    def do_backup(self):
        selected_datasets: List[Tuple[str, BackupSource]] = []
        # region check if all source datasets exist, are not selected twice and map them into selected_datasets
        for source in self.config.sources:
            for config_source_dataset in source.zfs_source:
                if not self.shell_command.has_dataset(config_source_dataset):
                    print("Source dataset {} defined in '{}' does not exist".format(
                        config_source_dataset, source.name))
                    print("Aborting...")
                    exit(1)
                selected_source_datasets = self.shell_command.get_datasets(config_source_dataset, source.recursive)
                overlapping_datasets = set(d for d, _ in selected_datasets).intersection(set(selected_source_datasets))
                if overlapping_datasets:
                    print("Source dataset(s) {} defined in '{}' overlap with already selected datasets".format(
                        ", ".join(overlapping_datasets), source.name))
                    print("Aborting...")
                    exit(1)
                selected_datasets.extend(((d, source) for d in selected_source_datasets))
        # endregion

        # region iter all selected datasets and perform backup
        for source_dataset, source in selected_datasets:
            source_dataset_snapshots = self.shell_command.get_snapshots(source_dataset)
            # reset snapshots if new backup is requested or clean is requested
            if self.cli_args.new or self.cli_args.clean:
                for snapshot in self._filter_backup_snapshots(source_dataset_snapshots):
                    print("Deleting snapshot {}@{}...".format(source_dataset, snapshot))
                    self.shell_command.delete_snapshot(source_dataset, snapshot)
                if self.cli_args.clean:
                    # abort further processing if clean is requested
                    continue
                source_dataset_snapshots = self.shell_command.get_snapshots(source_dataset)

            # recreate missing/aborted backup snapshots
            self._do_recreate_missing_backups(source_dataset, source_dataset_snapshots,
                                              source.get_all_target_paths())

            # detect previous and next snapshot
            if (not self._has_initial_backup_snapshot(source_dataset_snapshots)
                    and not self._has_intermediate_backup_snapshots(source_dataset_snapshots)):
                print("No initial snapshot found for dataset {}".format(source_dataset))
                print("Creating initial snapshot...")
                previous_snapshot = None
                next_snapshot = (self.config.snapshot_prefix
                                 + SNAPSHOT_PREFIX_POSTFIX_SEPARATOR
                                 + INITIAL_SNAPSHOT_POSTFIX)
            else:
                if self._has_initial_backup_snapshot(source_dataset_snapshots):
                    pass
                elif not self._has_intermediate_backup_snapshots(source_dataset_snapshots):
                    print("No intermediate backup snapshots found for dataset {}".format(source_dataset))
                    print("Cannot proceed without intermediate backup snapshots")
                    print("Aborting...")
                    sys.exit(1)
                previous_snapshot, next_snapshot = self._get_next_snapshot_name(source_dataset_snapshots)

            # create new snapshot and perform backup
            print("Creating snapshot {}@{}...".format(source_dataset, next_snapshot))
            self.shell_command.create_snapshot(source_dataset, next_snapshot)
            self._do_backup(source.get_all_target_paths(), source_dataset, previous_snapshot, next_snapshot)
        # endregion

    def _get_file_paths(self, target: str, path: str, file_postfix: str, collected_files: Set[str] = None) -> Set[str]:
        if collected_files is None:
            collected_files = set()
        files, directories = self.shell_command.list_directory(path, self.config.remote)
        for file in files:
            if file.endswith(file_postfix):
                collected_files.add(
                    os.path.join(path, file).replace(os.path.join(target, TARGET_SUBDIRECTORY) + os.path.sep, ''))
        for directory in directories:
            self._get_file_paths(target, os.path.join(path, directory), file_postfix, collected_files)
        return collected_files

    def _group_snapshots_by_source(self, snapshot_paths: Set[str], file_postfix: str) -> Dict[str, List[str]]:
        grouped_snapshots = {}
        for snapshot_path in snapshot_paths:
            source_dataset, snapshot = snapshot_path.rsplit(os.path.sep, 1)
            snapshot = snapshot.replace(file_postfix, '')
            if source_dataset not in grouped_snapshots:
                grouped_snapshots[source_dataset] = []
            grouped_snapshots[source_dataset].append(snapshot)
        for source_dataset in grouped_snapshots:
            grouped_snapshots[source_dataset] = self._filter_backup_snapshots(
                grouped_snapshots[source_dataset], sort=True)
        return grouped_snapshots

    def _do_restore_into_target(self, source_dataset: str, snapshots: List[str], root_path: str, targets: Set[str]):

        snapshot_sources = {}
        for snapshot_i, snapshot in enumerate(snapshots):
            snapshot_sources[snapshot] = set()
            if snapshot_i == 0 and not snapshot.endswith(SNAPSHOT_PREFIX_POSTFIX_SEPARATOR + INITIAL_SNAPSHOT_POSTFIX):
                print("Cannot restore {} under {} because initial snapshot is missing".format(
                    source_dataset, root_path))
                print("Aborting...")
                exit(1)
            for target in targets:
                backup_file = os.path.join(target, TARGET_SUBDIRECTORY, source_dataset,
                                           snapshot + BACKUP_FILE_POSTFIX)
                checksum_file = backup_file + CHECKSUM_FILE_POSTFIX
                if not self.shell_command.file_exists(backup_file, self.config.remote):
                    print("Cannot use {} as a restore source for {}@{} because it has no backup file".format(
                        target, source_dataset, snapshot))
                elif not self.shell_command.file_exists(checksum_file, self.config.remote):
                    print("Cannot use {} as a restore source for {}@{} because it has no checksum file".format(
                        target, source_dataset, snapshot))
                else:
                    snapshot_sources[snapshot].add(target)

            if not snapshot_sources[snapshot]:
                print("Cannot restore {}@{} because it has no backup on any target".format(
                    source_dataset, snapshot))
                print("Aborting...")
                exit(1)

        restored_snapshot_names = []
        for snapshot_i, snapshot in enumerate(snapshots):
            if snapshot in restored_snapshot_names:
                continue

            if self.shell_command.has_dataset(os.path.join(root_path, source_dataset)):
                current_snapshots = self.shell_command.get_snapshots(os.path.join(root_path, source_dataset))
            else:
                current_snapshots = []
            if snapshot in current_snapshots:
                print("Restoring {}@{} under {} skipped because it already exists".format(
                    source_dataset, snapshot, root_path))
                restored_snapshot_names.append(snapshot)
                continue

            for target_i, target in enumerate(snapshot_sources[snapshot]):
                print("Restoring {}@{} under {} from {}".format(source_dataset, snapshot, root_path, target))
                try:
                    self.shell_command.zfs_recv_snapshot(root_path, source_dataset, snapshot, target,
                                                         self.config.remote)
                except CommandExecutionError as e:
                    print("Error restoring {}@{} under {} from {}".format(source_dataset, snapshot, root_path, target))
                    if target_i + 1 < len(snapshot_sources[snapshot]):
                        print("Trying next target...")
                        continue
                    else:
                        print("all targets failed")
                        print("Aborting...")
                        exit(1)

                restored_snapshot_names.append(snapshot)
                break

        current_snapshots = self.shell_command.get_snapshots(os.path.join(root_path, source_dataset))
        if len(restored_snapshot_names) == len(snapshots) and set(restored_snapshot_names) == set(current_snapshots):
            print("Successfully restored {} under {}".format(source_dataset, root_path))
            return
        else:
            print("Failed to restore {} under {}".format(source_dataset, root_path))
            print("Only restored snapshots: {}".format(restored_snapshot_names))
            print("Missing snapshots: {}".format(set(snapshots) - set(restored_snapshot_names)))
            print("Aborting...")
            exit(1)

    def do_restore(self):
        if not self.shell_command.has_dataset(self.cli_args.restore):
            print("Root path {} does not exist".format(self.cli_args.restore))
            print("Aborting...")
            exit(1)

        grouped_snapshots = self._get_stored_snapshots_from_targets()

        if self.cli_args.filter:
            for dataset in sorted(grouped_snapshots.keys()):
                if self.cli_args.filter and self.cli_args.filter not in dataset:
                    continue
                self._do_restore_into_target(dataset, grouped_snapshots[dataset],
                                             self.cli_args.restore, self.config.get_all_target_paths())
        else:
            for source in self.config.sources:
                for dataset in sorted(grouped_snapshots.keys()):
                    if source.recursive:
                        if not any(dataset.startswith(s) for s in source.zfs_source):
                            continue
                    else:
                        if dataset not in source.zfs_source:
                            continue
                    self._do_restore_into_target(dataset, grouped_snapshots[dataset],
                                                 self.cli_args.restore, source.get_all_target_paths())

    def _itemize_option(self, option_content: Optional[str]) -> List[str]:
        if not option_content:
            return []
        items = []
        lines = [line.replace('\r\n', '').replace('\n', '').strip()
                 for line in option_content.splitlines()]
        for line in lines:
            items.extend(i.strip() for i in line.split(','))
        return items

    def _load_config(self, path: str) -> BackupSetup:
        parser = configparser.ConfigParser(interpolation=EnvInterpolation(),
                                           converters={'list': lambda x: [i.strip() for i in x.split(',')]}
                                           )
        parser.read(path)
        # parse optional remote section
        remote = None
        for section in parser.sections():
            if section.lower() == 'remote':
                remote = SshHost(parser.get(section, 'host'),
                                 parser.get(section, 'user', fallback=None),
                                 parser.getint(section, 'port', fallback=None),
                                 parser.get(section, 'key_path', fallback=None))

        # parse general settings
        snapshot_prefix = None
        include_intermediate_snapshots = False
        for section in parser.sections():
            if section.lower() == 'general':
                snapshot_prefix = parser.get(section, 'snapshot_prefix', fallback=None)
                include_intermediate_snapshots = parser.getboolean(section, 'include_intermediate_snapshots',
                                                                   fallback=False)
                break

        # parse TargetGroup sections
        target_groups = {}
        for section in parser.sections():
            if section.lower().startswith('target-group') or section.lower().startswith('targetgroup'):
                target_group = TargetGroup(self._itemize_option(parser.get(section, 'path')),
                                           section)
                target_groups[section] = target_group
                if section.lower().startswith('target-group'):
                    target_groups[section[len("target-group"):].strip()] = target_group
                elif section.lower().startswith('targetgroup'):
                    target_groups[section[len("targetgroup"):].strip()] = target_group

        # parse BackupSource sections
        backup_sources = []
        for section in parser.sections():
            if section.lower().startswith('source'):
                targets = self._itemize_option(parser.get(section, 'target'))
                for target in targets:
                    if target not in target_groups:
                        raise ValueError("TargetGroup '{}' not defined".format(target))
                sources = self._itemize_option(parser.get(section, 'source'))
                for source in sources:
                    if '@' in source:
                        raise ValueError("ZFS source '{}' contains '@', sources must not aim at snapshots!".format(
                            source))
                backup_sources.append(BackupSource(section,
                                                   sources,
                                                   [target_groups[t] for t in targets],
                                                   parser.getboolean(section, 'recursive'),
                                                   self._itemize_option(parser.get(section, 'exclude', fallback=None)),
                                                   self._itemize_option(parser.get(section, 'include', fallback=None)),
                                                   ))

        return BackupSetup(backup_sources, remote, snapshot_prefix, include_intermediate_snapshots)


if __name__ == "__main__":
    app = ZfsBackupTool()
    app.run()
