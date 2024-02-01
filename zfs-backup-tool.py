import argparse
import configparser
import os.path
import sys
from os.path import expandvars
from typing import List, Set, Optional, Tuple, Dict

from ZfsBackupTool.BackupSetup import BackupSetup
from ZfsBackupTool.BackupSource import BackupSource
from ZfsBackupTool.Constants import *
from ZfsBackupTool.DataSet import DataSet
from ZfsBackupTool.ShellCommand import ShellCommand, CommandExecutionError
from ZfsBackupTool.SshHost import SshHost
from ZfsBackupTool.TargetDataSet import TargetDataSet
from ZfsBackupTool.TargetGroup import TargetGroup


# region setup helper class for environment expanding in config file
class EnvInterpolation(configparser.BasicInterpolation):
    """Interpolation which expands environment variables in values."""

    def before_get(self, parser, section, option, value, defaults):
        value = super().before_get(parser, section, option, value, defaults)
        return expandvars(value)


# endregion


class ZfsBackupTool(object):
    # region argparse setup
    cli_parser = argparse.ArgumentParser(description='ZFS Backup Tool is a tool for performing ZFS backups.\n'
                                                     'It creates incremental backups and transmits them to one or more '
                                                     'target-disks on a local or remote machine.\n'
                                                     'Written files are verified using checksums.\n'
                                                     'It can also restore backups from one or more target-disks.\n'
                                                     'Usage steps:\n'
                                                     '  1. Create a config file.\n'
                                                     '  2. Initialize backup targets and verify write access.\n'
                                                     '  3. Perform backup.\n'
                                                     '  4. Restore backup.',
                                         formatter_class=argparse.RawTextHelpFormatter
                                         )
    cli_parser.add_argument('config', type=str, help='Path to config file')
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
    backup_parser.add_argument('--missing', action='store_true',
                               help='Re-create backups only for missing snapshots. '
                                    'Skips creation of a new incremental backup. '
                                    'Useful for resuming previously aborted backup runs.')
    backup_parser.add_argument('--skip-repaired-datasets', action='store_true',
                               help='Will not create new backup snapshots on a dataset if the incremental base was '
                                    'missing and re-created on any target. '
                                    'Useful for setups where a dataset was already backed up using a different '
                                    'config file previously. '
                                    'This occurs when the same dataset is backed up by two different config files. '
                                    'Use this option if this double referenced dataset should not get an additional '
                                    'backup snapshot during the second backup-run.')
    backup_parser.add_argument('--target-filter',
                               help='Perform backup only to targets starting with given filter.')
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
                             help='List all stored datasets and matching snapshots on all targets.'
                                  'When used with --local, lists all snapshots under matching datasets.')

    # endregion

    def __init__(self):
        self.cli_args: argparse.Namespace = None  # type: ignore
        self.config: BackupSetup = None  # type: ignore
        self.shell_command: ShellCommand = None  # type: ignore

    def run(self):
        self.cli_args = self.cli_parser.parse_args(sys.argv[1:])
        self.shell_command = ShellCommand(echo_cmd=self.cli_args.debug)
        self.config, global_remote = self._load_config(self.cli_args.config)
        self.shell_command.set_remote_host(global_remote)
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
            self.shell_command.program_is_installed(program, remote=None)
        for program in ["pv", "sha256sum"]:
            self.shell_command.program_is_installed(program)

    def do_list(self):
        if self.cli_args.local:
            for source in self.config.sources:
                invalid_sources = source.invalid_zfs_sources()
                if invalid_sources:
                    for invalid_source in invalid_sources:
                        print("Source dataset {} defined in '{}' does not exist".format(
                            invalid_source, source.name))
                        print("Aborting...")
                    exit(1)

                for dataset in source.get_matching_datasets():
                    if self.cli_args.all:
                        dataset_snapshots = dataset.get_snapshots()
                    else:
                        dataset_snapshots = dataset.get_backup_snapshots(self.config.snapshot_prefix)
                    for snapshot in dataset_snapshots:
                        print("{}@{}".format(dataset.zfs_path, snapshot))
        else:
            found_datasets: Dict[str, TargetDataSet] = {}
            for source in self.config.sources:
                if self.cli_args.all:
                    remote_datasets = source.get_available_target_datasets()
                else:
                    remote_datasets = source.get_matching_target_datasets()
                found_datasets.update({d.zfs_path: d for d in remote_datasets})
            for zfs_path in sorted(found_datasets.keys()):
                if self.cli_args.all:
                    snapshots = found_datasets[zfs_path].get_snapshots()
                else:
                    snapshots = found_datasets[zfs_path].get_backup_snapshots(self.config.snapshot_prefix)
                for snapshot in snapshots:
                    print("{}@{}".format(zfs_path, snapshot))

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
            self.shell_command.target_mkdir(os.path.join(target, TARGET_SUBDIRECTORY))
            self.shell_command.target_write_to_file(os.path.join(target, TARGET_SUBDIRECTORY, INITIALIZED_FILE_NAME),
                                                    "initialized")

    def _do_backup(self, target_paths: Set[str], source_dataset: str,
                   previous_snapshot: Optional[str], next_snapshot: str,
                   overwrite: bool = True):
        for target_path in target_paths:
            self.shell_command.target_mkdir(os.path.join(target_path, TARGET_SUBDIRECTORY, source_dataset))

        skip_zfs_send = False
        skip_verification = False
        backup_checksum = None

        if not overwrite:
            remotes_have_snapshot_file = (
                self.shell_command.target_file_exists(
                    os.path.join(tp, TARGET_SUBDIRECTORY, source_dataset,
                                 next_snapshot + BACKUP_FILE_POSTFIX))
                for tp in target_paths
            )
            if all(remotes_have_snapshot_file):
                remotes_have_checksum_file = (
                    self.shell_command.target_file_exists(
                        os.path.join(tp, TARGET_SUBDIRECTORY, source_dataset,
                                     next_snapshot + BACKUP_FILE_POSTFIX + CHECKSUM_FILE_POSTFIX))
                    for tp in target_paths
                )
                if all(remotes_have_checksum_file):
                    print("Backup {}@{} already exists on all targets and checksums were written.".format(
                        source_dataset, next_snapshot))
                    skip_zfs_send = True
                    skip_verification = True
                else:
                    print("Unusual State detected: checksum file missing on some targets.")

                    remotes_have_temporary_checksum_file = (
                        self.shell_command.target_file_exists(
                            os.path.join(tp, TARGET_SUBDIRECTORY, source_dataset,
                                         next_snapshot + BACKUP_FILE_POSTFIX + EXPECTED_CHECKSUM_FILE_POSTFIX))
                        for tp in target_paths
                    )
                    if any(remotes_have_temporary_checksum_file):
                        for target_path in target_paths:
                            try:
                                backup_checksum = self.shell_command.target_read_checksum_from_file(
                                    os.path.join(target_path, TARGET_SUBDIRECTORY, source_dataset,
                                                 next_snapshot + BACKUP_FILE_POSTFIX + EXPECTED_CHECKSUM_FILE_POSTFIX))
                            except CommandExecutionError:
                                pass
                            else:
                                print('Found checksum for backup "{}@{}" on target "{}": {}'.format(
                                    source_dataset, next_snapshot, target_path, backup_checksum))
                                break
                        if backup_checksum:
                            print('Skipping re-writing of backup "{}@{}" because it already exists on all targets and '
                                  'a checksum was found.'.format(source_dataset, next_snapshot))
                            skip_zfs_send = True
            else:
                print("Unusual State detected: Backup {}@{} does not exist on all targets.".format(
                    source_dataset, next_snapshot))

        if not skip_zfs_send:
            print("Transmitting backup snapshot {}@{} to target(s): {}...".format(
                source_dataset, next_snapshot, ", ".join(sorted(target_paths))))
            backup_checksum = self.shell_command.zfs_send_snapshot_to_target(
                source_dataset, previous_snapshot, next_snapshot, target_paths,
                self.config.include_intermediate_snapshots)
            print("Created backup snapshot {}@{} with checksum {}".format(
                source_dataset, next_snapshot, backup_checksum))

            # after transmission, write checksum to temporary file on target
            # it gets replaced later by the 'final' checksum file
            for target_path in target_paths:
                self.shell_command.target_write_to_file(
                    os.path.join(target_path, TARGET_SUBDIRECTORY, source_dataset,
                                 next_snapshot + BACKUP_FILE_POSTFIX + EXPECTED_CHECKSUM_FILE_POSTFIX),
                    "{} ./{}".format(backup_checksum, next_snapshot + BACKUP_FILE_POSTFIX))
        else:
            assert backup_checksum

        if not skip_verification:
            print("Verifying written backups...")
            read_checksums = self.shell_command.target_get_checksums(source_dataset, next_snapshot, target_paths)
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
                self.shell_command.target_write_to_file(
                    os.path.join(target_path, TARGET_SUBDIRECTORY, source_dataset,
                                 next_snapshot + BACKUP_FILE_POSTFIX + CHECKSUM_FILE_POSTFIX),
                    "{} ./{}".format(backup_checksum, next_snapshot + BACKUP_FILE_POSTFIX))

        for target_path in target_paths:
            self.shell_command.target_remove_file(
                os.path.join(target_path, TARGET_SUBDIRECTORY, source_dataset,
                             next_snapshot + BACKUP_FILE_POSTFIX + EXPECTED_CHECKSUM_FILE_POSTFIX))

    def _do_recreate_missing_backups(self, source_dataset: str, source_dataset_snapshots: List[str],
                                     target_paths: Set[str]) -> List[str]:
        recreated_snapshots: List[str] = []
        sorted_existing_backup_snapshots = DataSet.filter_backup_snapshots(source_dataset_snapshots,
                                                                           self.config.snapshot_prefix)
        for i, snapshot in enumerate(sorted_existing_backup_snapshots):
            incomplete_targets = set()
            # find missing/incomplete backups, backups are complete if they exist and have a checksum file
            for target_path in target_paths:
                if not (self.shell_command.target_file_exists(
                        os.path.join(target_path, TARGET_SUBDIRECTORY, source_dataset,
                                     snapshot + BACKUP_FILE_POSTFIX))
                        and self.shell_command.target_file_exists(
                            os.path.join(target_path, TARGET_SUBDIRECTORY, source_dataset,
                                         snapshot + BACKUP_FILE_POSTFIX + CHECKSUM_FILE_POSTFIX))):
                    incomplete_targets.add(target_path)
            # process incomplete targets
            if incomplete_targets:
                if i == 0 and snapshot.endswith(SNAPSHOT_PREFIX_POSTFIX_SEPARATOR + INITIAL_SNAPSHOT_POSTFIX):
                    previous_snapshot = None
                    next_snapshot = snapshot
                elif i == 0 and not snapshot.endswith(SNAPSHOT_PREFIX_POSTFIX_SEPARATOR + INITIAL_SNAPSHOT_POSTFIX):
                    print("Cannot recreate missing backup {}@{} on target(s) {} because intermediate source snapshot "
                          "is missing".format(source_dataset, snapshot, ", ".join(incomplete_targets)))
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
                                                                               ", ".join(incomplete_targets)))
                self._do_backup(incomplete_targets, source_dataset, previous_snapshot, next_snapshot, overwrite=False)
                recreated_snapshots.append(snapshot)
            else:
                print("Backup {}@{} is complete on all targets".format(source_dataset, snapshot))
        return recreated_snapshots

    def do_backup(self):

        print("Checking source datasets...")

        # region check if all source datasets exist, are not selected twice and map them into selected_datasets
        selected_datasets: Dict[str, DataSet] = {}
        for source in self.config.sources:
            # if not self.shell_command.has_dataset(config_source_dataset):
            invalid_sources = source.invalid_zfs_sources()
            if invalid_sources:
                for invalid_source in invalid_sources:
                    print("Source dataset {} defined in '{}' does not exist".format(
                        invalid_source, source.name))
                    print("Aborting...")
                exit(1)
            selected_source_datasets = {d.zfs_path: d for d in source.get_matching_datasets()}
            overlapping_datasets = set(selected_datasets.keys()).intersection(set(selected_source_datasets.keys()))
            if overlapping_datasets:
                print("Source dataset(s) {} defined in '{}' overlap with already selected datasets".format(
                    ", ".join(overlapping_datasets), source.name))
                print("Aborting...")
                exit(1)
            selected_datasets.update(selected_source_datasets)
        # endregion

        # region reset snapshots if requested
        if self.cli_args.new or self.cli_args.clean:
            for source in self.config.sources:
                for dataset in source.get_matching_datasets():
                    for snapshot in dataset.get_backup_snapshots(self.config.snapshot_prefix):
                        print("Deleting snapshot {}@{}...".format(dataset.zfs_path, snapshot))
                        dataset.delete_snapshot(snapshot)
                    dataset.invalidate_caches()
            if self.cli_args.clean:
                # abort further processing if clean is requested
                return
        # endregion

        # region recreate missing/aborted backup snapshots
        recreated_snapshots: Dict[str, List[str]] = {}
        if not self.cli_args.new:
            for source in self.config.sources:
                for dataset in source.get_matching_datasets():
                    recreated = self._do_recreate_missing_backups(
                        dataset.zfs_path,
                        dataset.get_backup_snapshots(self.config.snapshot_prefix),
                        source.get_all_target_paths(self.cli_args.target_filter))
                    recreated_snapshots[dataset.zfs_path] = recreated
            if self.cli_args.missing:
                # recreate missing/aborted backup snapshots, then exit
                return
        # endregion

        # region create new snapshots as bulk operation
        dataset_snapshot_names: Dict[str, Tuple[Optional[str], str]] = {}
        for source in self.config.sources:
            for dataset in source.get_matching_datasets():
                if (not dataset.has_initial_backup_snapshot(self.config.snapshot_prefix)
                        and not dataset.has_intermediate_backup_snapshots(self.config.snapshot_prefix)):
                    print("No initial snapshot found for dataset {}".format(dataset.zfs_path))
                    print("Creating initial snapshot...")
                    previous_snapshot = None
                    next_snapshot = (self.config.snapshot_prefix
                                     + SNAPSHOT_PREFIX_POSTFIX_SEPARATOR
                                     + INITIAL_SNAPSHOT_POSTFIX)
                else:
                    if dataset.has_initial_backup_snapshot(self.config.snapshot_prefix):
                        pass
                    elif not dataset.has_intermediate_backup_snapshots(self.config.snapshot_prefix):
                        print("No intermediate backup snapshots found for dataset {}".format(dataset.zfs_path))
                        print("Cannot proceed without intermediate backup snapshots")
                        print("Aborting...")
                        sys.exit(1)
                    previous_snapshot, next_snapshot = dataset.get_next_snapshot_name(self.config.snapshot_prefix)

                recreated = recreated_snapshots[dataset.zfs_path] if dataset.zfs_path in recreated_snapshots else []

                if previous_snapshot in recreated and self.cli_args.skip_repaired_datasets:
                    print("Skipping creation of new incremental backup for {} because snapshots were re-created on a "
                          "target".format(dataset.zfs_path))
                    continue
                else:
                    # create new snapshot and perform backup
                    print("Creating snapshot {}@{}...".format(dataset.zfs_path, next_snapshot))
                    dataset.create_snapshot(next_snapshot)
                    dataset_snapshot_names[dataset.zfs_path] = (previous_snapshot, next_snapshot)

        # endregion

        # region transmit backup as bulk operation
        for source in self.config.sources:
            for dataset in source.get_matching_datasets():
                if dataset.zfs_path in dataset_snapshot_names:
                    previous_snapshot, next_snapshot = dataset_snapshot_names[dataset.zfs_path]
                    self._do_backup(source.get_all_target_paths(self.cli_args.target_filter),
                                    dataset.zfs_path, previous_snapshot, next_snapshot)
        # endregion

    def _do_restore_into_target(self, source_dataset: str, snapshots: List[str], root_path: str, targets: Set[str]):

        snapshot_sources: Dict[str, Set[str]] = {}
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
                if not self.shell_command.target_file_exists(backup_file):
                    print("Cannot use {} as a restore source for {}@{} because it has no backup file".format(
                        target, source_dataset, snapshot))
                elif not self.shell_command.target_file_exists(checksum_file):
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
                    self.shell_command.zfs_recv_snapshot_from_target(root_path, source_dataset, snapshot, target)
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
        if (len(restored_snapshot_names) == len(snapshots)
                and set(restored_snapshot_names).issubset(set(current_snapshots))):
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

        for source in self.config.sources:
            remote_datasets = source.get_matching_target_datasets()

            for dataset in remote_datasets:
                if self.cli_args.filter and not dataset.zfs_path.startswith(self.cli_args.filter):
                    continue
                backup_snapshots = dataset.get_backup_snapshots(self.config.snapshot_prefix)
                if not backup_snapshots:
                    print("No matching snapshots found for dataset {}".format(dataset))
                    continue
                self._do_restore_into_target(dataset.zfs_path, backup_snapshots,
                                             self.cli_args.restore, dataset.get_all_target_paths())

    def _itemize_option(self, option_content: Optional[str]) -> List[str]:
        if not option_content:
            return []
        items: List[str] = []
        lines = [line.replace('\r\n', '').replace('\n', '').strip()
                 for line in option_content.splitlines()]
        for line in lines:
            items.extend(i.strip() for i in line.split(','))
        return items

    def _load_config(self, path: str) -> Tuple[BackupSetup, Optional[SshHost]]:
        parser = configparser.ConfigParser(interpolation=EnvInterpolation(),
                                           converters={'list': lambda x: [i.strip() for i in x.split(',')]}
                                           )
        if os.path.isdir(path):
            for root, dirs, files in os.walk(path):
                for file in sorted(files):
                    parser.read(os.path.join(root, file))
        else:
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
                target_group = TargetGroup(self.shell_command,
                                           self._itemize_option(parser.get(section, 'path')),
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

                backup_sources.append(BackupSource(self.shell_command, section,
                                                   sources,
                                                   [target_groups[t] for t in targets],
                                                   parser.getboolean(section, 'recursive', fallback=False),
                                                   self._itemize_option(parser.get(section, 'exclude', fallback=None)),
                                                   self._itemize_option(parser.get(section, 'include', fallback=None)),
                                                   ))

        return BackupSetup(backup_sources, snapshot_prefix, include_intermediate_snapshots), remote


if __name__ == "__main__":
    app = ZfsBackupTool()
    app.run()
