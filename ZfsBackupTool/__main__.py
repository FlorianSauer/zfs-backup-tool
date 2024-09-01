import argparse
import configparser
import os
import sys
from os.path import expandvars
from typing import Optional, List, Dict, Tuple

from ZfsBackupTool.BackupPlan import BackupPlan
from ZfsBackupTool.BackupPlan.Operations import make_next_backup_view, map_snapshots_to_data_sources
from ZfsBackupTool.BackupSetup import BackupSetup
from ZfsBackupTool.Config import TargetGroup, BackupSource
from ZfsBackupTool.Constants import (SNAPSHOT_PREFIX_POSTFIX_SEPARATOR, INITIAL_SNAPSHOT_POSTFIX,
                                     TARGET_STORAGE_SUBDIRECTORY, INITIALIZED_FILE_NAME)
from ZfsBackupTool.ShellCommand import ShellCommand, SshHost
from ZfsBackupTool.Zfs import scan_zfs_pools, PoolList, DataSet, ZfsResolveError


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
    cli_parser.add_argument('--dry-run', action='store_true', help='Dry run, do not execute any '
                                                                   'destructive commands')
    cli_parser.add_argument('--version', action='version', version='%(prog)s 0.1')
    subparsers = cli_parser.add_subparsers(dest='subparser_name')

    init_parser = subparsers.add_parser('init',
                                        help='Initialize backup targets and verify write access. '
                                             'Useful when using freshly formatted disks.'
                                             'Also verifies all needed programs are installed.',
                                        description='Initialize backup targets and verify write access. '
                                                    'Useful when using freshly formatted disks.'
                                                    'Also verifies all needed programs are installed.')
    backup_parser = subparsers.add_parser('backup', help='Perform backup.', description='Perform backup.')
    backup_parser.add_argument('--new', action='store_true',
                               help='Perform new backup. '
                                    'Deletes all existing backup snapshots before regular backup process.')
    backup_parser.add_argument('--clean', action='store_true',
                               help='Removes all backup snapshots. Does not create new backup.')
    backup_parser.add_argument('--missing', '--repair', action='store_true',
                               help='REPAIR-MODE: Re-create backups only for missing snapshots. '
                                    'Skips creation of a new incremental backups. '
                                    'Useful for resuming previously aborted backup runs.')
    backup_parser.add_argument('-f', '--filter',
                               help='Create new backups only for datasets starting with given filter.')
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
    restore_parser.add_argument('restore', type=str, help='Perform restore into given root path. '
                                                          'Use . for inplace restore. '
                                                          'WARNING: This will may cause temporary data loss.')
    restore_parser.add_argument('-f', '--filter',
                                help='Perform restore only for datasets starting with given filter.')
    restore_parser.add_argument('--force', action='store_true',
                                help='Force restore, even if the target path is not empty.')
    restore_parser.add_argument('-w', '--wipe', action='store_true',
                                help='Wipe datasets, if they already contain snapshots and a restore of an initial '
                                     'snapshot would fail.')
    verify_parser = subparsers.add_parser('verify', help='Verify datasets and their snapshots on targets.',
                                          description='Verify datasets and their snapshots on targets.')
    verify_parser.add_argument('-a', '--all', action='store_true',
                               help='Verify all stored and available datasets and snapshots on all configured targets. '
                                    'This will even check snapshots of datasets, which are not configured in the given '
                                    'config.')
    verify_parser.add_argument('-f', '--filter',
                               help='Perform verification only for datasets starting with given filter.')
    verify_parser.add_argument('-t', '--target-filter',
                               help='Perform verification only on targets starting with given filter.')
    verify_parser.add_argument('-r', '--remove-corrupted', action='store_true',
                               help='DESTRUCTIVE OPERATION: Remove corrupted files from targets. '
                                    'Detected corrupted files are removed from the target and can be re-created with '
                                    '\'backup --missing\'')
    list_parser = subparsers.add_parser('list', help='List backup snapshots stored on targets.',
                                        description='List backup snapshots stored on targets.')
    list_parser.add_argument('--plain', action='store_true',
                             help='Only list zfs paths, without additional grouping info.')
    list_parser.add_argument('--all-remote', action='store_true',
                             help='List all stored datasets and snapshots on all targets.')

    # endregion

    def __init__(self):
        self.cli_args: argparse.Namespace = None  # type: ignore
        self.config: BackupSetup = None  # type: ignore
        self.shell_command: ShellCommand = None  # type: ignore
        self.backup_plan: BackupPlan = None  # type: ignore

    def run(self):
        self.cli_args = self.cli_parser.parse_args(sys.argv[1:])
        self.shell_command = ShellCommand(echo_cmd=self.cli_args.debug)
        self.config = self._load_config(self.cli_args.config)
        self.backup_plan = BackupPlan(self.shell_command,
                                      include_intermediate_snapshots=self.config.include_intermediate_snapshots,
                                      dry_run=self.cli_args.dry_run
                                      )

        try:
            # self.do_check_programs_installed()
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
            if self.cli_args.subparser_name == 'verify':
                self.do_verify()
        except KeyboardInterrupt:
            # only print stack trace if debug is enabled
            if self.cli_args.debug:
                raise
            else:
                print("... Aborted!")
                sys.exit(1)

    def do_check_programs_installed(self):
        print("Checking if all locally needed programs are installed...")
        for program in ["ssh", "zfs", "pv", "sha256sum"]:
            self.shell_command.program_is_installed(program)
        for host in self.config.get_all_hosts():
            self.shell_command.set_remote_host(host)
            print("Checking if all remotely needed programs are installed on {} ...".format(host or 'localhost'))
            for program in ["pv", "sha256sum"]:
                self.shell_command.program_is_installed(program)

    def do_list(self):
        # scan local zfs setup
        local_pools = scan_zfs_pools(self.shell_command)
        local_pools.build_incremental_snapshot_refs()

        # filter out source pools by config
        backup_source_configured_pools_mapping = self.config.filter_by_sources(local_pools)

        # region check if all sources found a dataset

        # verify all sources found a pool/dataset
        for source, pools in backup_source_configured_pools_mapping.items():
            if not pools.has_datasets():
                print("Source {} has no datasets".format(source.name))
                pools.print()
                sys.exit(1)

        # endregion

        # scan remote file-based zfs setup, prefiltered by snapshot prefix.
        # scanned pools might contain datasets, that are not included by the current config.
        target_pools = self.config.gather_target_pools(self.shell_command, include_all=self.cli_args.all_remote)

        if self.cli_args.all_remote:
            if self.cli_args.plain:
                for (host, target_path), pools in target_pools.items():
                    for snapshot in pools.iter_snapshots():
                        host_target_path = "{}:{}".format(host or 'localhost', target_path)
                        print(os.path.join(host_target_path, TARGET_STORAGE_SUBDIRECTORY, snapshot.zfs_path))
            else:
                for (host, target_path), pools in target_pools.items():
                    print("Target: {}:{}".format(host or 'localhost', target_path))
                    pools.print()
                    print()
        else:
            if self.cli_args.plain:
                for snapshot in PoolList.merge(*backup_source_configured_pools_mapping.values()).iter_snapshots():
                    print(snapshot.zfs_path)
            else:
                for source, pools in backup_source_configured_pools_mapping.items():
                    print("Source: {}".format(source.name))
                    print("Target: {}".format(', '.join(
                        "{}:{}".format(host or 'localhost', target_path)
                        for (host, target_path) in source.get_all_host_taget_paths())))
                    pools.print()
                    print()

    def do_init(self):
        print("Initializing backup targets...")
        for host, target in self.config.get_all_host_target_paths():
            self.shell_command.set_remote_host(host)
            self.shell_command.target_mkdir(os.path.join(target, TARGET_STORAGE_SUBDIRECTORY))
            self.shell_command.target_write_to_file(os.path.join(target, TARGET_STORAGE_SUBDIRECTORY,
                                                                 INITIALIZED_FILE_NAME),
                                                    "initialized")
            print("Target {}:{} initialized.".format(host or 'localhost', target))
        print("Initialization done.")
        print("Checking if all needed programs are installed...")
        self.do_check_programs_installed()

    def do_backup(self):
        # scan local zfs setup
        local_pools = scan_zfs_pools(self.shell_command)
        local_pools.build_incremental_snapshot_refs()

        # filter out source pools by config
        backup_source_configured_pools_mapping = self.config.filter_by_sources(local_pools)

        # region check if all sources found a dataset

        # verify all sources found a pool/dataset
        for source, pools in backup_source_configured_pools_mapping.items():
            if not pools.has_datasets():
                print("Source {} has no datasets".format(source.name))
                pools.print()
                sys.exit(1)

        # endregion

        # scan remote file-based zfs setup, prefiltered by snapshot prefix.
        # scanned pools might contain datasets, that are not included by the current config.
        target_pools = self.config.gather_target_pools(self.shell_command, include_all=False)

        # region reset snapshots if requested

        if self.cli_args.new or self.cli_args.clean:
            # filter out target pools by config, merge them together afterwards and map them to the specific target
            # this will narrow down the target pools to the configured sources view
            configured_target_pools_with_targetinfo: Dict[Tuple[Optional[SshHost], str], PoolList] = {}
            for (host, target_path), pools in target_pools.items():
                configured_target_pools = self.config.filter_by_sources(pools)
                configured_target_pools_with_targetinfo[(host, target_path)] = PoolList.merge(
                    *configured_target_pools.values())

            # clean old snapshots
            self.backup_plan.clean_snapshots(PoolList.merge(*backup_source_configured_pools_mapping.values()),
                                             self.cli_args.filter)
            # and on remote

            # this might cause dataloss, restore from backup is not possible after this
            # however, the live data is still there, together with any non-managed snapshots
            self.backup_plan.clean_remote_snapshots(configured_target_pools_with_targetinfo,
                                                    self.cli_args.filter)

            if self.cli_args.clean:
                # abort further processing if clean is requested
                return

            # now we need to re-scan the local+remote zfs setup, because we deleted snapshots
            local_pools = scan_zfs_pools(self.shell_command)
            local_pools.build_incremental_snapshot_refs()

            # filter out source pools by config again
            backup_source_configured_pools_mapping = self.config.filter_by_sources(local_pools)

            # re-scan remote file-based zfs setup, prefiltered by snapshot prefix.
            # re-scanned pools might contain datasets, that are not included by the current config.
            target_pools = self.config.gather_target_pools(self.shell_command, include_all=False)
        # endregion

        # region recreate missing/aborted backup snapshots

        # find snapshots that need repairing for each target
        repair_pools: Dict[Tuple[Optional[SshHost], str], PoolList] = {}
        # diff every configured source pool with the found target pools as diff-base
        for source, pools in backup_source_configured_pools_mapping.items():
            # iter target groups coming from the source/local pool view
            # this will map the source pools to the target pools
            for target_group in source.targets:
                for target_path in target_group.target_paths:
                    if (target_group.remote, target_path) not in target_pools:
                        # missing completely, repair all
                        repair_diff = pools.view()
                    else:
                        remote = target_pools[(target_group.remote, target_path)]
                        shared_set = pools.intersection(remote)
                        source_with_shared = PoolList.merge(pools, shared_set)
                        repair_diff = source_with_shared.difference(remote)
                    # only include pools with snapshots that need repairing
                    if repair_diff.has_snapshots():
                        print("Repairing target '{}:{}' for source '{}'".format(target_group.remote or 'localhost',
                                                                                target_path,
                                                                                source.name))
                        repair_diff.print()
                        print()
                        if (target_group.remote, target_path) in repair_pools:
                            repair_pools[(target_group.remote, target_path)] = PoolList.merge(
                                repair_pools[(target_group.remote, target_path)],
                                repair_diff)
                        else:
                            repair_pools[(target_group.remote, target_path)] = repair_diff

        if not self.cli_args.new:
            # repair snapshots
            if any(p.has_snapshots() for p in repair_pools.values()):
                self.backup_plan.repair_snapshots(repair_pools)
            else:
                print("No snapshots need repairing, all ok :)")
            if self.cli_args.missing:
                # recreate missing/aborted backup snapshots, then exit
                return

        # endregion

        # region create new snapshots as bulk operation

        # merge all source pools to one view, so new snapshots can be made for all datasets at once
        # create a view, which contains all new needed backup snapshots
        backup_snapshots = make_next_backup_view(PoolList.merge(*backup_source_configured_pools_mapping.values()),
                                                 self.config.snapshot_prefix,
                                                 zfs_path_filter=self.cli_args.filter,
                                                 skip_view=PoolList.merge(*repair_pools.values()))

        # also verify, that each dataset has a now at least the initial snapshot
        # for this we need to merge the current local pools with the new backup snapshots
        for dataset in PoolList.merge(PoolList.merge(*backup_source_configured_pools_mapping.values()),
                                      backup_snapshots).iter_datasets():
            # also filter out datasets that are not included with the filter argument
            if self.cli_args.filter and not dataset.zfs_path.startswith(self.cli_args.filter):
                print("Skipping dataset {} because it does not match the filter".format(dataset.zfs_path))
                continue
            try:
                dataset.get_snapshot_by_name(
                    self.config.snapshot_prefix + SNAPSHOT_PREFIX_POSTFIX_SEPARATOR + INITIAL_SNAPSHOT_POSTFIX)
            except ZfsResolveError:
                print("No initial backup snapshot found for dataset {}".format(dataset.zfs_path))
                print("Cannot proceed without intermediate backup snapshots")
                print("Aborting...")
                sys.exit(1)

        # create new snapshots
        self.backup_plan.create_snapshots(backup_snapshots)

        # endregion

        # region transmit backup as bulk operation

        # map concrete backup target to the pools that need to be written
        backup_pools: Dict[Tuple[Optional[SshHost], str], PoolList] = {}
        for backup_source, pools in self.config.filter_by_sources(backup_snapshots).items():
            # iter target groups coming from the source/local pool view
            # this will map the source pools to the target pools
            for target_group in backup_source.targets:
                for target_path in target_group.target_paths:
                    # exclude targets that do not match the optional target_filter argument
                    if self.cli_args.target_filter and not target_path.startswith(self.cli_args.target_filter):
                        continue
                    if (target_group.remote, target_path) in backup_pools:
                        backup_pools[(target_group.remote, target_path)] = PoolList.merge(
                            backup_pools[(target_group.remote, target_path)],
                            pools)
                    else:
                        backup_pools[(target_group.remote, target_path)] = pools

        # write new snapshots to targets
        self.backup_plan.backup_snapshots(backup_pools)

        # endregion

        print("Backup done.")

    def do_restore(self):
        if self.cli_args.restore.startswith('/'):
            print("Zfs path must not start with /")
            sys.exit(1)
        if self.cli_args.restore == '.':
            restore_prefix = ''
        else:
            restore_prefix = self.cli_args.restore
            if not restore_prefix.endswith('/'):
                restore_prefix += '/'

        # scan local zfs setup and build up incremental snapshot refs
        all_local_pools = scan_zfs_pools(self.shell_command)
        all_local_pools.build_incremental_snapshot_refs()

        # filter out source pools by config
        configured_local_pools_sources_mapping = self.config.filter_by_sources(all_local_pools)

        # scan remote file-based zfs setup, prefiltered by snapshot prefix.
        # scanned pools might contain datasets, that are not included by the current config.
        remote_pools_host_paths_mapping = self.config.gather_target_pools(self.shell_command,
                                                                          include_all=False)

        # merge all remote pools into one, build up incremental snapshot backrefs for this remote summary pool
        all_remote_pools = PoolList.merge(*remote_pools_host_paths_mapping.values())
        all_remote_pools.build_incremental_snapshot_refs()

        # filter out remote pools by config
        configured_remote_pools_sources_mapping = self.config.filter_by_sources(all_remote_pools)

        # we now have gathered all infos about our local and remote status.
        # we also know the status of the configured restricted view.

        # we only repair configured pools/datasets/snapshots

        # we should run a sanity check and verify, that both local and remote configured sources are equal
        # compare BackupSource objects for this
        assert not set(configured_local_pools_sources_mapping.keys()
                       ).difference(configured_remote_pools_sources_mapping.keys())

        # we now know, that we have the same sources configured on local and remote
        # we can now build up a list of pools/datasets/snapshots that need to be repaired logically.
        # after that we will find out, where we can fetch the repair data from

        repair_pools: PoolList = PoolList()
        for source, remote_pool in configured_remote_pools_sources_mapping.items():
            # the main problem is the shifting of the backup into another zfs path position with the
            # self.cli_args.restore parameter. we need to find out, which snapshots are missing locally.
            # first we need to do the actual shifting of the logical target-state into the correct zfs path position.
            if not restore_prefix:
                # for inplace (empty restore_prefix) we do not need any shifting
                final_expected_pool = remote_pool
            else:
                final_expected_pool = remote_pool.prefixed_view(restore_prefix)
            # now we need to find the missing snapshots on the local side
            # for this we can combine the current local pool with the final expected pool
            # afterwards we can find the difference between the two, so we get a set of effectively missing snapshots
            # worst case: the whole shifted view is returned, because all is missing under the target path
            # mixed case: some datasets are missing, some are not. we need to repair the missing ones.
            repair_diff = PoolList.merge(all_local_pools, final_expected_pool).difference(all_local_pools)
            # local_pool = configured_local_pools_sources_mapping[source]
            # repair_diff = remote_pool.difference(local_pool)
            if self.cli_args.filter:
                repair_diff = repair_diff.filter_include_by_zfs_path_prefix(self.cli_args.filter)
            if repair_diff.has_snapshots():
                print("Repairing source: ", source.name)
                repair_diff.print()
                repair_pools = PoolList.merge(repair_pools, repair_diff)

        if not repair_pools.has_snapshots():
            print("No snapshots need repairing, all ok :)")
            return

        print("hard missing snapshots:")
        repair_pools.print()

        # we now have to refine this list even more. restores of single intermediate snapshots which have snapshot
        # children are not possible. we need to restore the whole dataset with all snapshots from the first missing
        # snapshot up to the latest/last snapshot.

        # to do this, we need to create a list of pools/datasets/snapshots that are configured and available remotely.
        # afterwards we also have to find out, if the local pools have snapshots that are missing on the remote pools,
        # because this would fail the restore process. intermediate snapshots cannot be restored alone.
        # including snapshot children is required for restore, but for this we need the restore data from the remote.
        # if snapshots are missing on the remote side, but exist on the local side, this would be unsolvable.
        # we would have to delete local snapshots and create data loss that way.
        # first backing them up and then restoring it is a possible solution, but this can be solved by repairing the
        # remote backups.
        all_remote_configured_pools = PoolList.merge(*configured_remote_pools_sources_mapping.values())
        all_local_configured_pools = PoolList.merge(*configured_local_pools_sources_mapping.values())

        intermediate_children_of_repair_pools: PoolList = PoolList()
        conflicting_intermediate_snapshots = PoolList()
        for pool in repair_pools:
            repair_pool = pool.copy()
            intermediate_children_of_repair_pools.add_pool(repair_pool)
            for dataset in pool:
                # resolve the dataset on the remote side which contains all snapshots from the first missing snapshot
                # up to the latest snapshot
                first_needed_snapshot = next(dataset.iter_snapshots())
                fully_available_dataset = all_remote_configured_pools.prefixed_view(
                    restore_prefix).get_dataset_by_path(dataset.zfs_path)
                try:
                    current_local_dataset = all_local_configured_pools.get_dataset_by_path(dataset.zfs_path)
                except ZfsResolveError:
                    # the whole dataset is missing on local -> copy the remote dataset object without snapshots.
                    current_local_dataset = fully_available_dataset.copy()
                logical_full_dataset = DataSet.merge(pool.pool_name,
                                                     fully_available_dataset, current_local_dataset)
                incremental_children_dataset = logical_full_dataset.get_incremental_children(first_needed_snapshot)

                # verify that the intermediate needed snapshots are available on the remote side
                if incremental_children_dataset.difference(fully_available_dataset).has_snapshots():
                    print("Intermediate snapshots are missing on the remote side")
                    print("This would fail the restore process")
                    print("We need to repair the remote side first")
                    print("needed snapshots for full restore:")
                    incremental_children_dataset.print()
                    print("missing intermediate snapshots:")
                    incremental_children_dataset.difference(fully_available_dataset).print()
                    print("Remote available snapshots:")
                    fully_available_dataset.print()
                    print("Local available snapshots:")
                    current_local_dataset.print()
                    return

                # verify that the intermediate needed snapshots are not existing on the local side
                if incremental_children_dataset.intersection(current_local_dataset).has_snapshots():
                    if self.cli_args.force:
                        print("removing existing intermediate snapshots on local side")
                        self.backup_plan.clean_snapshots(
                            incremental_children_dataset.intersection(current_local_dataset),
                            zfs_path_filter=self.cli_args.filter)
                    else:
                        print("Intermediate snapshots are existing on the local side for dataset: {}".format(
                            dataset.zfs_path))
                        conflicting_intermediate_snapshots.add_dataset(
                            incremental_children_dataset.intersection(current_local_dataset))

                # verify that no intermediate snapshot is missing
                if any(not snapshot.has_incremental_base()
                       for snapshot in incremental_children_dataset
                       if not snapshot.snapshot_name.endswith(SNAPSHOT_PREFIX_POSTFIX_SEPARATOR
                                                              + INITIAL_SNAPSHOT_POSTFIX)
                       ):
                    breakpoint()

                repair_pool.add_dataset(incremental_children_dataset)

        if conflicting_intermediate_snapshots.has_snapshots():
            print("Error:")
            print("The following intermediate snapshots need to be removed on the local side")
            conflicting_intermediate_snapshots.print()
            print("Use --force to remove them")
            print("Aborting due to existing intermediate snapshots on local side")
            sys.exit(1)

        repair_pools_with_intermediate_children = PoolList.merge(repair_pools, intermediate_children_of_repair_pools)

        # we now have a list of pools/datasets/snapshots that need to be repaired
        # we will now find out, where we can fetch the repair data from
        print("hard missing snapshots with intermediate children")
        repair_pools_with_intermediate_children.print()

        # we only need the snapshot objects in the correct order and the host-target-path-mappings from where to fetch
        # the snapshot data from
        # but our repair set is shifted by the restore_prefix, so we need to de-shift the repair set
        de_shifted_repair_pools_with_intermediate_children = repair_pools_with_intermediate_children.prefixed_view(
            restore_prefix, deshift=True)
        repair_snapshot_restore_source_mapping = map_snapshots_to_data_sources(
            de_shifted_repair_pools_with_intermediate_children,
            remote_pools_host_paths_mapping)

        # we now have a list of snapshots and the host-target-path-mappings from where we can fetch the repair data from
        if restore_prefix:
            # restore to given path
            self.backup_plan.restore_snapshots(repair_snapshot_restore_source_mapping,
                                               restore_target=restore_prefix,
                                               inplace=False,
                                               initial_wipe=self.cli_args.wipe)
        else:
            # inplace restore
            self.backup_plan.restore_snapshots(repair_snapshot_restore_source_mapping,
                                               inplace=True,
                                               initial_wipe=self.cli_args.wipe)

        print("Restore done.")

    def do_verify(self):
        # scan local zfs setup
        local_pools = scan_zfs_pools(self.shell_command)
        local_pools.build_incremental_snapshot_refs()

        # filter out source pools by config
        backup_source_configured_pools_mapping = self.config.filter_by_sources(local_pools)

        # region check if all sources found a dataset

        # verify all sources found a pool/dataset
        for source, pools in backup_source_configured_pools_mapping.items():
            if not pools.has_datasets():
                print("Source {} has no datasets".format(source.name))
                pools.print()
                sys.exit(1)

        # endregion

        # scan remote file-based zfs setup, prefiltered by snapshot prefix.
        # scanned pools might contain datasets, that are not included by the current config.
        target_pools = self.config.gather_target_pools(self.shell_command, include_all=self.cli_args.all)

        # we now have gathered all infos about our local and remote status.
        # we also know the status of the configured restricted view.
        # we can now verify the stored backups, by using the local pools as primary view and the remote pools as
        # secondary 'source view'.
        # if --all is used, we will also verify datasets that are not configured in the current config.
        # this means we ignore the primary view and just use the available secondary remote view.

        if self.cli_args.all:
            # we will now verify all datasets that are available on the remote side
            # we will not use the local pools for this, because we want to verify all datasets on all targets
            # we will use the remote pools as primary view

            self.backup_plan.verify_snapshots(target_pools, self.cli_args.remove_corrupted,
                                              target_path_prefix_filter=self.cli_args.target_filter,
                                              zfs_path_prefix_filter=self.cli_args.filter)
            return  # we are done here

        # find snapshots that need verification for each target
        verify_pools: Dict[Tuple[Optional[SshHost], str], PoolList] = {}
        # diff every configured source pool with the found target pools as diff-base
        for source, pools in backup_source_configured_pools_mapping.items():
            # iter target groups coming from the source/local pool view
            # this will map the source pools to the target pools
            for target_group in source.targets:
                for target_path in target_group.target_paths:
                    if self.cli_args.target_filter and not target_path.startswith(self.cli_args.target_filter):
                        continue
                    if (target_group.remote, target_path) not in target_pools:
                        # missing completely, still verify all to trigger warnings
                        verify_diff = pools.view()
                    else:
                        remote = target_pools[(target_group.remote, target_path)]
                        shared_set = pools.intersection(remote)
                        source_with_shared = PoolList.merge(pools, shared_set)
                        verify_diff = source_with_shared
                    # empty diffs should not be a case, because we actively include the local view
                    if not verify_diff.has_snapshots():
                        raise NotImplementedError("Empty diff should not be a case")

                    if self.cli_args.filter:
                        verify_diff = verify_diff.filter_include_by_zfs_path_prefix(self.cli_args.filter)
                        if not verify_diff.has_snapshots():
                            continue

                    print("Verifying target '{}:{}' for source '{}'".format(target_group.remote or 'localhost',
                                                                            target_path,
                                                                            source.name))
                    verify_diff.print()
                    print()
                    if (target_group.remote, target_path) in verify_pools:
                        verify_pools[(target_group.remote, target_path)] = PoolList.merge(
                            verify_pools[(target_group.remote, target_path)],
                            verify_diff)
                    else:
                        verify_pools[(target_group.remote, target_path)] = verify_diff

        self.backup_plan.verify_snapshots(verify_pools, self.cli_args.remove_corrupted)
        print("Verification done.")

    def _itemize_option(self, option_content: Optional[str]) -> List[str]:
        if not option_content:
            return []
        items: List[str] = []
        lines = [line.replace('\r\n', '').replace('\n', '').strip()
                 for line in option_content.splitlines()]
        for line in lines:
            items.extend(i.strip() for i in line.split(','))
        return items

    def _load_config(self, path: str) -> BackupSetup:
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
        remotes = {}
        for section in parser.sections():
            if section.lower().startswith('remote '):
                remote_name = section[len("remote "):].strip()
                remotes[remote_name] = SshHost(parser.get(section, 'host'),
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
                if parser.has_option(section, 'remote'):
                    try:
                        remote: Optional[SshHost] = remotes[parser.get(section, 'remote')]
                    except KeyError:
                        raise ValueError("Remote '{}' not defined".format(parser.get(section, 'remote')))
                else:
                    remote = None

                if section.lower().startswith('target-group '):
                    target_group_name = section[len("target-group "):].strip()
                elif section.lower().startswith('targetgroup '):
                    target_group_name = section[len("targetgroup "):].strip()
                else:
                    raise NotImplementedError("Invalid section name for target group")

                target_group = TargetGroup(target_group_name, self._itemize_option(parser.get(section, 'path')),
                                           remote)
                target_groups[target_group_name] = target_group

        # parse BackupSource sections
        backup_sources = []
        for section in parser.sections():
            if section.lower().startswith('source'):
                source_name = section[len("source "):].strip()
                targets = self._itemize_option(parser.get(section, 'target'))
                for target in targets:
                    if target not in target_groups:
                        raise ValueError("TargetGroup '{}' not defined".format(target))
                sources = self._itemize_option(parser.get(section, 'source'))
                for source in sources:
                    if '@' in source:
                        raise ValueError("ZFS source '{}' contains '@', sources must not aim at snapshots!".format(
                            source))

                backup_sources.append(BackupSource(source_name,
                                                   sources,
                                                   [target_groups[t] for t in targets],
                                                   parser.getboolean(section, 'recursive', fallback=False),
                                                   self._itemize_option(parser.get(section, 'exclude', fallback=None)),
                                                   self._itemize_option(parser.get(section, 'include', fallback=None)),
                                                   ))

        return BackupSetup(backup_sources, snapshot_prefix, include_intermediate_snapshots)


if __name__ == "__main__":
    app = ZfsBackupTool()
    app.run()
