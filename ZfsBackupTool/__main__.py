import argparse
import configparser
import os
import sys
from os.path import expandvars
from typing import Optional, List, Dict, Tuple

from ZfsBackupTool.BackupPlan import BackupPlan
from ZfsBackupTool.BackupPlan.Operations import (make_next_backup_view, map_snapshots_to_data_sources,
                                                 find_snapshot_holes_of_dataset, PlanningException,
                                                 find_repairable_snapshots, find_conflicting_intermediate_snapshots,
                                                 find_initial_conflicting_snapshots, find_restore_chain_holes)
from ZfsBackupTool.BackupSetup import BackupSetup
from ZfsBackupTool.Config import TargetGroup, BackupSource
from ZfsBackupTool.Constants import (SNAPSHOT_PREFIX_POSTFIX_SEPARATOR, INITIAL_SNAPSHOT_POSTFIX,
                                     TARGET_STORAGE_SUBDIRECTORY, INITIALIZED_FILE_NAME)
from ZfsBackupTool.ShellCommand import ShellCommand, SshHost
from ZfsBackupTool.Zfs import scan_zfs_pools, PoolList, ZfsResolveError


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
    restore_parser.add_argument('restore_path', type=str,
                                help='Perform restore into given zfs path. Use . for inplace restore.')
    restore_parser.add_argument('-f', '--filter',
                                help='Perform restore only for datasets starting with given filter.')
    restore_parser.add_argument('--force', action='store_true',
                                help='Force restore, even if the target path already contains intermediate backup '
                                     'snapshots. This will delete intermediate snapshots and recreates them. This will '
                                     'cause temporary data loss.')
    restore_parser.add_argument('-r', '--replace', action='store_true',
                                help='Initial snapshots can only be restored into a new non-existing dataset. '
                                     'If a dataset with the target name already exists, it needs to be moved or '
                                     'deleted. Use this flag to move the restored dataset into the place of the '
                                     'already present dataset and the already present dataset to "<dataset>.replaced". '
                                     'Children will get renamed/moved under the restored dataset.')
    restore_parser.add_argument('-w', '--wipe', action='store_true',
                                help='Deletes the replaced datasets (.replaced postfix) completely. '
                                     'WARNING: This will cause permanent DATA LOSS!')
    restore_parser.add_argument('-i', '--incremental', action='store_true',
                                help='Restore not all missing snapshots, but only the latest missing snapshots. Useful '
                                     'for restoring a backup, when the initial snapshot (or other previous incremental '
                                     'snapshots) are not needed, but only the snapshots after the latest currently '
                                     'existing snapshot is needed.')
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
                                      dry_run=self.cli_args.dry_run,
                                      debug=self.cli_args.debug)

        try:
            # Todo: add a remote clean function. when the config file changes and some datasets are not included anymore,
            #  the old datasets are still present on the remote side. we should clean them up but keep the still
            #  configured datasets.
            # self.do_check_programs_installed()
            if self.cli_args.subparser_name is None:
                self.cli_parser.print_help()
                sys.exit(1)
            if self.cli_args.subparser_name == 'init':
                self.do_init()
            if self.cli_args.subparser_name == 'list':
                self.do_list()
            if self.cli_args.subparser_name == 'backup':
                self.do_backup()
            if self.cli_args.subparser_name == 'verify':
                self.do_verify()
            if self.cli_args.subparser_name == 'restore':
                self.do_restore()
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
            self.shell_command.program_is_installed(program, verbose=True)
        for host in self.config.get_all_hosts():
            if host is None:
                # already checked locally
                continue
            self.shell_command.set_remote_host(host)
            print("Checking if all remotely needed programs are installed on {} ...".format(host or 'localhost'))
            for program in ["pv", "sha256sum"]:
                self.shell_command.program_is_installed(program, verbose=True)
        print("All needed programs are installed.")

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
                pools.print(with_incremental_base=self.cli_args.debug)
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
                    pools.print(with_incremental_base=self.cli_args.debug)
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
                    pools.print(with_incremental_base=self.cli_args.debug)
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
        print("Checking if all needed programs are installed...")
        self.do_check_programs_installed()
        print("Initialization done.")

    def do_backup(self):
        # scan local zfs setup
        local_pools = scan_zfs_pools(self.shell_command)
        local_pools.build_incremental_snapshot_refs()

        # filter out source pools by config
        backup_source_configured_pools_mapping = self.config.filter_by_sources(local_pools)

        # region check if all sources found a dataset and backup chain is complete

        # verify all sources found a pool/dataset
        for source, pools in backup_source_configured_pools_mapping.items():
            if not pools.has_datasets():
                print("Source {} has no datasets".format(source.name))
                pools.print(with_incremental_base=self.cli_args.debug)
                sys.exit(1)

        # we should also check, if the local side is missing any snapshots. if this is the case, we cannot restore
        # because we cannot restore intermediate snapshots without the full chain of snapshots (A->B->C is ok,
        # A->C is not). Datasets are allowed to be missing, but we at least need a consistent snapshot chain.
        for local_pool in PoolList.merge(*backup_source_configured_pools_mapping.values()):
            for dataset in local_pool.iter_datasets():
                dataset_holes = find_snapshot_holes_of_dataset(dataset, self.config.snapshot_prefix)
                if dataset_holes.has_snapshots():
                    print("Dataset {} has holes in the snapshot chain".format(dataset.zfs_path))
                    print("This would fail the repair process.")
                    print("A consistent snapshot chain is needed.")
                    print("Solutions for this:")
                    print("1. Perform a new backup with the --new flag.")
                    print("2. Perform a repair of the local side with the restore command.")
                    print("3. Remove local snapshots which come after the holes.")
                    print()
                    print("Holes:")
                    dataset_holes.print(with_incremental_base=self.cli_args.debug)
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
            self.backup_plan.clean_snapshots(
                PoolList.merge(*backup_source_configured_pools_mapping.values()
                               ).filter_include_by_zfs_path_prefix(self.cli_args.filter))
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
                        repair_diff.print(with_incremental_base=self.cli_args.debug)
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

        selected_backup_pools = PoolList.merge(*backup_source_configured_pools_mapping.values()
                                               ).filter_include_by_zfs_path_prefix(self.cli_args.filter)

        # merge all source pools to one view, so new snapshots can be made for all datasets at once
        # create a view, which contains all new needed backup snapshots
        backup_snapshots = make_next_backup_view(selected_backup_pools,
                                                 self.config.snapshot_prefix,
                                                 skip_view=PoolList.merge(*repair_pools.values()))

        # also verify, that each dataset has a now at least the initial snapshot
        # for this we need to merge the current local pools with the new backup snapshots
        for dataset in PoolList.merge(PoolList.merge(*backup_source_configured_pools_mapping.values()),
                                      backup_snapshots).iter_datasets():
            # also filter out datasets that are not included with the filter argument
            if self.cli_args.filter and not dataset.zfs_path.startswith(self.cli_args.filter):
                continue
            try:
                dataset.get_snapshot_by_name(
                    self.config.snapshot_prefix + SNAPSHOT_PREFIX_POSTFIX_SEPARATOR + INITIAL_SNAPSHOT_POSTFIX)
            except ZfsResolveError:
                print("No initial backup snapshot found for dataset {}".format(dataset.zfs_path))
                print("Restore might only work in intermediate mode, not in full mode.")

        # also map the backup snapshots to configured sources
        configured_backup_snapshots = self.config.filter_by_sources(backup_snapshots)

        # create new snapshots
        print("New backup snapshots:")
        backup_snapshots.print(with_incremental_base=self.cli_args.debug)
        self.backup_plan.create_snapshots(backup_snapshots)

        # endregion

        # region transmit backup as bulk operation

        # map concrete backup target to the pools that need to be written
        backup_pools: Dict[Tuple[Optional[SshHost], str], PoolList] = {}
        for backup_source, pools in configured_backup_snapshots.items():
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
        if self.cli_args.restore_path.startswith('/'):
            print("Zfs path must not start with /")
            sys.exit(1)
        if self.cli_args.restore_path == '.':
            restore_prefix = ''
        else:
            restore_prefix = self.cli_args.restore_path
            if not restore_prefix.endswith('/'):
                restore_prefix += '/'

        # scan local zfs setup and build up incremental snapshot refs
        all_local_pools = scan_zfs_pools(self.shell_command)
        all_local_pools.build_incremental_snapshot_refs()

        # filter out source pools by our backup snapshot prefix
        configured_local_backup_pools = self.config.filter_by_prefix(all_local_pools)

        # filter out source pools by config
        configured_local_pools_sources_mapping = self.config.filter_by_sources(all_local_pools)

        # scan remote file-based zfs setup, prefiltered by snapshot prefix.
        # scanned pools might contain datasets, that are not included by the current config.
        remote_pools_host_paths_mapping = self.config.gather_target_pools(self.shell_command,
                                                                          include_all=False)

        # merge all remote pools into one, build up incremental snapshot backrefs for this remote summary pool
        all_remote_pools = PoolList.merge(*remote_pools_host_paths_mapping.values())
        all_remote_pools.build_incremental_snapshot_refs()

        # also build up the incremental snapshot refs for the remote pools on each host
        for host, remote_pool in remote_pools_host_paths_mapping.items():
            remote_pool.build_incremental_snapshot_refs()

        # filter out remote pools by config
        configured_remote_pools_sources_mapping = self.config.filter_by_sources(all_remote_pools)
        # also do this with the host-path mapping
        configured_remote_pools_host_paths_mapping = {}
        for host_path, remote_pool in remote_pools_host_paths_mapping.items():
            configured_remote_pool = PoolList.merge(*self.config.filter_by_sources(remote_pool).values())
            configured_remote_pools_host_paths_mapping[host_path] = configured_remote_pool

        # we now have gathered all infos about our local and remote status.
        # we also know the status of the configured restricted view.

        # we only repair configured pools/datasets/snapshots

        # we should run a sanity check and verify, that both local and remote configured sources are equal
        # compare BackupSource objects for this
        assert not set(configured_local_pools_sources_mapping.keys()
                       ).difference(configured_remote_pools_sources_mapping.keys())

        # we should also check, if the remote side is missing any snapshots. if this is the case, we cannot restore
        # because we cannot restore intermediate snapshots without the full chain of snapshots (A->B->C is ok,
        # A->C is not, B is hardly needed for C).

        logical_remote_holes = find_restore_chain_holes(
            restore_source=PoolList.merge(*configured_remote_pools_sources_mapping.values()
                                          ).filter_include_by_zfs_path_prefix(restore_prefix),
            snapshot_prefix=self.config.snapshot_prefix)

        remote_holes: Dict[Tuple[Optional[SshHost], str], PoolList] = {}
        for remote_host_path, remote_pool in configured_remote_pools_host_paths_mapping.items():
            if self.cli_args.filter:
                real_remote_pool = remote_pool.filter_include_by_zfs_path_prefix(self.cli_args.filter)
            else:
                real_remote_pool = remote_pool
            if not real_remote_pool.has_snapshots():
                continue
            holes = find_restore_chain_holes(
                restore_source=real_remote_pool,
                snapshot_prefix=self.config.snapshot_prefix)
            if holes.has_snapshots():
                remote_holes[remote_host_path] = holes
        if logical_remote_holes.has_snapshots():
            print("Remote side has holes in the snapshot chain")
            print("This would fail the restore process")
            print("We need to repair the remote side first")
            for remote_host_path, holes in remote_holes.items():
                print("Holes on {}:{}".format(remote_host_path[0] or 'localhost', remote_host_path[1]))
                holes.print(with_incremental_base=self.cli_args.debug)
            sys.exit(1)

        # restore source does not have any holes, we can now build up a list of pools/datasets/snapshots that need to
        # be repaired logically

        # to restore the backup correctly, we must account for the shifting of the backup into another zfs path position
        # aka the restore target path. we need to find out, which snapshots are missing locally in that specific path.
        # -> we have to filter everything out, except the datasets that are under the restore path
        # the restore path can also be 'in place' aka '.', which means the backup is restored into the same zfs path
        # to find the corresponding remote sources, we must also shift all remote sources into the restore path, so
        # they can match with the local filtered view.
        # we can then find the missing snapshots on the local side by comparing the local view with the shifted remote
        # view. the difference is the set of effectively missing snapshots.
        # we can also change the size of this set by using the --incremental flag, which will only restore the latest
        # missing snapshots, not the whole chain from first missing snapshot to the last needed.

        local_repair_set = PoolList()
        for source in configured_remote_pools_sources_mapping.keys():
            # local_pool = configured_local_pools_sources_mapping[source]
            remote_pool = configured_remote_pools_sources_mapping[source]
            # also filter out datasets that are not included with the filter argument, this is for remote side before
            # shifting the view
            if self.cli_args.filter:
                remote_pool = remote_pool.filter_include_by_zfs_path_prefix(self.cli_args.filter)

            # do the shifting and filtering of the local and remote view
            local_pool = configured_local_backup_pools.filter_include_by_zfs_path_prefix(
                restore_prefix)  # filter out datasets
            remote_pool = remote_pool.prefixed_view(restore_prefix)  # shift the whole pool into the restore path

            repair_diff = find_repairable_snapshots(
                source_pools=remote_pool,
                target_pools=local_pool,
                incremental_only=self.cli_args.incremental)
            if repair_diff.has_snapshots():
                print("Repairing source: ", source.name)
                repair_diff.print(with_incremental_base=self.cli_args.debug)
                local_repair_set = PoolList.merge(local_repair_set, repair_diff)

        # we now have a set of pools/datasets/snapshots that need to be repaired logically.
        # we also grouped them together into one, and not a per configured source basis.

        # before we can restore the backup, we need to find out, if anything could conflict with the restore process
        # we need to check, if the restore path already contains intermediate backup snapshots
        # we need to check, if the restore path already contains datasets, if we have to restore initial snaphots

        conflicting_intermediate_snapshots = find_conflicting_intermediate_snapshots(
            repair_diff=local_repair_set,
            complete_target=all_local_pools,
            skip_sortability=False  # we can sort by the creation date, because we have full zfs access for local side
        )
        hard_conflicting_datasets = find_initial_conflicting_snapshots(
            repair_diff=local_repair_set,
            complete_target=all_local_pools,
        )

        # before we print infos about the conflicts, we should check if we can restore the needed snapshots
        # for this we have to complete the restore set with the needed additional snapshots. Those snapshots come from
        # the conflicting snapshot sets, which are not in the repair set.
        # the conflicting snapshots need to be re-created, so we can restore the local side correctly.
        # the conflicting snapshot sets also include snapshots, which might not belong to us, aka are not configured.
        # we need to filter out those, so we only re-create the snapshots, that are actually managed by us.
        local_repair_set = PoolList.merge(local_repair_set,
                                          PoolList.merge(
                                              *self.config.filter_by_sources(conflicting_intermediate_snapshots
                                                                             ).values()))
        local_repair_set = PoolList.merge(local_repair_set,
                                          PoolList.merge(*self.config.filter_by_sources(hard_conflicting_datasets
                                                                                        ).values()))

        # now we can attempt to actually restore them. but we also have to undo the shifting of the view, so we can
        # successfully map the needed snapshots to the available data sources.
        deshifted_local_repair_set = local_repair_set.prefixed_view(restore_prefix, deshift=True)
        try:
            restore_snapshots_with_source = map_snapshots_to_data_sources(deshifted_local_repair_set,
                                                                          configured_remote_pools_host_paths_mapping)
        except PlanningException as e:
            print("Error:")
            print("Could not map snapshots to sources:")
            print(e)
            sys.exit(1)

        if conflicting_intermediate_snapshots.has_snapshots():
            print()
            print("Conflicting intermediate snapshots:")
            conflicting_intermediate_snapshots.print(with_incremental_base=self.cli_args.debug)
        if hard_conflicting_datasets.has_snapshots():
            print()
            print("Hard conflicting datasets:")
            hard_conflicting_datasets.print(with_incremental_base=self.cli_args.debug)

        abort_due_to_conflicts = False
        if conflicting_intermediate_snapshots.has_snapshots() and not self.cli_args.force:
            print()
            print("Error:")
            print("Intermediate snapshots found in restore path")
            print("This would fail the restore process")
            print("Use --force to remove them")
            print("Alternatively you can use --incremental to only restore the latest missing snapshots.")
            print("The --force flag might still be needed in conjunction with --incremental.")
            abort_due_to_conflicts = True

        if hard_conflicting_datasets.has_snapshots() and not self.cli_args.replace:
            print()
            print("Error:")
            print("Found existing datasets, which would prevent the restore of the initial snapshot:")
            hard_conflicting_datasets.print(with_incremental_base=self.cli_args.debug)
            print("Cannot proceed without removing/replacing the conflicting datasets.")
            print("These existing snapshots AND datasets need to be removed or replaced in the restore target.")
            print("The datasets get replaced or deleted by this process.")
            print("Snapshots in the replaced datasets will be kept and moved to a new location. "
                  "More disk space is needed for this.")
            print("Use --replace to replace them (Warning: more space needed)")
            print("Use --replace AND --wipe to wipe them (Warning: permanent data loss)")
            abort_due_to_conflicts = True

        if abort_due_to_conflicts:
            print("Aborting...")
            sys.exit(1)

        # we now have a list of snapshots and the host-target-path-mappings from where we can fetch the repair data from
        if restore_prefix == '':
            # inplace restore
            self.backup_plan.restore_snapshots(
                restore_snapshots=restore_snapshots_with_source,
                replace_dataset=list(hard_conflicting_datasets.iter_datasets()),
                conflicting_snapshots=list(conflicting_intermediate_snapshots.filter_include_by_zfs_path_prefix(
                    self.cli_args.filter).iter_datasets()),
                restore_target=None,
                inplace=True,
                wipe_replacement=self.cli_args.wipe)
        else:
            # restore to given path
            self.backup_plan.restore_snapshots(
                restore_snapshots=restore_snapshots_with_source,
                replace_dataset=list(hard_conflicting_datasets.iter_datasets()),
                conflicting_snapshots=list(conflicting_intermediate_snapshots.filter_include_by_zfs_path_prefix(
                    self.cli_args.filter).iter_datasets()),
                restore_target=restore_prefix,
                inplace=False,
                wipe_replacement=self.cli_args.wipe)

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
                pools.print(with_incremental_base=self.cli_args.debug)
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

        # we should also check, if the remote side is missing any snapshots. if this is the case, we cannot restore
        # because we cannot restore intermediate snapshots without the full chain of snapshots (A->B->C is ok,
        # A->C is not).
        if self.cli_args.all:
            hole_scan_pools = PoolList.merge(*target_pools.values())
        else:
            hole_scan_pools = PoolList.merge(*backup_source_configured_pools_mapping.values())
        for remote_pool in hole_scan_pools:
            for dataset in remote_pool.iter_datasets():
                dataset_holes = find_snapshot_holes_of_dataset(dataset, self.config.snapshot_prefix)
                if dataset_holes.has_snapshots():
                    print("Dataset {} on remote side has holes in the snapshot chain".format(dataset.zfs_path))
                    print("This would fail the restore process")
                    print()
                    print("Holes:")
                    dataset_holes.print(with_incremental_base=self.cli_args.debug)
                    sys.exit(1)

        if self.cli_args.all:
            # we will now verify all datasets that are available on the remote side
            # we will not use the local pools for this, because we want to verify all datasets on all targets
            # we will use the remote pools as primary view

            invalid_snapshots = self.backup_plan.verify_snapshots(target_pools, self.cli_args.remove_corrupted,
                                                                  target_path_prefix_filter=self.cli_args.target_filter,
                                                                  zfs_path_prefix_filter=self.cli_args.filter)
        else:
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
                        verify_diff.print(with_incremental_base=self.cli_args.debug)
                        print()
                        if (target_group.remote, target_path) in verify_pools:
                            verify_pools[(target_group.remote, target_path)] = PoolList.merge(
                                verify_pools[(target_group.remote, target_path)],
                                verify_diff)
                        else:
                            verify_pools[(target_group.remote, target_path)] = verify_diff

            invalid_snapshots = self.backup_plan.verify_snapshots(verify_pools, self.cli_args.remove_corrupted)
        if any(p.has_snapshots() for p in invalid_snapshots.values()):
            print("Verification failed, found invalid snapshots:")
            for (host, target_path), pools in invalid_snapshots.items():
                print("Target: {}:{}".format(host or 'localhost', target_path))
                pools.print(with_incremental_base=self.cli_args.debug)
                print()
            sys.exit(1)
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
