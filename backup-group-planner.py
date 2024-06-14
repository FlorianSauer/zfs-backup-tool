import argparse
import sys

from ZfsBackupTool.DataSet import DataSet
from ZfsBackupTool.ResourcePacker import ResourcePacker
from ZfsBackupTool.ShellCommand import ShellCommand


class BackupGroupPlanner(object):
    cli_parser = argparse.ArgumentParser(description="Script to plan backup groups",
                                         formatter_class=argparse.RawTextHelpFormatter)
    cli_parser.add_argument('--debug', action='store_true', help='Debug output')
    cli_parser.add_argument('--version', action='version', version='%(prog)s 0.1')

    # add argument (can be given multiple times) to specify a disk with its size
    cli_parser.add_argument("--disk", "-d", action="append", nargs=2, metavar=("group_name", "size"),
                            help="Add a disk with its size. "
                                 "The order of the disks is the priority order. "
                                 "Disk size MUST be in bytes. "
                                 "Disk size can be determined with 'lsblk -b' or 'blockdev --getsize64 /dev/sdX'.")

    cli_parser.add_argument('--disk-free-percentage', type=float, default=0.15,
                            help="Percentage of disk space that should be kept free. "
                                 "Useful for growing datasets. "
                                 "Default: 0.15")

    cli_parser.add_argument('--packing-method', type=int, default=1,
                            help="Packing method to use. "
                                 "1: Filling "
                                 "2: Bin packing "
                                 "Default: 1")

    # add positional argument to specify multiple source datasets
    cli_parser.add_argument("source_datasets", nargs="+",
                            help="Source datasets to plan backup groups for.")

    def __init__(self):
        self.cli_args: argparse.Namespace = None  # type: ignore
        self.config: BackupSetup = None  # type: ignore
        self.shell_command: ShellCommand = None  # type: ignore

    def run(self):
        self.cli_args = self.cli_parser.parse_args(sys.argv[1:])
        self.shell_command = ShellCommand(echo_cmd=self.cli_args.debug)

        self.shell_command.program_is_installed("zfs")

        if not self.cli_args.disk:
            self.cli_parser.error("No disks specified with --disk")
            sys.exit(1)

        if self.cli_args.packing_method == 1:
            packer = ResourcePacker(ResourcePacker.FILLING)
        elif self.cli_args.packing_method == 2:
            packer = ResourcePacker(ResourcePacker.BIN_PACKING)
        else:
            self.cli_parser.error("Invalid packing method")
            sys.exit(1)

        if self.cli_args.debug:
            print(self.cli_args)

        source_datasets = []
        for dataset in self.cli_args.source_datasets:
            if '*' in dataset:
                source_datasets.extend(self.shell_command.get_datasets(dataset, recursive=True))
            else:
                source_datasets.extend(self.shell_command.get_datasets(dataset, recursive=False))

        disk_priorities = [disk[0] for disk in self.cli_args.disk]

        disk_sizes_with_labels = {disk[0]: int(disk[1]) for disk in self.cli_args.disk}

        disk_free_percentage = self.cli_args.disk_free_percentage

        datasets = []
        for source_dataset in source_datasets:
            datasets.append(DataSet(self.shell_command, source_dataset, set()))

        for label in disk_priorities:
            disk_size = disk_sizes_with_labels[label]
            print("=========================================")
            print("Disk:", label)
            usage_size = 0
            packets = packer.getFragmentPackets(disk_size - int((disk_size * disk_free_percentage)), datasets)
            print("Packet content:")
            for fragment, size in sorted(packets[0].items(), key=lambda x: x[0].zfs_path):
                print(f"  {fragment.zfs_path}: {size}")
                usage_size += size
                datasets.remove(fragment)

            remaining_dataset_size = sum([dataset.get_dataset_size() for dataset in datasets])
            if remaining_dataset_size + usage_size < disk_size:
                for dataset in datasets:
                    size = dataset.get_dataset_size()
                    print(f"  {dataset.zfs_path}: {size}")
                    usage_size += size
                    datasets.remove(dataset)

            print("Disk size: ", disk_size)
            print("Usage size:", usage_size)

        print("=========================================")
        print("=========================================")

        if datasets:
            print("Remaining datasets:")
            for dataset in sorted(datasets, key=lambda x: x.zfs_path):
                print(f"  {dataset.zfs_path}")


if __name__ == "__main__":
    app = BackupGroupPlanner()
    app.run()
