import argparse
import sys

from ZfsBackupTool.ResourcePacker import ResourcePacker, PackingError
from ZfsBackupTool.ShellCommand import ShellCommand
from ZfsBackupTool.Zfs import scan_zfs_pools, PoolList


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
    cli_parser.add_argument('--write-config', type=str,
                            help="Generate a config file and write it to the specified path.")

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

        available_pools = scan_zfs_pools(self.shell_command, include_dataset_sizes=True)
        source_datasets = []
        for dataset in self.cli_args.source_datasets:
            if dataset.endswith("*"):
                dataset = dataset[:-1]
                dataset = dataset[:-1] if dataset.endswith("/") else dataset
                source_datasets.append(available_pools.filter_include_by_zfs_path_prefix(dataset))
            else:
                source_datasets.append(available_pools.filter_include_by_zfs_path_prefix(dataset + '@'))

        disk_priorities = [disk[0] for disk in self.cli_args.disk]

        disk_sizes_with_labels = {disk[0]: int(disk[1]) for disk in self.cli_args.disk}

        disk_free_percentage = self.cli_args.disk_free_percentage

        datasets = PoolList.merge(*source_datasets)

        if self.cli_args.write_config:
            # write Target-Group section for each disk
            with open(self.cli_args.write_config, "w") as f:
                for disk in disk_priorities:
                    f.write("[Target-Group {}]\n".format(disk))
                    f.write("path = /mnt/{}\n".format(disk))
                    f.write("\n")
                f.write("\n")

        for i, label in enumerate(disk_priorities):
            disk_size = disk_sizes_with_labels[label]
            disk_is_smallest_disk = disk_size == min(disk_sizes_with_labels.values())
            disk_is_not_smallest_and_last_disk = not (disk_is_smallest_disk and i == len(disk_priorities) - 1)
            print("=========================================")
            print("Disk:", label)
            usage_size = 0
            try:
                packets = packer.getFragmentPackets(disk_size - int((disk_size * disk_free_percentage)),
                                                    datasets.iter_datasets(),
                                                    allow_oversized=disk_is_not_smallest_and_last_disk)
            except PackingError as e:
                if self.cli_args.debug:
                    dataset_size_dict = {dataset.zfs_path: self.shell_command.get_dataset_size(dataset.zfs_path,
                                                                                               recursive=False)
                                         for dataset in datasets}
                    print("Dataset sizes:")
                    print(repr(dataset_size_dict))
                    print("Packed packets:")
                    packets_dict = [{k.zfs_path: v for k, v in d.items()} for d in e.packets]
                    print(repr(packets_dict))
                print(e)
                print("Try to lower the disk_free_percentage or increase the disk size.")
                sys.exit(1)
            if self.cli_args.debug:
                dataset_size_dict = {dataset.zfs_path: self.shell_command.get_dataset_size(dataset.zfs_path,
                                                                                           recursive=False)
                                     for dataset in datasets}
                print("Dataset sizes:")
                print(repr(dataset_size_dict))
                print("Packed packets:")
                packets_dict = [{k.zfs_path: v for k, v in d.items()} for d in packets]
                print(repr(packets_dict))

            if packets:
                print("Packet content:")
                for fragment, size in sorted(packets[0].items(), key=lambda x: x[0].zfs_path):
                    print("  {}: {}".format(fragment.zfs_path, size))
                    usage_size += size
                    datasets.pools[fragment.pool_name].remove_dataset(fragment)

                if self.cli_args.write_config:
                    # append Source section
                    with open(self.cli_args.write_config, "a") as f:
                        f.write("[Source {}]\n".format(
                            ",".join([dataset.zfs_path
                                      for dataset in sorted(packets[0].keys(), key=lambda x: x.zfs_path)])))
                        f.write("source = {}\n".format(
                            ", ".join([dataset.zfs_path
                                       for dataset in sorted(packets[0].keys(), key=lambda x: x.zfs_path)])))
                        f.write("target = {}\n".format(label))
                        f.write("recursive = False\n")
                        f.write("\n")

            remaining_dataset_size = sum([dataset.dataset_size for dataset in datasets.iter_datasets()])
            if remaining_dataset_size + usage_size < disk_size:
                for dataset in datasets.iter_datasets():
                    size = dataset.dataset_size
                    print("  {}: {}".format(dataset.zfs_path, size))
                    usage_size += size
                    datasets.pools[dataset.pool_name].remove_dataset(dataset)

            print("Disk size: ", disk_size)
            print("Usage size:", usage_size)
            print("Disk free size:", disk_size - usage_size)
            print("Disk free percentage:", "{:.2f}%".format(100 - ((usage_size / disk_size) * 100)))

            if not remaining_dataset_size and i < len(disk_priorities) - 1:
                print("=========================================")
                print("All datasets mapped to disk. Skipping remaining disks.")
                return

        print("=========================================")
        print("=========================================")

        if datasets:
            print("Remaining datasets:")
            for dataset in sorted(datasets, key=lambda x: x.zfs_path):
                print(f"  {dataset.zfs_path}")


if __name__ == "__main__":
    app = BackupGroupPlanner()
    app.run()
