import argparse
import os
import sys
from typing import Dict, Iterable, List

from humanfriendly import parse_size, format_size

from ZfsBackupTool.ResourcePacker import ResourcePacker, PackingError
from ZfsBackupTool.ShellCommand import ShellCommand
from ZfsBackupTool.Zfs import scan_zfs_pools, PoolList, ZfsResolveError, DataSet


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
                            help="Packing method to use. Use '1' for simple filling, '2' for bin packing algorithm. "
                                 "Default: '1'")
    cli_parser.add_argument('--write-config', type=str,
                            help="Generate a config file and write it to the specified path.")

    cli_parser.add_argument('-g', '--group', action='append',
                            help='Force the given datasets to be on the same disk.')

    # add positional argument to specify multiple source datasets
    cli_parser.add_argument("source_datasets", nargs="+",
                            help="Source datasets to plan backup groups for. Given strings are used as prefixes. "
                                 "To select a single dataset, use a trailing '/'.")

    def __init__(self):
        self.cli_args: argparse.Namespace = None  # type: ignore
        self.config: BackupSetup = None  # type: ignore
        self.shell_command: ShellCommand = None  # type: ignore

    def print_dataset_sizes(self, datasets: Iterable[DataSet]):
        print("Dataset sizes:")
        for dataset in datasets:
            print("  {}: {} ({})".format(dataset.zfs_path, dataset.dataset_size, format_size(dataset.dataset_size)))

    def re_expand_packets(self, packets: Iterable[Dict[str, int]],
                          grouped_datasets: Dict[str, PoolList]) -> List[Dict[str, int]]:
        expanded_packets = []
        for packet in packets:
            expanded_packet = {}
            for dataset_zfs_path, size in packet.items():
                if dataset_zfs_path in grouped_datasets:
                    for grouped_dataset in grouped_datasets[dataset_zfs_path].iter_datasets():
                        expanded_packet[grouped_dataset.zfs_path] = grouped_dataset.dataset_size
                else:
                    expanded_packet[dataset_zfs_path] = size
            expanded_packets.append(expanded_packet)
        return expanded_packets

    def filter_datasets(self, datasets_to_filter: PoolList, filters: List[str]) -> PoolList:
        filtered_datasets = PoolList()
        for dataset_filter in filters:
            if dataset_filter.endswith("*"):
                dataset_filter = dataset_filter[:-1]
                dataset_filter = dataset_filter[:-1] if dataset_filter.endswith("/") else dataset_filter
                filtered_datasets = PoolList.merge(filtered_datasets,
                                                   datasets_to_filter.filter_include_by_zfs_path_prefix(dataset_filter))
            elif dataset_filter.endswith("/"):
                dataset_filter = dataset_filter[:-1]
                try:
                    _dataset = datasets_to_filter.get_dataset_by_path(dataset_filter)
                except ZfsResolveError:
                    print("Dataset '{}' not found".format(dataset_filter))
                    continue
                single_dataset_poollist = PoolList()
                single_dataset_poollist.add_dataset(_dataset)
                filtered_datasets = PoolList.merge(filtered_datasets, single_dataset_poollist)
            else:
                filtered_datasets = PoolList.merge(filtered_datasets,
                                                   datasets_to_filter.filter_include_by_zfs_path_prefix(dataset_filter))
        return filtered_datasets

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

        datasets = self.filter_datasets(available_pools, self.cli_args.source_datasets)

        disk_priorities = [disk[0] for disk in self.cli_args.disk]

        disk_sizes_with_labels = {disk[0]: parse_size(disk[1]) for disk in self.cli_args.disk}

        disk_free_percentage = self.cli_args.disk_free_percentage

        if self.cli_args.write_config:
            # write Target-Group section for each disk
            with open(self.cli_args.write_config, "w") as f:
                for disk in disk_priorities:
                    f.write("[Target-Group {}]\n".format(disk))
                    f.write("path = /mnt/{}\n".format(disk))
                    f.write("\n")
                f.write("\n")

        for disk_priority_index, label in enumerate(disk_priorities):
            dataset_path_sizes_dict = {dataset.zfs_path: dataset.dataset_size for dataset in datasets.iter_datasets()}
            grouped_datasets: Dict[str, PoolList] = {}
            if self.cli_args.group:
                for group in self.cli_args.group:
                    placeholder_dataset_name = "/$!-" + os.urandom(8).hex()
                    # filter datasets by group
                    grouped_datasets[placeholder_dataset_name] = self.filter_datasets(datasets, [group, ])
                    # remove datasets from main list
                    for dataset in grouped_datasets[placeholder_dataset_name].iter_datasets():
                        dataset_path_sizes_dict.pop(dataset.zfs_path)
                    # sum up dataset sizes of group
                    group_size = sum([dataset.dataset_size
                                      for dataset in grouped_datasets[placeholder_dataset_name].iter_datasets()])
                    # add placeholder dataset to size dict
                    dataset_path_sizes_dict[placeholder_dataset_name] = group_size
            disk_size = disk_sizes_with_labels[label]
            disk_is_smallest_disk = disk_size == min(disk_sizes_with_labels.values())
            disk_is_not_smallest_and_last_disk = not (
                    disk_is_smallest_disk and disk_priority_index == len(disk_priorities) - 1)
            print("=========================================")
            print("Disk:", label)
            usage_size = 0
            try:
                packets = packer.getFragmentPackets(disk_size - int((disk_size * disk_free_percentage)),
                                                    dataset_path_sizes_dict,
                                                    allow_oversized=disk_is_not_smallest_and_last_disk)
            except PackingError as e:
                if self.cli_args.debug:
                    print("Dataset sizes:")
                    self.print_dataset_sizes(datasets.iter_datasets())
                    print("Packed packets:")
                    # re-replace placeholder dataset names with original dataset names
                    expanded_packets = self.re_expand_packets(e.packets, grouped_datasets)
                    print(repr(expanded_packets))
                print(e)
                print("Try to lower the disk_free_percentage or increase the disk size.")
                sys.exit(1)
            packets_dict = self.re_expand_packets(packets, grouped_datasets)
            if self.cli_args.debug:
                self.print_dataset_sizes(datasets.iter_datasets())
                print("Packed packets:")
                for packet_index, packet in enumerate(packets_dict):
                    print("Packet {}: ".format(packet_index))
                    for dataset_zfs_path in sorted(packet.keys()):
                        size = packet[dataset_zfs_path]
                        print("  {}: {} ({})".format(dataset_zfs_path, size, format_size(size)))

            if packets_dict:
                print("Packet content for disk {}:".format(label))
                for fragment, size in sorted(packets_dict[0].items(), key=lambda x: x[0]):
                    print("  {}: {} ({})".format(fragment, size, format_size(size)))
                    usage_size += size
                    dataset = datasets.get_dataset_by_path(fragment)
                    datasets.remove_dataset(dataset)

                if self.cli_args.write_config:
                    # append Source section
                    # it's not possible to group the datasets back together to the given source_datasets selectors.
                    # dataset children could be distributed to different disks.
                    with open(self.cli_args.write_config, "a") as f:
                        f.write("[Source Datasets for Disk {}]\n".format(label))
                        f.write("source = {}\n".format(
                            ", ".join([dataset for dataset in sorted(packets_dict[0].keys(), key=lambda x: x)])))
                        f.write("target = {}\n".format(label))
                        f.write("recursive = False\n")
                        f.write("\n")

            remaining_dataset_size = sum([dataset.dataset_size for dataset in datasets.iter_datasets()])
            if remaining_dataset_size + usage_size < (disk_size - (disk_size * disk_free_percentage)):
                for dataset in datasets.iter_datasets():
                    size = dataset.dataset_size
                    print("  {}: {}".format(dataset.zfs_path, size))
                    usage_size += size
                    datasets.pools[dataset.pool_name].remove_dataset(dataset)

            print("Disk size: {} ({})".format(disk_size, format_size(disk_size)))
            print("Usage size: {} ({})".format(usage_size, format_size(usage_size)))
            print("Disk free size: {} ({})".format(disk_size - usage_size, format_size(disk_size - usage_size)))
            print("Disk free percentage:", "{:.2f}%".format(100 - ((usage_size / disk_size) * 100)))

            if not remaining_dataset_size and disk_priority_index < len(disk_priorities) - 1:
                print("=========================================")
                print("All datasets mapped to disk. Skipping remaining disks.")
                return

        print("=========================================")
        print("=========================================")

        if datasets.has_snapshots():
            print("Remaining datasets:")
            for dataset in sorted(datasets.iter_datasets(), key=lambda x: x.zfs_path):
                print("  {}: {} ({})".format(dataset.zfs_path, dataset.dataset_size, format_size(dataset.dataset_size)))


if __name__ == "__main__":
    app = BackupGroupPlanner()
    app.run()
