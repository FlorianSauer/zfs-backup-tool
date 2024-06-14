import sys
from collections import OrderedDict
from typing import Iterable, List, Dict

from ZfsBackupTool.DataSet import DataSet


class ResourcePacker(object):
    BIN_PACKING = 1
    FILLING = 2

    def __init__(self, packing_method=FILLING):
        # type: (int) -> None
        self.packing_method = packing_method

    def getFragmentPackets(self, resource_size: int, fragments: Iterable[DataSet], allow_oversized: bool=False
                           ) -> List[Dict[DataSet, int]]:
        if self.packing_method == self.BIN_PACKING:
            packets = self._get_binpacked_resources(resource_size, fragments)
        elif self.packing_method == self.FILLING:
            packets = self._get_sequential_filled_resources(resource_size, fragments)
        else:
            raise NotImplementedError
        if not allow_oversized and any(sum(packet.values()) > resource_size for packet in packets):
            raise PackingError(packets,
                               'Unable to form packets that do not exceed the given resource size of {}'.format(
                                   resource_size))
        return sorted(packets, key=lambda packet: sum(packet.values()), reverse=True)

    def checkPackagesReachMinimumFillLevel(self, packets, resource_size, minimum_fill_level):
        # type: (List[Dict[DataSet, int]], int, float) -> bool
        if minimum_fill_level > 1.0:
            raise ValueError("fill level greater than 100% | 1.0")
        if len(packets) == 2 and len(packets[1]) == 1 and (
                sum(packets[0].values()) + sum(packets[1].values()) > resource_size
                and
                sum(packets[0].values()) / resource_size < minimum_fill_level
        ):
            # edge case where fragment cache was pushed over the resource_size limit with the last fragment (in
            # packets[1]), however the other previous fragments combined can not reach the desired minimum_fill_level
            # occurrs with FILLING-mode
            return True
        return any(sum(packet.values()) / resource_size >= minimum_fill_level for packet in packets)

    def _get_binpacked_resources(self, resource_size, fragments):
        # type: (int, Iterable[DataSet]) -> List[Dict[DataSet, int]]
        try:
            import binpacking
        except ImportError:
            print("Please install the 'binpacking' package to use the BIN_PACKING packing method.")
            sys.exit(1)
        return binpacking.to_constant_volume({f: f.get_dataset_size() for f in fragments}, resource_size,
                                             upper_bound=resource_size + 1)

    def _get_sequential_filled_resources(self, resource_size, fragments):
        # type: (int, Iterable[DataSet]) -> List[Dict[DataSet, int]]
        buckets = []
        new_bucket = OrderedDict()
        new_bucket_size = 0
        for fragment in fragments:
            if new_bucket_size + fragment.get_dataset_size() > resource_size:
                buckets.append(new_bucket)
                new_bucket = OrderedDict()
                new_bucket_size = 0
            new_bucket[fragment] = fragment.get_dataset_size()
            new_bucket_size += fragment.get_dataset_size()
        if len(new_bucket):
            buckets.append(new_bucket)
        return buckets


class PackingError(Exception):
    def __init__(self, packets: list, message: str):
        super(PackingError, self).__init__(message)
        self.packets = packets
