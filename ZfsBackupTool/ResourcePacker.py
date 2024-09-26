import sys
from collections import OrderedDict
from typing import Iterable, List, Dict, Tuple


class ResourcePacker(object):
    BIN_PACKING = 1
    FILLING = 2

    def __init__(self, packing_method=FILLING):
        # type: (int) -> None
        self.packing_method = packing_method

    def getFragmentPackets(self, resource_size: int, fragments: Dict[str, int], allow_oversized: bool = False
                           ) -> List[Dict[str, int]]:
        if self.packing_method == self.BIN_PACKING:
            packets = self._get_binpacked_resources(resource_size, fragments.items())
        elif self.packing_method == self.FILLING:
            packets = self._get_sequential_filled_resources(resource_size, fragments.items())
        else:
            raise NotImplementedError
        if not allow_oversized and any(sum(packet.values()) > resource_size for packet in packets):
            raise PackingError(packets,
                               'Unable to form packets that do not exceed the given resource size of {}'.format(
                                   resource_size))
        return sorted(packets, key=lambda packet: sum(packet.values()), reverse=True)

    def _get_binpacked_resources(self, resource_size, fragments):
        # type: (int, Iterable[Tuple[str, int]]) -> List[Dict[str, int]]
        try:
            import binpacking
        except ImportError:
            print("Please install the 'binpacking' package to use the BIN_PACKING packing method.")
            sys.exit(1)
        return binpacking.to_constant_volume({fn: fs for fn, fs in fragments}, resource_size,
                                             upper_bound=resource_size + 1)

    def _get_sequential_filled_resources(self, resource_size, fragments):
        # type: (int, Iterable[Tuple[str, int]]) -> List[Dict[str, int]]
        datasets_sizes = {fn: fs for fn, fs in fragments}

        buckets: List[Dict[str, int]] = []
        while datasets_sizes:
            # try to fill existing buckets
            for bucket in buckets:
                for fragment in datasets_sizes:
                    if sum(bucket.values()) + datasets_sizes[fragment] <= resource_size:
                        bucket[fragment] = datasets_sizes[fragment]
                        del datasets_sizes[fragment]
            # create new bucket
            new_bucket = OrderedDict()
            new_bucket_size = 0
            for fragment in sorted(datasets_sizes, key=lambda x: datasets_sizes[x], reverse=True):
                if new_bucket_size + datasets_sizes[fragment] > resource_size:
                    continue
                new_bucket[fragment] = datasets_sizes[fragment]
                new_bucket_size += datasets_sizes[fragment]
            if not new_bucket:
                break
            for fragment in new_bucket:
                del datasets_sizes[fragment]
            buckets.append(new_bucket)
        return buckets


class PackingError(Exception):
    def __init__(self, packets: list, message: str):
        super(PackingError, self).__init__(message)
        self.packets = packets
