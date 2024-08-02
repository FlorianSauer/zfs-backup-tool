#!/usr/bin/env bash

set -e

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )


disk_0_file="/dev/shm/disk0"
disk_1_file="/dev/shm/disk1"

# cleanup
zpool destroy devpool
rm -f "$disk_0_file" "$disk_1_file"

rm -rf /dev/shm/local_mirror_storage1
rm -rf /dev/shm/local_mirror_storage2
