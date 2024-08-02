#!/usr/bin/env bash

set -e

rm -rf /dev/shm/local_mirror_storage1/zfs/devpool/dataset2/dataset2-1
rm -rf /dev/shm/local_mirror_storage1/zfs/devpool/dataset0/test-backup.1*

rm -rf /dev/shm/local_mirror_storage2/zfs/devpool/dataset2/dataset2-1
rm -rf /dev/shm/local_mirror_storage2/zfs/devpool/dataset1/test-backup.1.zfs*
rm -rf /dev/shm/local_mirror_storage2/zfs/devpool/dataset1/dataset1-1/test-backup.1.zfs*
rm -rf /dev/shm/local_mirror_storage2/zfs/devpool/dataset1/dataset1-1/dataset1-1-1/test-backup.1.zfs*