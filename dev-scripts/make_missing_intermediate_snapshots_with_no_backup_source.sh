#!/usr/bin/env bash

set -e

zfs destroy devpool/dataset0@test-backup.2

rm -rf /dev/shm/local_mirror_storage1/zfs/devpool/dataset0/test-backup.2.zfs*
rm -rf /dev/shm/local_mirror_storage2/zfs/devpool/dataset0/test-backup.2.zfs*
