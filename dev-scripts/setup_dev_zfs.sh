#!/usr/bin/env bash

set -e

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

disk_0_file="/dev/shm/disk0"
disk_1_file="/dev/shm/disk1"

# cleanup
rm -f "$disk_0_file" "$disk_1_file"

truncate -s 2G "$disk_0_file"
truncate -s 2G "$disk_1_file"

zpool create -m /mnt/devpool devpool mirror "$disk_0_file" "$disk_1_file"

zfs create "devpool/dataset0"
zfs create "devpool/dataset1"
zfs create "devpool/dataset1/dataset1-1"
zfs create "devpool/dataset1/dataset1-1/dataset1-1-1"
zfs create "devpool/dataset2"
zfs create "devpool/dataset2/dataset2-1"
zfs create "devpool/dataset2/dataset2-2"
zfs snapshot -r "devpool@initial_snapshot"

echo "ZFS pool created"
echo "for teardown run:"
echo "sudo zpool destroy devpool && sudo rm -f \"$disk_0_file\" \"$disk_1_file\""

mkdir -p "/dev/shm/local_mirror_storage1"
mkdir -p "/dev/shm/local_mirror_storage2"

# make sure the storage is writable
chmod -R 777 /dev/shm/local_mirror_storage1
chmod -R 777 /dev/shm/local_mirror_storage2

# install old version of zfs-backup-tool for basic testing
(cd /tmp && git clone "http://git.lan:3000/fsauer/zfs-backup-tool.git")
(cd /tmp/zfs-backup-tool && git checkout "v1.0.0")

# backup the dev pool completely 3 times
(cd /tmp/zfs-backup-tool && ./zfs-backup-tool.sh "$SCRIPT_DIR/dev-scripts/dev_config_full_backup.conf" backup)
(cd /tmp/zfs-backup-tool && ./zfs-backup-tool.sh "$SCRIPT_DIR/dev-scripts/dev_config_full_backup.conf" backup)
(cd /tmp/zfs-backup-tool && ./zfs-backup-tool.sh "$SCRIPT_DIR/dev-scripts/dev_config_full_backup.conf" backup)
