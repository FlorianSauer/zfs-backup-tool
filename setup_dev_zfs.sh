#!/usr/bin/env bash

cd /dev/shm

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
