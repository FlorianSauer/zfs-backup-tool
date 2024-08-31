#!/usr/bin/env bash

set -e

# usage: make_test_file_M <path> <size>
function make_test_file_M() {
    dd if=/dev/urandom of="$1" bs=1M count="$2" iflag=fullblock
}

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

disk_0_file="/dev/shm/disk0"
disk_1_file="/dev/shm/disk1"

# cleanup
rm -f "$disk_0_file" "$disk_1_file"

truncate -s 2G "$disk_0_file"
truncate -s 2G "$disk_1_file"

zpool create -m /mnt/devpool devpool mirror "$disk_0_file" "$disk_1_file"

zfs create "devpool/dataset0"
make_test_file_M /mnt/devpool/dataset0/10MB.bin 10
zfs create "devpool/dataset1"
make_test_file_M /mnt/devpool/dataset1/10MB.bin 10
zfs create "devpool/dataset1/dataset1-1"
make_test_file_M /mnt/devpool/dataset1/dataset1-1/10MB.bin 10
zfs create "devpool/dataset1/dataset1-1/dataset1-1-1"
make_test_file_M /mnt/devpool/dataset1/dataset1-1/dataset1-1-1/10MB.bin 10
zfs create "devpool/dataset2"
make_test_file_M /mnt/devpool/dataset2/10MB.bin 10
zfs create "devpool/dataset2/dataset2-1"
make_test_file_M /mnt/devpool/dataset2/dataset2-1/10MB.bin 10
zfs create "devpool/dataset2/dataset2-2"
make_test_file_M /mnt/devpool/dataset2/dataset2-2/10MB.bin 10
zfs create "devpool/dataset3"
make_test_file_M /mnt/devpool/dataset3/10MB.bin 10
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
tmp_dir=$(mktemp -d)
mkdir -p "$tmp_dir"
(cd "$tmp_dir" && git clone "http://git.lan:3000/fsauer/zfs-backup-tool.git")
(cd "$tmp_dir/zfs-backup-tool" && git checkout "v1.0.0")

# backup the dev pool completely 3 times
(cd "$tmp_dir/zfs-backup-tool" && python3 ./zfs-backup-tool.py "$SCRIPT_DIR/dev_config_full_backup.conf" backup)
make_test_file_M /mnt/devpool/dataset0/20MB.bin 20
make_test_file_M /mnt/devpool/dataset1/20MB.bin 20
make_test_file_M /mnt/devpool/dataset1/dataset1-1/20MB.bin 20
make_test_file_M /mnt/devpool/dataset1/dataset1-1/dataset1-1-1/20MB.bin 20
make_test_file_M /mnt/devpool/dataset2/20MB.bin 20
make_test_file_M /mnt/devpool/dataset2/dataset2-1/20MB.bin 20
make_test_file_M /mnt/devpool/dataset2/dataset2-2/20MB.bin 20
make_test_file_M /mnt/devpool/dataset3/20MB.bin 20
(cd "$tmp_dir/zfs-backup-tool" && python3 ./zfs-backup-tool.py "$SCRIPT_DIR/dev_config_full_backup.conf" backup)
make_test_file_M /mnt/devpool/dataset0/30MB.bin 30
make_test_file_M /mnt/devpool/dataset1/30MB.bin 30
make_test_file_M /mnt/devpool/dataset1/dataset1-1/30MB.bin 30
make_test_file_M /mnt/devpool/dataset1/dataset1-1/dataset1-1-1/30MB.bin 30
make_test_file_M /mnt/devpool/dataset2/30MB.bin 30
make_test_file_M /mnt/devpool/dataset2/dataset2-1/30MB.bin 30
make_test_file_M /mnt/devpool/dataset2/dataset2-2/30MB.bin 30
make_test_file_M /mnt/devpool/dataset3/30MB.bin 30
(cd "$tmp_dir/zfs-backup-tool" && python3 ./zfs-backup-tool.py "$SCRIPT_DIR/dev_config_full_backup.conf" backup)
# and verify the backups
(cd "$tmp_dir/zfs-backup-tool" && python3 ./zfs-backup-tool.py "$SCRIPT_DIR/dev_config_full_backup.conf" verify)

# cleanup temp dir
rm -rf "$tmp_dir"

# make sure the storage is writable after the backup
chmod -R 777 /dev/shm/local_mirror_storage1
chmod -R 777 /dev/shm/local_mirror_storage2