#!/usr/bin/env bash

set -e

# usage: make_test_file_M <path> <size>
function make_test_file_M() {
    dd if=/dev/urandom of="${1}/${2}MB.bin" bs=1M count="${2}" iflag=fullblock
}

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

disk_0_file="/dev/shm/disk0"
disk_1_file="/dev/shm/disk1"

# cleanup
rm -f "$disk_0_file" "$disk_1_file"

truncate -s 2G "$disk_0_file"
truncate -s 2G "$disk_1_file"

zpool create -m /mnt/devpool devpool mirror "$disk_0_file" "$disk_1_file"

test_file_size="1"
zfs create "devpool/dataset0"
make_test_file_M "/mnt/devpool/dataset0" "${test_file_size}"
zfs create "devpool/dataset1"
make_test_file_M "/mnt/devpool/dataset1" "${test_file_size}"
zfs create "devpool/dataset1/dataset1-1"
make_test_file_M "/mnt/devpool/dataset1/dataset1-1" "${test_file_size}"
zfs create "devpool/dataset1/dataset1-1/dataset1-1-1"
make_test_file_M "/mnt/devpool/dataset1/dataset1-1/dataset1-1-1" "${test_file_size}"
zfs create "devpool/dataset2"
make_test_file_M "/mnt/devpool/dataset2" "${test_file_size}"
zfs create "devpool/dataset2/dataset2-1"
make_test_file_M "/mnt/devpool/dataset2/dataset2-1" "${test_file_size}"
zfs create "devpool/dataset2/dataset2-2"
make_test_file_M "/mnt/devpool/dataset2/dataset2-2" "${test_file_size}"
zfs create "devpool/dataset3"
make_test_file_M "/mnt/devpool/dataset3" "${test_file_size}"
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

# backup the dev pool completely 4 times
(cd "$tmp_dir/zfs-backup-tool" && python3 ./zfs-backup-tool.py "$SCRIPT_DIR/dev_config_full_backup.conf" backup)
zfs snapshot -r "devpool@intermediate_snapshot.1"
test_file_size="2"
make_test_file_M "/mnt/devpool/dataset0" "${test_file_size}"
make_test_file_M "/mnt/devpool/dataset1" "${test_file_size}"
make_test_file_M "/mnt/devpool/dataset1/dataset1-1" "${test_file_size}"
make_test_file_M "/mnt/devpool/dataset1/dataset1-1/dataset1-1-1" "${test_file_size}"
make_test_file_M "/mnt/devpool/dataset2" "${test_file_size}"
make_test_file_M "/mnt/devpool/dataset2/dataset2-1" "${test_file_size}"
make_test_file_M "/mnt/devpool/dataset2/dataset2-2" "${test_file_size}"
make_test_file_M "/mnt/devpool/dataset3" "${test_file_size}"
(cd "$tmp_dir/zfs-backup-tool" && python3 ./zfs-backup-tool.py "$SCRIPT_DIR/dev_config_full_backup.conf" backup)
zfs snapshot -r "devpool@intermediate_snapshot.2"
test_file_size="3"
make_test_file_M "/mnt/devpool/dataset0" "${test_file_size}"
make_test_file_M "/mnt/devpool/dataset1" "${test_file_size}"
make_test_file_M "/mnt/devpool/dataset1/dataset1-1" "${test_file_size}"
make_test_file_M "/mnt/devpool/dataset1/dataset1-1/dataset1-1-1" "${test_file_size}"
make_test_file_M "/mnt/devpool/dataset2" "${test_file_size}"
make_test_file_M "/mnt/devpool/dataset2/dataset2-1" "${test_file_size}"
make_test_file_M "/mnt/devpool/dataset2/dataset2-2" "${test_file_size}"
make_test_file_M "/mnt/devpool/dataset3" "${test_file_size}"
(cd "$tmp_dir/zfs-backup-tool" && python3 ./zfs-backup-tool.py "$SCRIPT_DIR/dev_config_full_backup.conf" backup)
zfs snapshot -r "devpool@intermediate_snapshot.3"
test_file_size="4"
make_test_file_M "/mnt/devpool/dataset0" "${test_file_size}"
make_test_file_M "/mnt/devpool/dataset1" "${test_file_size}"
make_test_file_M "/mnt/devpool/dataset1/dataset1-1" "${test_file_size}"
make_test_file_M "/mnt/devpool/dataset1/dataset1-1/dataset1-1-1" "${test_file_size}"
make_test_file_M "/mnt/devpool/dataset2" "${test_file_size}"
make_test_file_M "/mnt/devpool/dataset2/dataset2-1" "${test_file_size}"
make_test_file_M "/mnt/devpool/dataset2/dataset2-2" "${test_file_size}"
make_test_file_M "/mnt/devpool/dataset3" "${test_file_size}"
(cd "$tmp_dir/zfs-backup-tool" && python3 ./zfs-backup-tool.py "$SCRIPT_DIR/dev_config_full_backup.conf" backup)
# and verify the backups
(cd "$tmp_dir/zfs-backup-tool" && python3 ./zfs-backup-tool.py "$SCRIPT_DIR/dev_config_full_backup.conf" verify)

# cleanup temp dir
rm -rf "$tmp_dir"

# make sure the storage is writable after the backup
chmod -R 777 /dev/shm/local_mirror_storage1
chmod -R 777 /dev/shm/local_mirror_storage2