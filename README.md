# ZFS Backup Tool

This tool is designed to back up ZFS datasets to a remote non-zfs-server via SSH or to a local directory,
like a mounted backup drive.

The zfs-backup-tool will create snapshots and zfs-send it to a local or remote target.
The saved snapshot is stored as a FILE.
After the snapshot is sent, the created files are checked for integrity via sha256sum.

Running the tool again will create an incremental snapshot and only send the changed data.

Its also possible to restore a snapshot from the remote server.

Source datasets and targets are configured via config-file.

It is also possible to replicate the same snapshot to multiple directories (multiple mounted backup drives) while only 
sending out the backup stream once.

## Requirements

### Local Machine
- Python 3
- ZFS
- ssh-client + SSH Key for remote server (only needed if remote server is used)
- pv
- sha256sum

### Remote Server
- ssh-server
- pv
- sha256sum
