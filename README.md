# ZFS Backup Tool

## CURRENTLY IN DEVELOPMENT

This tool is designed to back up ZFS datasets to a remote non-zfs-server or to a local directory,
like a mounted backup drive.

The tool will create a snapshot of the filesystem and send it to the remote server.
The saved snapshot is stored as a FILE.

Running the tool again will create an incremental snapshot and only send the changed data.

Its also possible to restore a snapshot from the remote server.


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
