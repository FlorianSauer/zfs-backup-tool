from ..ShellCommand import ShellCommand
from ..Zfs import Pool, DataSet, Snapshot

class BackupPlan(object):
    def __init__(self, shell_command: ShellCommand,
                 # snapshot_prefix: str
                 ):
        self.shell_command = shell_command
        # self.snapshot_prefix = snapshot_prefix

    def create_snapshots(self, *pools: Pool):
        for pool in pools:
            for dataset in pool:
                for snapshot in dataset:
                    self.shell_command.create_snapshot(dataset.zfs_path, snapshot.snapshot_name)

    def verify_pool(self, pool: Pool):
        # checks if the pool exists on the target pool
        # if not self.shell_command.target_dir_exists()
        # checks if all snapshots exist on the target pool and verifies them
        if not self.shell_command.target_dir_exists():
            return False

    def verify_snapshot(self, snapshot: Snapshot):
        # checks if the snapshot file exists on the target pool
        # if not self.shell_command.target_file_exists()
        # checks if the snapshot file has a checksum file beside it
        # checks if the checksum was calculated and stored in a second checksum file
        pass




