import unittest

from Tests.helpers import make_dataset, make_pool, make_poollist, pop_random_snapshot


class MyTestCase(unittest.TestCase):

    def test_dataset_diff(self):
        # DATASETS = ["pool/dataset1", "pool/dataset2", "pool/dataset3", "pool/dataset4", "pool/dataset5"]

        dataset_1 = make_dataset("test", "test", 10)
        dataset_2 = make_dataset("test", "test", 10)

        snapshot_names_1 = [snapshot.zfs_path for snapshot in dataset_1.iter_snapshots()]
        snapshot_names_2 = [snapshot.zfs_path for snapshot in dataset_2.iter_snapshots()]

        self.assertEqual(snapshot_names_1, snapshot_names_2)
        self.assertEqual(set(snapshot_names_1).difference(set(snapshot_names_2)),
                         {snapshot.zfs_path for snapshot in dataset_1.difference(dataset_2).iter_snapshots()})
        self.assertFalse(dataset_1.difference(dataset_2).has_snapshots())

        # pop random snapshot from snapshot_names_1
        random_snapshot = pop_random_snapshot(dataset_1)
        snapshot_names_1.remove(random_snapshot.zfs_path)

        # compare diff zfs paths
        diff = dataset_1.difference(dataset_2)
        set_diff = set(snapshot_names_1).difference(set(snapshot_names_2))
        self.assertEqual(set_diff, {snapshot.zfs_path for snapshot in diff.iter_snapshots()})
        reverse_diff = dataset_2.difference(dataset_1)
        reverse_set_diff = set(snapshot_names_2).difference(set(snapshot_names_1))
        self.assertEqual(reverse_set_diff, {snapshot.zfs_path for snapshot in reverse_diff.iter_snapshots()})

        # pop random snapshot from snapshot_names_2
        random_snapshot = pop_random_snapshot(dataset_2)
        snapshot_names_2.remove(random_snapshot.zfs_path)
        # pop random snapshot from snapshot_names_2
        random_snapshot = pop_random_snapshot(dataset_2)
        snapshot_names_2.remove(random_snapshot.zfs_path)

        # compare diff zfs paths
        diff = dataset_1.difference(dataset_2)
        set_diff = set(snapshot_names_1).difference(set(snapshot_names_2))
        self.assertEqual(set_diff, {snapshot.zfs_path for snapshot in diff.iter_snapshots()})
        reverse_diff = dataset_2.difference(dataset_1)
        reverse_set_diff = set(snapshot_names_2).difference(set(snapshot_names_1))
        self.assertEqual(reverse_set_diff, {snapshot.zfs_path for snapshot in reverse_diff.iter_snapshots()})

        # pop random snapshot from snapshot_names_1
        random_snapshot = pop_random_snapshot(dataset_1)
        snapshot_names_1.remove(random_snapshot.zfs_path)
        # pop random snapshot from snapshot_names_1
        random_snapshot = pop_random_snapshot(dataset_1)
        snapshot_names_1.remove(random_snapshot.zfs_path)

        # compare diff zfs paths
        diff = dataset_1.difference(dataset_2)
        set_diff = set(snapshot_names_1).difference(set(snapshot_names_2))
        self.assertEqual(set_diff, {snapshot.zfs_path for snapshot in diff.iter_snapshots()})
        reverse_diff = dataset_2.difference(dataset_1)
        reverse_set_diff = set(snapshot_names_2).difference(set(snapshot_names_1))
        self.assertEqual(reverse_set_diff, {snapshot.zfs_path for snapshot in reverse_diff.iter_snapshots()})

    def test_pool_diff(self):
        pool_1 = make_pool("test", 5, 5)
        pool_2 = make_pool("test", 5, 5)

        snapshot_names_1 = [snapshot.zfs_path for snapshot in pool_1.iter_snapshots()]
        snapshot_names_2 = [snapshot.zfs_path for snapshot in pool_2.iter_snapshots()]

        self.assertEqual(snapshot_names_1, snapshot_names_2)
        self.assertEqual(set(snapshot_names_1).difference(set(snapshot_names_2)),
                         {snapshot.zfs_path for snapshot in pool_1.difference(pool_2).iter_snapshots()})
        self.assertFalse(pool_1.difference(pool_2).has_snapshots())

        # pop random snapshot from snapshot_names_1
        random_snapshot = pop_random_snapshot(pool_1)
        snapshot_names_1.remove(random_snapshot.zfs_path)

        # compare diff zfs paths
        diff = pool_1.difference(pool_2)
        set_diff = set(snapshot_names_1).difference(set(snapshot_names_2))
        self.assertEqual(set_diff, {snapshot.zfs_path for snapshot in diff.iter_snapshots()})
        reverse_diff = pool_2.difference(pool_1)
        reverse_set_diff = set(snapshot_names_2).difference(set(snapshot_names_1))
        self.assertEqual(reverse_set_diff, {snapshot.zfs_path for snapshot in reverse_diff.iter_snapshots()})

        # pop random snapshot from snapshot_names_2
        random_snapshot = pop_random_snapshot(pool_2)
        snapshot_names_2.remove(random_snapshot.zfs_path)
        # pop random snapshot from snapshot_names_2
        random_snapshot = pop_random_snapshot(pool_2)
        snapshot_names_2.remove(random_snapshot.zfs_path)

        # compare diff zfs paths
        diff = pool_1.difference(pool_2)
        set_diff = set(snapshot_names_1).difference(set(snapshot_names_2))
        self.assertEqual(set_diff, {snapshot.zfs_path for snapshot in diff.iter_snapshots()})
        reverse_diff = pool_2.difference(pool_1)
        reverse_set_diff = set(snapshot_names_2).difference(set(snapshot_names_1))
        self.assertEqual(reverse_set_diff, {snapshot.zfs_path for snapshot in reverse_diff.iter_snapshots()})

        # pop random snapshot from snapshot_names_1
        random_snapshot = pop_random_snapshot(pool_1)
        snapshot_names_1.remove(random_snapshot.zfs_path)
        # pop random snapshot from snapshot_names_1
        random_snapshot = pop_random_snapshot(pool_1)
        snapshot_names_1.remove(random_snapshot.zfs_path)

        # compare diff zfs paths
        diff = pool_1.difference(pool_2)
        set_diff = set(snapshot_names_1).difference(set(snapshot_names_2))
        self.assertEqual(set_diff, {snapshot.zfs_path for snapshot in diff.iter_snapshots()})
        reverse_diff = pool_2.difference(pool_1)
        reverse_set_diff = set(snapshot_names_2).difference(set(snapshot_names_1))
        self.assertEqual(reverse_set_diff, {snapshot.zfs_path for snapshot in reverse_diff.iter_snapshots()})

    def test_poollist_diff(self):
        poollist_1 = make_poollist(5, 5, 5)
        poollist_2 = make_poollist(5, 5, 5)

        snapshot_names_1 = [snapshot.zfs_path for snapshot in poollist_1.iter_snapshots()]
        snapshot_names_2 = [snapshot.zfs_path for snapshot in poollist_2.iter_snapshots()]

        self.assertEqual(snapshot_names_1, snapshot_names_2)
        self.assertEqual(set(snapshot_names_1).difference(set(snapshot_names_2)),
                         {snapshot.zfs_path for snapshot in poollist_1.difference(poollist_2).iter_snapshots()})
        self.assertFalse(poollist_1.difference(poollist_2).has_snapshots())

        # pop random snapshot from snapshot_names_1
        random_snapshot = pop_random_snapshot(poollist_1)
        snapshot_names_1.remove(random_snapshot.zfs_path)

        # compare diff zfs paths
        diff = poollist_1.difference(poollist_2)
        set_diff = set(snapshot_names_1).difference(set(snapshot_names_2))
        self.assertEqual(set_diff, {snapshot.zfs_path for snapshot in diff.iter_snapshots()})
        reverse_diff = poollist_2.difference(poollist_1)
        reverse_set_diff = set(snapshot_names_2).difference(set(snapshot_names_1))
        self.assertEqual(reverse_set_diff, {snapshot.zfs_path for snapshot in reverse_diff.iter_snapshots()})

        # pop random snapshot from snapshot_names_2
        random_snapshot = pop_random_snapshot(poollist_2)
        snapshot_names_2.remove(random_snapshot.zfs_path)
        # pop random snapshot from snapshot_names_2
        random_snapshot = pop_random_snapshot(poollist_2)
        snapshot_names_2.remove(random_snapshot.zfs_path)

        # compare diff zfs paths
        diff = poollist_1.difference(poollist_2)
        set_diff = set(snapshot_names_1).difference(set(snapshot_names_2))
        self.assertEqual(set_diff, {snapshot.zfs_path for snapshot in diff.iter_snapshots()})
        reverse_diff = poollist_2.difference(poollist_1)
        reverse_set_diff = set(snapshot_names_2).difference(set(snapshot_names_1))
        self.assertEqual(reverse_set_diff, {snapshot.zfs_path for snapshot in reverse_diff.iter_snapshots()})

        # pop random snapshot from snapshot_names_1
        random_snapshot = pop_random_snapshot(poollist_1)
        snapshot_names_1.remove(random_snapshot.zfs_path)
        # pop random snapshot from snapshot_names_1
        random_snapshot = pop_random_snapshot(poollist_1)
        snapshot_names_1.remove(random_snapshot.zfs_path)

        # compare diff zfs paths
        diff = poollist_1.difference(poollist_2)
        set_diff = set(snapshot_names_1).difference(set(snapshot_names_2))
        self.assertEqual(set_diff, {snapshot.zfs_path for snapshot in diff.iter_snapshots()})
        reverse_diff = poollist_2.difference(poollist_1)
        reverse_set_diff = set(snapshot_names_2).difference(set(snapshot_names_1))
        self.assertEqual(reverse_set_diff, {snapshot.zfs_path for snapshot in reverse_diff.iter_snapshots()})



if __name__ == '__main__':
    unittest.main()
