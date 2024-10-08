import unittest

from Tests.helpers import make_dataset, make_pool, make_poollist, pop_random_snapshot


class MyTestCase(unittest.TestCase):

    def test_dataset_intersection(self):
        dataset_1 = make_dataset("test", "test", 10)
        dataset_2 = make_dataset("test", "test", 10)

        snapshot_names_1 = [snapshot.zfs_path for snapshot in dataset_1.iter_snapshots()]
        snapshot_names_2 = [snapshot.zfs_path for snapshot in dataset_2.iter_snapshots()]

        self.assertEqual(snapshot_names_1, snapshot_names_2)
        self.assertEqual(set(snapshot_names_1).intersection(set(snapshot_names_2)),
                         {snapshot.zfs_path for snapshot in dataset_1.intersection(dataset_2).iter_snapshots()})
        self.assertTrue(dataset_1.intersection(dataset_2).has_snapshots())

        # pop random snapshot from snapshot_names_1
        random_snapshot = pop_random_snapshot(dataset_1)
        snapshot_names_1.remove(random_snapshot.zfs_path)
        
        # compare intersect zfs paths
        intersect = dataset_1.intersection(dataset_2)
        set_intersect = set(snapshot_names_1).intersection(set(snapshot_names_2))
        self.assertEqual(set_intersect, {snapshot.zfs_path for snapshot in intersect.iter_snapshots()})
        reverse_intersect = dataset_2.intersection(dataset_1)
        reverse_set_intersect = set(snapshot_names_2).intersection(set(snapshot_names_1))
        self.assertEqual(reverse_set_intersect, {snapshot.zfs_path for snapshot in reverse_intersect.iter_snapshots()})

        # pop random snapshot from snapshot_names_2
        random_snapshot = pop_random_snapshot(dataset_2)
        snapshot_names_2.remove(random_snapshot.zfs_path)
        # pop random snapshot from snapshot_names_2
        random_snapshot = pop_random_snapshot(dataset_2)
        snapshot_names_2.remove(random_snapshot.zfs_path)

        # compare intersect zfs paths
        intersect = dataset_1.intersection(dataset_2)
        set_intersect = set(snapshot_names_1).intersection(set(snapshot_names_2))
        self.assertEqual(set_intersect, {snapshot.zfs_path for snapshot in intersect.iter_snapshots()})
        reverse_intersect = dataset_2.intersection(dataset_1)
        reverse_set_intersect = set(snapshot_names_2).intersection(set(snapshot_names_1))
        self.assertEqual(reverse_set_intersect, {snapshot.zfs_path for snapshot in reverse_intersect.iter_snapshots()})

        # pop random snapshot from snapshot_names_1
        random_snapshot = pop_random_snapshot(dataset_1)
        snapshot_names_1.remove(random_snapshot.zfs_path)
        # pop random snapshot from snapshot_names_1
        random_snapshot = pop_random_snapshot(dataset_1)
        snapshot_names_1.remove(random_snapshot.zfs_path)

        # compare intersect zfs paths
        intersect = dataset_1.intersection(dataset_2)
        set_intersect = set(snapshot_names_1).intersection(set(snapshot_names_2))
        self.assertEqual(set_intersect, {snapshot.zfs_path for snapshot in intersect.iter_snapshots()})
        reverse_intersect = dataset_2.intersection(dataset_1)
        reverse_set_intersect = set(snapshot_names_2).intersection(set(snapshot_names_1))
        self.assertEqual(reverse_set_intersect, {snapshot.zfs_path for snapshot in reverse_intersect.iter_snapshots()})

    def test_pool_intersect(self):
        pool_1 = make_pool("test", 5, 5)
        pool_2 = make_pool("test", 5, 5)

        snapshot_names_1 = [snapshot.zfs_path for snapshot in pool_1.iter_snapshots()]
        snapshot_names_2 = [snapshot.zfs_path for snapshot in pool_2.iter_snapshots()]

        self.assertEqual(snapshot_names_1, snapshot_names_2)
        self.assertEqual(set(snapshot_names_1).intersection(set(snapshot_names_2)),
                         {snapshot.zfs_path for snapshot in pool_1.intersection(pool_2).iter_snapshots()})
        self.assertTrue(pool_1.intersection(pool_2).has_snapshots())

        # pop random snapshot from snapshot_names_1
        random_snapshot = pop_random_snapshot(pool_1)
        snapshot_names_1.remove(random_snapshot.zfs_path)

        # compare intersect zfs paths
        intersect = pool_1.intersection(pool_2)
        set_intersect = set(snapshot_names_1).intersection(set(snapshot_names_2))
        self.assertEqual(set_intersect, {snapshot.zfs_path for snapshot in intersect.iter_snapshots()})
        reverse_intersect = pool_2.intersection(pool_1)
        reverse_set_intersect = set(snapshot_names_2).intersection(set(snapshot_names_1))
        self.assertEqual(reverse_set_intersect, {snapshot.zfs_path for snapshot in reverse_intersect.iter_snapshots()})

        # pop random snapshot from snapshot_names_2
        random_snapshot = pop_random_snapshot(pool_2)
        snapshot_names_2.remove(random_snapshot.zfs_path)
        # pop random snapshot from snapshot_names_2
        random_snapshot = pop_random_snapshot(pool_2)
        snapshot_names_2.remove(random_snapshot.zfs_path)

        # compare intersect zfs paths
        intersect = pool_1.intersection(pool_2)
        set_intersect = set(snapshot_names_1).intersection(set(snapshot_names_2))
        self.assertEqual(set_intersect, {snapshot.zfs_path for snapshot in intersect.iter_snapshots()})
        reverse_intersect = pool_2.intersection(pool_1)
        reverse_set_intersect = set(snapshot_names_2).intersection(set(snapshot_names_1))
        self.assertEqual(reverse_set_intersect, {snapshot.zfs_path for snapshot in reverse_intersect.iter_snapshots()})

        # pop random snapshot from snapshot_names_1
        random_snapshot = pop_random_snapshot(pool_1)
        snapshot_names_1.remove(random_snapshot.zfs_path)
        # pop random snapshot from snapshot_names_1
        random_snapshot = pop_random_snapshot(pool_1)
        snapshot_names_1.remove(random_snapshot.zfs_path)

        # compare intersect zfs paths
        intersect = pool_1.intersection(pool_2)
        set_intersect = set(snapshot_names_1).intersection(set(snapshot_names_2))
        self.assertEqual(set_intersect, {snapshot.zfs_path for snapshot in intersect.iter_snapshots()})
        reverse_intersect = pool_2.intersection(pool_1)
        reverse_set_intersect = set(snapshot_names_2).intersection(set(snapshot_names_1))
        self.assertEqual(reverse_set_intersect, {snapshot.zfs_path for snapshot in reverse_intersect.iter_snapshots()})

    def test_poollist_intersect(self):
        poollist_1 = make_poollist(5, 5, 5)
        poollist_2 = make_poollist(5, 5, 5)

        snapshot_names_1 = [snapshot.zfs_path for snapshot in poollist_1.iter_snapshots()]
        snapshot_names_2 = [snapshot.zfs_path for snapshot in poollist_2.iter_snapshots()]

        self.assertEqual(snapshot_names_1, snapshot_names_2)
        self.assertEqual(set(snapshot_names_1).intersection(set(snapshot_names_2)),
                         {snapshot.zfs_path for snapshot in poollist_1.intersection(poollist_2).iter_snapshots()})
        self.assertTrue(poollist_1.intersection(poollist_2).has_snapshots())

        # pop random snapshot from snapshot_names_1
        random_snapshot = pop_random_snapshot(poollist_1)
        snapshot_names_1.remove(random_snapshot.zfs_path)

        # compare intersect zfs paths
        intersect = poollist_1.intersection(poollist_2)
        set_intersect = set(snapshot_names_1).intersection(set(snapshot_names_2))
        self.assertEqual(set_intersect, {snapshot.zfs_path for snapshot in intersect.iter_snapshots()})
        reverse_intersect = poollist_2.intersection(poollist_1)
        reverse_set_intersect = set(snapshot_names_2).intersection(set(snapshot_names_1))
        self.assertEqual(reverse_set_intersect, {snapshot.zfs_path for snapshot in reverse_intersect.iter_snapshots()})

        # pop random snapshot from snapshot_names_2
        random_snapshot = pop_random_snapshot(poollist_2)
        snapshot_names_2.remove(random_snapshot.zfs_path)
        # pop random snapshot from snapshot_names_2
        random_snapshot = pop_random_snapshot(poollist_2)
        snapshot_names_2.remove(random_snapshot.zfs_path)

        # compare intersect zfs paths
        intersect = poollist_1.intersection(poollist_2)
        set_intersect = set(snapshot_names_1).intersection(set(snapshot_names_2))
        self.assertEqual(set_intersect, {snapshot.zfs_path for snapshot in intersect.iter_snapshots()})
        reverse_intersect = poollist_2.intersection(poollist_1)
        reverse_set_intersect = set(snapshot_names_2).intersection(set(snapshot_names_1))
        self.assertEqual(reverse_set_intersect, {snapshot.zfs_path for snapshot in reverse_intersect.iter_snapshots()})

        # pop random snapshot from snapshot_names_1
        random_snapshot = pop_random_snapshot(poollist_1)
        snapshot_names_1.remove(random_snapshot.zfs_path)
        # pop random snapshot from snapshot_names_1
        random_snapshot = pop_random_snapshot(poollist_1)
        snapshot_names_1.remove(random_snapshot.zfs_path)

        # compare intersect zfs paths
        intersect = poollist_1.intersection(poollist_2)
        set_intersect = set(snapshot_names_1).intersection(set(snapshot_names_2))
        self.assertEqual(set_intersect, {snapshot.zfs_path for snapshot in intersect.iter_snapshots()})
        reverse_intersect = poollist_2.intersection(poollist_1)
        reverse_set_intersect = set(snapshot_names_2).intersection(set(snapshot_names_1))
        self.assertEqual(reverse_set_intersect, {snapshot.zfs_path for snapshot in reverse_intersect.iter_snapshots()})


if __name__ == '__main__':
    unittest.main()
