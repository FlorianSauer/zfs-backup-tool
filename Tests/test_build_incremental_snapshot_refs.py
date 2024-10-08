import unittest

from ZfsBackupTool.Constants import SNAPSHOT_PREFIX_POSTFIX_SEPARATOR, INITIAL_SNAPSHOT_POSTFIX
from ZfsBackupTool.Zfs import DataSet, Snapshot


class MyTestCase(unittest.TestCase):

    def test_build_incremental_snapshot_refs(self):
        dataset = DataSet("test", "test")

        snapshot_prefix = "test"
        initial_snapshot = Snapshot("test", "test",
                                    snapshot_prefix + SNAPSHOT_PREFIX_POSTFIX_SEPARATOR + INITIAL_SNAPSHOT_POSTFIX)
        snapshot_1 = Snapshot("test", "test", snapshot_prefix + SNAPSHOT_PREFIX_POSTFIX_SEPARATOR + "1")
        snapshot_2 = Snapshot("test", "test", snapshot_prefix + SNAPSHOT_PREFIX_POSTFIX_SEPARATOR + "2")
        snapshot_3 = Snapshot("test", "test", snapshot_prefix + SNAPSHOT_PREFIX_POSTFIX_SEPARATOR + "3")

        dataset.add_snapshot(initial_snapshot)
        dataset.add_snapshot(snapshot_1)
        dataset.add_snapshot(snapshot_2)
        dataset.add_snapshot(snapshot_3)

        dataset.build_incremental_snapshot_refs()

        self.assertFalse(initial_snapshot.has_incremental_base())

        self.assertTrue(snapshot_1.has_incremental_base())
        self.assertEqual(snapshot_1.get_incremental_base(), initial_snapshot)

        self.assertTrue(snapshot_2.has_incremental_base())
        self.assertEqual(snapshot_2.get_incremental_base(), snapshot_1)

        self.assertTrue(snapshot_3.has_incremental_base())
        self.assertEqual(snapshot_3.get_incremental_base(), snapshot_2)

    def test_build_incremental_snapshot_refs_with_gaps(self):
        dataset = DataSet("test", "test")

        snapshot_prefix = "test"
        initial_snapshot = Snapshot("test", "test",
                                    snapshot_prefix + SNAPSHOT_PREFIX_POSTFIX_SEPARATOR + INITIAL_SNAPSHOT_POSTFIX)
        snapshot_1 = Snapshot("test", "test", snapshot_prefix + SNAPSHOT_PREFIX_POSTFIX_SEPARATOR + "1")
        snapshot_2 = Snapshot("test", "test", snapshot_prefix + SNAPSHOT_PREFIX_POSTFIX_SEPARATOR + "2")
        snapshot_3 = Snapshot("test", "test", snapshot_prefix + SNAPSHOT_PREFIX_POSTFIX_SEPARATOR + "3")

        dataset.add_snapshot(initial_snapshot)
        dataset.add_snapshot(snapshot_1)
        # exclude snapshot_2
        # dataset.add_snapshot(snapshot_2)
        dataset.add_snapshot(snapshot_3)

        dataset.build_incremental_snapshot_refs()

        self.assertFalse(initial_snapshot.has_incremental_base())

        self.assertTrue(snapshot_1.has_incremental_base())
        self.assertEqual(snapshot_1.get_incremental_base(), initial_snapshot)

        self.assertFalse(snapshot_3.has_incremental_base())

        dataset.add_snapshot(snapshot_2)
        dataset.build_incremental_snapshot_refs()

        self.assertTrue(snapshot_2.has_incremental_base())
        self.assertEqual(snapshot_2.get_incremental_base(), snapshot_1)

        self.assertTrue(snapshot_3.has_incremental_base())
        self.assertEqual(snapshot_3.get_incremental_base(), snapshot_2)

    def test_no_backpropagate_incremental_snapshot_refs(self):
        dataset = DataSet("test", "test")

        snapshot_prefix = "test"
        initial_snapshot = Snapshot("test", "test",
                                    snapshot_prefix + SNAPSHOT_PREFIX_POSTFIX_SEPARATOR + INITIAL_SNAPSHOT_POSTFIX)
        snapshot_1 = Snapshot("test", "test", snapshot_prefix + SNAPSHOT_PREFIX_POSTFIX_SEPARATOR + "1")
        snapshot_2 = Snapshot("test", "test", snapshot_prefix + SNAPSHOT_PREFIX_POSTFIX_SEPARATOR + "2")
        snapshot_3 = Snapshot("test", "test", snapshot_prefix + SNAPSHOT_PREFIX_POSTFIX_SEPARATOR + "3")

        dataset.add_snapshot(initial_snapshot)
        dataset.add_snapshot(snapshot_1)
        dataset.add_snapshot(snapshot_2)
        dataset.add_snapshot(snapshot_3)

        # create view of dataset
        dataset_view = dataset.view()

        # build incremental refs on view
        dataset_view.build_incremental_snapshot_refs()

        # check that the view has incremental refs
        self.assertTrue(dataset_view.has_incremental_snapshot_refs())

        # check that the original dataset has no incremental refs
        self.assertFalse(dataset.has_incremental_snapshot_refs())

        # and also for the original snapshots
        self.assertFalse(initial_snapshot.has_incremental_base())
        self.assertFalse(snapshot_1.has_incremental_base())
        self.assertFalse(snapshot_2.has_incremental_base())
        self.assertFalse(snapshot_3.has_incremental_base())

    def test_inherit_incremental_snapshot_refs(self):
        dataset = DataSet("test", "test")

        snapshot_prefix = "test"
        initial_snapshot = Snapshot("test", "test",
                                    snapshot_prefix + SNAPSHOT_PREFIX_POSTFIX_SEPARATOR + INITIAL_SNAPSHOT_POSTFIX)
        snapshot_1 = Snapshot("test", "test", snapshot_prefix + SNAPSHOT_PREFIX_POSTFIX_SEPARATOR + "1")
        snapshot_2 = Snapshot("test", "test", snapshot_prefix + SNAPSHOT_PREFIX_POSTFIX_SEPARATOR + "2")
        snapshot_3 = Snapshot("test", "test", snapshot_prefix + SNAPSHOT_PREFIX_POSTFIX_SEPARATOR + "3")

        dataset.add_snapshot(initial_snapshot)
        dataset.add_snapshot(snapshot_1)
        dataset.add_snapshot(snapshot_2)
        dataset.add_snapshot(snapshot_3)

        # create view of dataset
        dataset_view = dataset.view()

        # original and view have no incremental refs
        self.assertFalse(dataset.has_incremental_snapshot_refs())
        self.assertFalse(dataset_view.has_incremental_snapshot_refs())

        # build incremental refs on original dataset
        dataset.build_incremental_snapshot_refs()

        # check that the original dataset has incremental refs now
        # but the view still has no incremental refs
        self.assertTrue(dataset.has_incremental_snapshot_refs())
        self.assertFalse(dataset_view.has_incremental_snapshot_refs())

        # re-create view of dataset
        dataset_view = dataset.view()

        # check that the view has incremental refs
        self.assertTrue(dataset_view.has_incremental_snapshot_refs())

        # verify that the original dataset snapshots use the same objects for the incremental refs
        self.assertTrue(dataset.snapshots[initial_snapshot.zfs_path] is initial_snapshot)
        self.assertTrue(dataset.snapshots[snapshot_1.zfs_path] is snapshot_1)
        self.assertTrue(dataset.snapshots[snapshot_1.zfs_path].get_incremental_base() is initial_snapshot)
        self.assertTrue(dataset.snapshots[snapshot_2.zfs_path] is snapshot_2)
        self.assertTrue(dataset.snapshots[snapshot_2.zfs_path].get_incremental_base() is snapshot_1)
        self.assertTrue(dataset.snapshots[snapshot_3.zfs_path] is snapshot_3)
        self.assertTrue(dataset.snapshots[snapshot_3.zfs_path].get_incremental_base() is snapshot_2)

        # verify that the view uses different objects for the incremental refs
        self.assertFalse(dataset_view.snapshots[initial_snapshot.zfs_path] is initial_snapshot)
        self.assertFalse(dataset_view.snapshots[snapshot_1.zfs_path] is snapshot_1)
        self.assertFalse(dataset_view.snapshots[snapshot_1.zfs_path].get_incremental_base() is initial_snapshot)
        self.assertFalse(dataset_view.snapshots[snapshot_2.zfs_path] is snapshot_2)
        self.assertFalse(dataset_view.snapshots[snapshot_2.zfs_path].get_incremental_base() is snapshot_1)
        self.assertFalse(dataset_view.snapshots[snapshot_3.zfs_path] is snapshot_3)
        self.assertFalse(dataset_view.snapshots[snapshot_3.zfs_path].get_incremental_base() is snapshot_2)

        # but uses the same objects for the incremental refs
        self.assertTrue(dataset_view.snapshots[snapshot_1.zfs_path].get_incremental_base()
                        is dataset_view.snapshots[initial_snapshot.zfs_path])
        self.assertTrue(dataset_view.snapshots[snapshot_2.zfs_path].get_incremental_base()
                        is dataset_view.snapshots[snapshot_1.zfs_path])
        self.assertTrue(dataset_view.snapshots[snapshot_3.zfs_path].get_incremental_base()
                        is dataset_view.snapshots[snapshot_2.zfs_path])

    def test_incremental_snapshot_detection(self):
        dataset = DataSet("test", "test")

        snapshot_prefix = "test"
        initial_snapshot = Snapshot("test", "test",
                                    snapshot_prefix + SNAPSHOT_PREFIX_POSTFIX_SEPARATOR + INITIAL_SNAPSHOT_POSTFIX)
        snapshot_1 = Snapshot("test", "test", snapshot_prefix + SNAPSHOT_PREFIX_POSTFIX_SEPARATOR + "1")
        snapshot_2 = Snapshot("test", "test", snapshot_prefix + SNAPSHOT_PREFIX_POSTFIX_SEPARATOR + "2")
        snapshot_3 = Snapshot("test", "test", snapshot_prefix + SNAPSHOT_PREFIX_POSTFIX_SEPARATOR + "3")

        other_snapshot_prefix = "other"
        other_initial_snapshot = Snapshot("test", "test",
                                          other_snapshot_prefix + SNAPSHOT_PREFIX_POSTFIX_SEPARATOR + INITIAL_SNAPSHOT_POSTFIX)
        other_snapshot_1 = Snapshot("test", "test", other_snapshot_prefix + SNAPSHOT_PREFIX_POSTFIX_SEPARATOR + "1")
        other_snapshot_2 = Snapshot("test", "test", other_snapshot_prefix + SNAPSHOT_PREFIX_POSTFIX_SEPARATOR + "2")
        other_snapshot_3 = Snapshot("test", "test", other_snapshot_prefix + SNAPSHOT_PREFIX_POSTFIX_SEPARATOR + "3")

        mixed_snapshot_1 = Snapshot("test", "test", 'foo' + SNAPSHOT_PREFIX_POSTFIX_SEPARATOR + "bar")
        mixed_snapshot_2 = Snapshot("test", "test", 'foo' + SNAPSHOT_PREFIX_POSTFIX_SEPARATOR + "baz")
        mixed_snapshot_3 = Snapshot("test", "test", 'foo' + SNAPSHOT_PREFIX_POSTFIX_SEPARATOR)
        mixed_snapshot_4 = Snapshot("test", "test", SNAPSHOT_PREFIX_POSTFIX_SEPARATOR + "bar")

        dataset.add_snapshot(initial_snapshot)
        dataset.add_snapshot(snapshot_1)
        dataset.add_snapshot(snapshot_2)
        dataset.add_snapshot(snapshot_3)

        dataset.add_snapshot(other_initial_snapshot)
        dataset.add_snapshot(other_snapshot_1)
        dataset.add_snapshot(other_snapshot_2)
        dataset.add_snapshot(other_snapshot_3)

        dataset.add_snapshot(mixed_snapshot_1)
        dataset.add_snapshot(mixed_snapshot_2)
        dataset.add_snapshot(mixed_snapshot_3)
        dataset.add_snapshot(mixed_snapshot_4)

        # create view of dataset
        dataset_view = dataset.view()

        # build incremental refs on view
        dataset_view.build_incremental_snapshot_refs()

        # check that the view has incremental refs
        self.assertTrue(dataset_view.has_incremental_snapshot_refs())

        # check that the original dataset has no incremental refs
        self.assertFalse(dataset.has_incremental_snapshot_refs())

        # and also for the original snapshots
        self.assertFalse(initial_snapshot.has_incremental_base())
        self.assertFalse(snapshot_1.has_incremental_base())
        self.assertFalse(snapshot_2.has_incremental_base())
        self.assertFalse(snapshot_3.has_incremental_base())

        self.assertFalse(other_initial_snapshot.has_incremental_base())
        self.assertFalse(other_snapshot_1.has_incremental_base())
        self.assertFalse(other_snapshot_2.has_incremental_base())
        self.assertFalse(other_snapshot_3.has_incremental_base())

        # build incremental refs on original dataset
        dataset.build_incremental_snapshot_refs()

        # verify datasets of both prefixes have incremental refs now and are in the correct order
        self.assertFalse(initial_snapshot.has_incremental_base())
        self.assertTrue(snapshot_1.has_incremental_base())
        self.assertEqual(snapshot_1.get_incremental_base(), initial_snapshot)
        self.assertTrue(snapshot_2.has_incremental_base())
        self.assertEqual(snapshot_2.get_incremental_base(), snapshot_1)
        self.assertTrue(snapshot_3.has_incremental_base())
        self.assertEqual(snapshot_3.get_incremental_base(), snapshot_2)

        self.assertFalse(other_initial_snapshot.has_incremental_base())
        self.assertTrue(other_snapshot_1.has_incremental_base())
        self.assertEqual(other_snapshot_1.get_incremental_base(), other_initial_snapshot)
        self.assertTrue(other_snapshot_2.has_incremental_base())
        self.assertEqual(other_snapshot_2.get_incremental_base(), other_snapshot_1)
        self.assertTrue(other_snapshot_3.has_incremental_base())
        self.assertEqual(other_snapshot_3.get_incremental_base(), other_snapshot_2)

        self.assertFalse(mixed_snapshot_1.has_incremental_base())
        self.assertFalse(mixed_snapshot_2.has_incremental_base())
        self.assertFalse(mixed_snapshot_3.has_incremental_base())
        self.assertFalse(mixed_snapshot_4.has_incremental_base())




if __name__ == '__main__':
    unittest.main()
