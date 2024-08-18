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


if __name__ == '__main__':
    unittest.main()
