import unittest

from structures import Disk
from utils import *

class TestDisk(unittest.TestCase):

    def test_disk(self):
        self.assertEqual(Disk.next_free_idx, 1)
        self.assertEqual(len(Disk.free_queue), 0)
        
        # Must write back to disk for changes to be seen
        idx = Disk.get_next_free()
        block = Disk.read_block(idx)
        block.bytes[0] = 10
        self.assertNotEqual(block, Disk.read_block(idx))
        Disk.write_block(idx, block)
        self.assertEqual(block, Disk.read_block(idx))

        idx = Disk.get_next_free()
        self.assertEqual(idx, 2)
        idx = Disk.get_next_free()
        self.assertEqual(idx, 3)
        Disk.deallocate(1)
        self.assertEqual(len(Disk.free_queue), 1)
        idx = Disk.get_next_free()
        self.assertEqual(idx, 1)
        self.assertEqual(len(Disk.free_queue), 0)
        idx = Disk.get_next_free()
        self.assertEqual(idx, 4)
        Disk.deallocate(4)
        Disk.deallocate(2)
        self.assertEqual(len(Disk.free_queue), 2)
        idx = Disk.get_next_free()
        self.assertEqual(idx, 4)
        idx = Disk.get_next_free()
        self.assertEqual(idx, 2)