import unittest

from structures import *

class TestTree(unittest.TestCase):

    def test_tree1(self):
        tree = BPTree()
        
        self.assertEqual(tree.root.block_id, 0)
        self.assertEqual(tree.root.keys, [])
        self.assertEqual(tree.root.pointers, [None])
        self.assertEqual(tree.root.leaf, True)
        self.assertEqual(tree.root.max_keys, 10)
        self.assertEqual(tree.root.min_leaf_keys, 5)
        self.assertEqual(tree.root.min_non_leaf_keys, 5)
        
        tree.insert(5, ["5", 5.5, 5])
        self.assertEqual(tree.root.block_id, 0)
        self.assertEqual(tree.root.keys, [5])
        self.assertEqual(DataNode(tree.root.pointers[0]).records, [["5", 5.5, 5]])
        self.assertEqual(tree.root.pointers[1], None)
        self.assertEqual(tree.root.leaf, True)
        self.assertEqual(tree.root.max_keys, 10)
        self.assertEqual(tree.root.min_leaf_keys, 5)
        self.assertEqual(tree.root.min_non_leaf_keys, 5)

        tree.insert(1, ["1", 1.1, 1])
        self.assertEqual(tree.root.block_id, 0)
        self.assertEqual(tree.root.keys, [1, 5])
        self.assertEqual(DataNode(tree.root.pointers[0]).records, [["1", 1.1, 1]])
        self.assertEqual(tree.root.pointers[2], None)
        self.assertEqual(tree.root.leaf, True)
        self.assertEqual(tree.root.max_keys, 10)
        self.assertEqual(tree.root.min_leaf_keys, 5)
        self.assertEqual(tree.root.min_non_leaf_keys, 5)

    def test_tree2(self):
        tree = BPTree()
        for i in range(20):
            tree.insert(i, [f"{i}", 5.3, 3])
        