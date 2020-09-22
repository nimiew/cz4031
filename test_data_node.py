import unittest

from structures import *

class TestDataNode(unittest.TestCase):
    
    def test_data_node(self):
        data_node = DataNode()
        self.assertEqual(data_node.next_block_id, None)
        self.assertEqual(data_node.next_free_offset, 17)
        self.assertEqual(data_node.record_size, 18)
        self.assertEqual(data_node.records, [])
        self.assertEqual(data_node.get_values(), [])

        records = [
            ["asdf", 1.1, 1],
            ["bfew", 1.2, 2],
            ["cqwe", 1.3, 3],
            ["qwef", 1.4, 4],
            ["qefe", 1.5, 5],
            ["Ffew", 1.6, 6],
            ["fqwg", 1.7, 7],
            ["few3", 1.8, 8],
            ["afe3", 1.9, 9],
            ["afq3", 2.0, 10],
        ]

        data_node.append(records[0])
        self.assertEqual(data_node.next_block_id, None)
        self.assertEqual(data_node.next_free_offset, 35)
        self.assertEqual(data_node.record_size, 18)
        self.assertEqual(data_node.records, [records[0]])
        self.assertEqual(data_node.get_values(), [records[0]])

        data_node.append(records[1])
        self.assertEqual(data_node.next_block_id, None)
        self.assertEqual(data_node.next_free_offset, 53)
        self.assertEqual(data_node.record_size, 18)
        self.assertEqual(data_node.records, records[:2])
        self.assertEqual(data_node.get_values(), records[:2])

        data_node.append(records[2])
        self.assertEqual(data_node.next_block_id, None)
        self.assertEqual(data_node.next_free_offset, 71)
        self.assertEqual(data_node.record_size, 18)
        self.assertEqual(data_node.records, records[:3])
        self.assertEqual(data_node.get_values(), records[:3])

        data_node.append(records[3])
        self.assertEqual(data_node.next_block_id, None)
        self.assertEqual(data_node.next_free_offset, 89)
        self.assertEqual(data_node.record_size, 18)
        self.assertEqual(data_node.records, records[:4])
        self.assertEqual(data_node.get_values(), records[:4])

        data_node.append(records[4])
        self.assertEqual(data_node.next_block_id, 1)
        self.assertEqual(data_node.next_free_offset, 89)
        self.assertEqual(data_node.record_size, 18)
        self.assertEqual(data_node.records, records[:4])
        self.assertEqual(data_node.get_values(), records[:5])

        data_node.append(records[5])
        self.assertEqual(data_node.next_block_id, 1)
        self.assertEqual(data_node.next_free_offset, 89)
        self.assertEqual(data_node.record_size, 18)
        self.assertEqual(data_node.records, records[:4])
        self.assertEqual(data_node.get_values(), records[:6])

        data_node = DataNode(data_node.next_block_id)
        self.assertEqual(data_node.next_block_id, None)
        self.assertEqual(data_node.next_free_offset, 53)
        self.assertEqual(data_node.record_size, 18)
        self.assertEqual(data_node.records, records[4:6])
        self.assertEqual(data_node.get_values(), records[4:6])

        another_node = DataNode(0)
        self.assertEqual(another_node.next_block_id, 1)
        self.assertEqual(another_node.next_free_offset, 89)
        self.assertEqual(another_node.record_size, 18)
        self.assertEqual(another_node.records, records[:4])
        self.assertEqual(another_node.get_values(), records[:6])

        another_node = DataNode(1)
        self.assertEqual(another_node.next_block_id, None)
        self.assertEqual(another_node.next_free_offset, 53)
        self.assertEqual(another_node.record_size, 18)
        self.assertEqual(another_node.records, records[4:6])
        self.assertEqual(another_node.get_values(), records[4:6])

        new_node = DataNode()
        for record in records:
            new_node.append(record)
        self.assertEqual(new_node.get_values(), records)

        new_node_id = new_node.block_id
        new_node.deallocate() # will deallocate the whole chain
        self.assertEqual(Disk.read_block(new_node_id), Block())
        self.assertEqual(Disk.read_block(new_node_id+1), Block())
        self.assertEqual(Disk.read_block(new_node_id+2), Block())