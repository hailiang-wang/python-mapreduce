#!/usr/bin/env python
# -*- coding: utf-8 -*-
#===============================================================================
#
# Copyright (c) 2017 <> All Rights Reserved
#
#
# File: /Users/hain/git/python-mapreduce/src/mapreduce.py
# Author: Hai Liang Wang
# Date: 2017-11-21:16:46:37
#
#===============================================================================

"""
   
"""
from __future__ import print_function
from __future__ import division

__copyright__ = "Copyright (c) 2017 . All Rights Reserved"
__author__    = "Hai Liang Wang"
__date__      = "2017-11-21:16:46:37"


import os
import sys
curdir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(curdir)

from multiprocessing import Pool

if sys.version_info[0] < 3:
    reload(sys)
    sys.setdefaultencoding("utf-8")
    # raise "Must be using Python 3"


def echo_mapper(data):
    result = {}
    for x in data:
        o = x.split("\t")
        if len(o) == 2:
            words = o.split("")


    return result


class MapReduce():
    '''
    MapReduce Object
    '''

    def __init__(self, mapper_size = 5):
        self.mapper_size = mapper_size
        self.pool = Pool(processes=self.mapper_size)


    def partition_data(items):
        """Cuts data in parts. This parts is the data that will receive each of
        the workers
        :param items: Iterable containing all the data to process
        """
        # Get the number of data for each process (int for python 3)
        number_to_split = int(len(items) / self.mapper_size)

        # Create a list with lists
        for i in range(self.mapper_size + 1):
            yield(items[i * number_to_split:(i + 1) * (number_to_split)])

        # Add the remaining data
        remaining_group = items[(i + 1) * number_to_split:]
        if remaining_group:
            yield(remaining_group)


    def start(self, iterms, mapf, reducef):
        group_items = list(self.partition_data(iterms))
        sub_map_result = self.pool.map(mapf, group_items)
        return reducef(sub_map_result)

import unittest

# run testcase: python /Users/hain/git/python-mapreduce/src/mapreduce.py Test.testExample
class Test(unittest.TestCase):
    '''
    
    '''
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_start_job(self):
        print("test_start_job")


def test():
    unittest.main()

if __name__ == '__main__':
    test()
