# -*- coding: utf-8 -*-
import os
import pickle
import math
import threading
import sys
from random import choice
from enum import Enum

# DFS Meta Data
CHUNK_SIZE = 2 * 1024 * 1024
BUFSIZE = 4 * 1024 * 1024
NUM_DATA_SERVER = 4
NUM_REPLICATION = 4
# Data Node Addr
DATA_NODE_ADDR = ['121.37.138.6', '124.70.177.49', '124.70.128.29', '124.70.153.253']
DATA_NODE_PORT = 20000
CHUNK_PATTERN = 'file-%s-%s'

TIME_OUT = 3

SYSTEM_Mode = 'DISTRIBUTED'  # LOCAL/DISTRIBUTED  本地/分布式

# Name Node Meta Data
NAME_NODE_META_PATH = './dfs/namenode/meta.pkl'
# Local Data Node
DATA_NODE_DIR = './dfs/datanode%s'


# Operations
operation_names = ('put', 'read', 'fetch', 'quit', 'ls', 'delete', 'll')
COMMAND = Enum('COMMAND', operation_names)
