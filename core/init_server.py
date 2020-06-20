import os
from core.config import *
from core.datanode import DataNode
from core.namenode import NameNode

def init_data_node(name, server_id, events, params_data):
    if SYSTEM_Mode == 'LOCAL':
        if not os.path.isdir("dfs"):
            for i in range(NUM_DATA_SERVER):
                os.makedirs("dfs/datanode%d"%i)

    data_server = DataNode(server_id, events, params_data)
    data_server.start()
    print(data_server.name, '启动')

def init_name_node(name, events, params_data):
    # make dfs dir
    if not os.path.isdir("dfs"):
        os.makedirs("dfs/namenode")

    name_server = NameNode('NameServer', events, params_data)
    name_server.start()
    print(name_server.name, '启动')