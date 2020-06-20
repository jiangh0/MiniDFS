# -*- coding: utf-8 -*-

from core.config import *
from core.init_server import *
from core.deal_cmd import *
from core.datanode import DataNode
from core.namenode import NameNode
import sys

def run(events, params_data):
    cmd_prompt = 'MiniDFS >>> '
    print(cmd_prompt, end='')
    while True:
        cmd_str = input()
        params_data.cmd_flag, params_data.cmd_type  = deal_cmd(cmd_str, params_data)

        if params_data.cmd_flag:
            if params_data.cmd_type == COMMAND.quit:
                sys.exit(0)

            # tell name node to process cmd
            events.name_event.set()

            if params_data.cmd_type in [COMMAND.ll, COMMAND.read, COMMAND.ls, COMMAND.put, COMMAND.delete]:
                events.name_event_back.wait()
                events.name_event_back.clear()
            elif params_data.cmd_type == COMMAND.fetch:
                if SYSTEM_Mode == 'LOCAL':
                    events.name_event_back.wait()
                    events.name_event_back.clear()
                    if params_data.fetch_chunks > 0:
                        f_fetch = open(params_data.fetch_savepath, mode='wb')
                        for i in range(params_data.fetch_chunks):
                            server_id = params_data.fetch_servers[i]
                            chunk_file_path = "dfs/datanode" + str(server_id) + "/file-" + str(params_data.file_id) + '-' + str(i)
                            chunk_file = open(chunk_file_path, "rb")
                            f_fetch.write(chunk_file.read())
                            chunk_file.close()
                        f_fetch.close()
                        print('Finished download!')
                if SYSTEM_Mode == 'DISTRIBUTED':
                    events.name_event_back.wait()
                    events.name_event_back.clear()

        print(cmd_prompt, end='')


if __name__ == '__main__':
    # events
    class Event:
        name_event = threading.Event()
        name_event_back = threading.Event()
        data_events = [threading.Event() for i in range(NUM_DATA_SERVER)]
        data_events_back = [threading.Event() for i in range(NUM_DATA_SERVER)]
    events = Event()
    # 公共变量，线程间传递参数
    params_data = ParamsData()
    # 启动节点
    init_name_node('NameNode', events, params_data)
    for i in range(NUM_DATA_SERVER):
        init_data_node('DataNode', i, events, params_data)

    # 运行主程序
    run(events, params_data)
