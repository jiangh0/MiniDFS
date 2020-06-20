from core.config import *

class ParamsData():
    # global variables, shared between Name node and Data nodes
    server_chunk_map = {} # datanode -> chunks
    read_chunk = None
    read_offset = None
    read_count = None

    cmd_flag = False
    cmd_type = None
    file_id = None
    file_dir = None # read or fetch using dir/filename
    file_path = None # local source path

    delete_chunk = None
    put_savepath = None # put2 save using dir on DFS
    fetch_savepath = None # download on local
    fetch_servers = []
    fetch_chunks = None
    fetch_size = 0
    
    server_status = [True, True, True, True]

def deal_cmd(cmd_str, params_data):
    ''' 处理bash指令 '''
    cmds = cmd_str.split()
    cmd_flag, cmd_type = False, 0
    if len(cmds) >= 1 and cmds[0] in operation_names:
        if cmds[0] == operation_names[0]:
            if len(cmds) != 2:
                print('Usage: put source_file_path')
            else:
                if not os.path.isfile(cmds[1]):
                    print('Error: input file does not exist')
                else:
                    params_data.file_path = cmds[1]
                    cmd_type = COMMAND.put
                    cmd_flag = True
        elif cmds[0] == operation_names[1]:
            if len(cmds) != 4:
                print('Usage: read file_id offset count')
            else:
                try:
                    params_data.file_id = int(cmds[1])
                    params_data.read_offset = int(cmds[2])
                    params_data.read_count = int(cmds[3])
                except ValueError:
                    print('Error: fileid, offset, count should be integer')
                else:
                    cmd_type = COMMAND.read
                    cmd_flag = True
        elif cmds[0] == operation_names[2]:
            if len(cmds) != 3:
                print('Usage: fetch file_id save_path')
            else:
                params_data.fetch_savepath = cmds[2]
                base = os.path.split(params_data.fetch_savepath)[0]
                if len(base) > 0 and not os.path.exists(base):
                    print('Error: input save_path does not exist')
                else:
                    try:
                        params_data.file_id = int(cmds[1])
                    except ValueError:
                        print('Error: fileid should be integer')
                    else:
                        cmd_type = COMMAND.fetch
                        cmd_flag = True
        elif cmds[0] == operation_names[3]:
            if len(cmds) != 1:
                print('Usage: quit')
            else:
                print("Bye: Exiting miniDFS...")
                os._exit(0)
                cmd_flag = True
                cmd_type = COMMAND.quit
        elif cmds[0] == operation_names[4]:
            if len(cmds) != 1:
                print('Usage: ls')
            else:
                cmd_flag = True
                cmd_type = COMMAND.ls
        elif cmds[0] == operation_names[5]:
            if len(cmds) != 2:
                print('Usage: delete file_id')
            else:
                try:
                    params_data.file_id = int(cmds[1])
                except ValueError:
                    print('Error: fileid should be integer')
                else:
                    cmd_type = COMMAND.delete
                    cmd_flag = True
        elif cmds[0] == operation_names[6]:
            if len(cmds) != 1:
                print('Usage: ll')
            else:
                cmd_flag = True
                cmd_type = COMMAND.ll
        else:
            pass
    else:
        print('Usage: put|read|fetch|quit|ls|delete')

    return cmd_flag, cmd_type
