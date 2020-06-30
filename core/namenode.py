from core.config import *
import os
import pickle
import threading
import socket

class NameNode(threading.Thread):
    def __init__(self, name, events, params_data):
        super(NameNode, self).__init__(name=name)
        self.params_data = params_data
        self.events = events
        self.id_chunk_map = None # file id -> chunk, eg. {0: ['file-0-0'], 1: ['file-1-0']}
        self.id_file_map = None # file id -> name, eg. {0: ('README.md', 1395), 1: ('mini_dfs.py', 14603)}
        self.chunk_server_map = None # chunk -> data servers, eg. {'file-0-0': [0, 1, 2], 'file-1-0': [0, 1, 2]}
        self.last_file_id = -1 # eg. 1
        self.last_data_server_id = -1 # eg. 2
        self.load_meta()

    def run(self):
        while True:
            # waiting for main.py
            self.events.name_event.wait()
            if self.params_data.cmd_type == COMMAND.put:
                self.put()
            elif self.params_data.cmd_type == COMMAND.read:
                self.read()
            elif self.params_data.cmd_type == COMMAND.fetch:
                self.fetch()
            elif self.params_data.cmd_type == COMMAND.ls:
                self.ls()
            elif self.params_data.cmd_type == COMMAND.delete:
                self.delete()
            elif self.params_data.cmd_type == COMMAND.ll:
                self.ll()
            self.events.name_event.clear()
            self.events.name_event_back.set()


    def load_meta(self):
        ''' 加载元数据 '''
        if not os.path.isfile(NAME_NODE_META_PATH):
            self.metas = {
                'id_chunk_map': {},
                'id_file_map': {},
                'chunk_server_map': {},
                'last_file_id': -1,
                'last_data_server_id': -1
            }
        else:
            with open(NAME_NODE_META_PATH, 'rb') as f:
                self.metas = pickle.load(f)
        self.id_chunk_map = self.metas['id_chunk_map']
        self.id_file_map = self.metas['id_file_map']
        self.chunk_server_map = self.metas['chunk_server_map']
        self.last_file_id = self.metas['last_file_id']
        self.last_data_server_id = self.metas['last_data_server_id']
    
    def update_meta(self):
        ''' 更新元数据 '''
        with open(NAME_NODE_META_PATH, 'wb') as f:
            self.metas['last_file_id'] = self.last_file_id
            self.metas['last_data_server_id'] = self.last_data_server_id
            self.metas['id_chunk_map'] = self.id_chunk_map
            self.metas['id_file_map'] = self.id_file_map
            self.metas['chunk_server_map'] = self.chunk_server_map
            pickle.dump(self.metas, f)


    def ls(self):
        ''' 显示文件列表 '''
        print('File total number', len(self.id_file_map))
        for file_id, (file_name, file_len) in self.id_file_map.items():
            print('%s\t%20s\t%10s' % (file_id, file_name, file_len))


    def ll(self):
        ''' 显示文件列表 '''
        # ['DataNode%s' %NUM_DATA_SERVER]
        closed_dataserver = []
        for i in range(0, NUM_DATA_SERVER):
            sock = self.create_socket(i)
            if sock != None:
                sock.close()
                continue
            closed_dataserver.append(i)
        print('id\tname\t\tsize\tchunk\t\tchunk_server')
        for id, chunk_list in self.id_chunk_map.items():
            file_length = self.id_file_map[id][1]
            num = len(chunk_list)
            chunk_size_list = [CHUNK_SIZE if i+1 < num else file_length%CHUNK_SIZE for i in range(num)]
            for chunk, chunk_size in zip(chunk_list, chunk_size_list):
                chunk_server_list = [-1  if i in closed_dataserver else i for i in self.chunk_server_map[chunk]]
                print('%s\t%s\t%s\t%s\t%s' % (id, self.id_file_map[id][0], chunk_size, chunk, chunk_server_list))


    def put(self):
        ''' 上传文件 '''
        in_path = self.params_data.file_path

        file_name = in_path.split('/')[-1]
        self.last_file_id += 1

        server_id = self.last_data_server_id

        file_length = os.path.getsize(in_path)
        chunks = int(math.ceil(float(file_length) / CHUNK_SIZE))

        # generate chunk, add into <id, chunk> mapping
        self.id_chunk_map[self.last_file_id] = [CHUNK_PATTERN % (self.last_file_id, i) for i in range(chunks)]
        self.id_file_map[self.last_file_id] = (file_name, file_length)

        self.params_data.server_chunk_map = {}
        for i, chunk in enumerate(self.id_chunk_map[self.last_file_id]):
            self.chunk_server_map[chunk] = []

            server_id = (server_id + 1 ) % NUM_DATA_SERVER
            # copy to 4 data nodes
            for j in range(NUM_REPLICATION):
                assign_server = (server_id + j) % NUM_DATA_SERVER
                self.chunk_server_map[chunk].append(assign_server)

                # add chunk-server info to global variable
                size_in_chunk = CHUNK_SIZE if i < chunks - 1 else file_length % CHUNK_SIZE
                if assign_server not in self.params_data.server_chunk_map:
                    self.params_data.server_chunk_map[assign_server] = []
                self.params_data.server_chunk_map[assign_server].append((chunk, CHUNK_SIZE * i, size_in_chunk))
        server_list = []
        if SYSTEM_Mode == 'DISTRIBUTED':
            for j in range(NUM_REPLICATION):
                assign_server = (server_id + j) % NUM_DATA_SERVER
                sock = self.create_socket(assign_server)
                if sock != None:
                    sock.close()
                    self.events.data_events[assign_server].set()
                    server_list.append(assign_server)
        if SYSTEM_Mode == 'LOCAL':
            for j in range(NUM_REPLICATION):
                assign_server = (server_id + j) % NUM_DATA_SERVER
                self.events.data_events[assign_server].set()
                server_list.append(assign_server)
            
        for i in server_list:
            self.events.data_events_back[i].wait()
            self.events.data_events_back[i].clear()
            
            
        self.last_data_server_id = server_id
        self.update_meta()

        self.params_data.file_id = self.last_file_id
        print('Put succeed! File_ID is %d' % (self.params_data.file_id,))



    def delete(self):
        ''' 上传文件 '''
        params_data = self.params_data
        file_id = params_data.file_id

        if file_id not in self.id_file_map:
            params_data.fetch_chunks = -1
            print('No such file with file_id =', file_id)
        else:
            file_chunks = self.id_chunk_map[file_id]

            for chunk in file_chunks:
                params_data.delete_chunk = chunk
                server_list = self.chunk_server_map[chunk]
                for i in server_list:
                    sock = self.create_socket(i)
                    if sock == None:
                        server_list.remove(i)
                    else: sock.close()
                for i in server_list:
                    self.events.data_events[i].set()
                for i in server_list:
                    self.events.data_events_back[i].wait()
                    self.events.data_events_back[i].clear()

            print('Detele succeed! File_ID is %d' % (params_data.file_id,))
            self.id_chunk_map.pop(file_id)
            self.id_file_map.pop(file_id)
            self.update_meta()


    def read(self):
        ''' 读取文件 '''
        params_data = self.params_data
        file_id = params_data.file_id
        read_offset = params_data.read_offset
        read_count = params_data.read_count

        # find file_id
        file_dir = params_data.file_dir

        if file_id not in self.id_file_map:
            print('No such file with file_id =', file_id)
            self.events.name_event_back.set()
        elif read_offset < 0 or read_count < 0:
            print('Read offset or count cannot less than 0')
            self.events.name_event_back.set()
        elif (read_offset + read_count) > self.id_file_map[file_id][1]:
            print('The expected reading exceeds the file, file size:', self.id_file_map[file_id][1])
            self.events.name_event_back.set()
        else:
            start_chunk = int(math.floor(read_offset / CHUNK_SIZE))
            space_left_in_chunk = (start_chunk + 1) * CHUNK_SIZE - read_offset

            if space_left_in_chunk < read_count:
                print('Cannot read across chunks')
                self.events.name_event_back.set()
            else:
                # randomly select a data server to read chunk
                read_server_candidates = self.chunk_server_map[CHUNK_PATTERN % (file_id, start_chunk)]
                read_server_id = choice(read_server_candidates)
                params_data.read_chunk = CHUNK_PATTERN % (file_id, start_chunk)
                params_data.read_offset = read_offset - start_chunk * CHUNK_SIZE
                if SYSTEM_Mode == 'LOCAL':
                    self.events.data_events[read_server_id].set()
                    self.events.data_events_back[read_server_id].wait()
                    self.events.data_events_back[read_server_id].clear()
                if SYSTEM_Mode == 'DISTRIBUTED':
                    for i in read_server_candidates:
                        sock = self.create_socket(i)
                        if sock != None:
                            sock.close()
                            self.events.data_events[i].set()
                            self.events.data_events_back[i].wait()
                            self.events.data_events_back[i].clear()
                            break
                return True

        return False


    def fetch(self):
        ''' 下载文件 '''
        if SYSTEM_Mode == 'DISTRIBUTED':
            params_data = self.params_data
            file_id = params_data.file_id

            if file_id not in self.id_file_map:
                params_data.fetch_chunks = -1
                print('No such file with file_id =', file_id)
            else:
                last_chunk = self.id_chunk_map[file_id][-1]
                for chunk in self.id_chunk_map[file_id]:
                    params_data.fetch_chunks = chunk
                    if chunk == last_chunk:
                        params_data.fetch_size = self.id_file_map[file_id][1] % CHUNK_SIZE
                    else: params_data.fetch_size = CHUNK_SIZE
                    for i in self.chunk_server_map[chunk]:
                        sock = self.create_socket(i)
                        if sock != None:
                            sock.close()
                            self.events.data_events[i].set()
                            self.events.data_events_back[i].wait()
                            self.events.data_events_back[i].clear()
                            print('DataServer-' + str(i), ' fetch', chunk)
                            break
                print('Finished download!')
                return True
            return None
        
        if SYSTEM_Mode == 'LOCAL':
            params_data = self.params_data
            file_id = params_data.file_id

            # find file_id
            file_dir = params_data.file_dir

            if file_id not in self.id_file_map:
                params_data.fetch_chunks = -1
                print('No such file with id =', file_id)
            else:
                file_chunks = self.id_chunk_map[file_id]
                params_data.fetch_chunks = len(file_chunks)
                for chunk in file_chunks:
                    params_data.fetch_servers.append(self.chunk_server_map[chunk][0])
            self.events.name_event_back.set()            


    def create_socket(self, server_id):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, BUFSIZE)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, BUFSIZE)
            sock.settimeout(TIME_OUT)
            sock.connect((DATA_NODE_ADDR[server_id], DATA_NODE_PORT))
        except Exception as e:
            print("DataNode ", server_id, "[Error]: {0}".format(e))
            return None
        return sock