from core.config import *
import socket
import math, time

class DataNode(threading.Thread):
    
    def __init__(self, server_id, events, params_data):
        super(DataNode, self).__init__(name='DataServer%s' % (server_id,))
        self.events = events
        self.params_data = params_data
        self.server_id = server_id

    def run(self):
        while True:
            self.events.data_events[self.server_id].wait()
            if self.params_data.cmd_flag:
                if self.params_data.cmd_type == COMMAND.put and self.server_id in self.params_data.server_chunk_map:
                    self.save()
                elif self.params_data.cmd_type == COMMAND.read:
                    self.read()
                elif self.params_data.cmd_type == COMMAND.delete:
                    self.delete()
                elif self.params_data.cmd_type == COMMAND.fetch:
                    self.fetch()
                else:
                    pass
            self.events.data_events[self.server_id].clear()
            self.events.data_events_back[self.server_id].set()


    def save(self):
        ''' 存储 '''
        ''' 分布式代码 '''
        if SYSTEM_Mode == 'DISTRIBUTED':
            with open(self.params_data.file_path, 'r') as f_in:
                for chunk, offset, count in self.params_data.server_chunk_map[self.server_id]:
                    sock = self.create_socket()
                    num = math.ceil(count / 512)
                    command = ('save ' + chunk + ' ' + str(num) + ' ').encode('utf-8')
                    sock.send(command)
                    for i in range(0, num):
                        f_in.seek(offset + i*512, 0)
                        content = f_in.read(512)
                        sock.send(content.encode('utf-8'))
                    print('DataServer-'+ str(self.server_id) + ' save', chunk, offset, count)
                    sock.close()

        ''' 非分布式代码 '''
        if SYSTEM_Mode == 'LOCAL':
            data_node_dir = DATA_NODE_DIR % (self.server_id,)
            with open(self.params_data.file_path, 'r') as f_in:
                for chunk, offset, count in self.params_data.server_chunk_map[self.server_id]:
                    f_in.seek(offset, 0)
                    content = f_in.read(count)

                    with open(data_node_dir + os.path.sep + chunk, 'w') as f_out:
                        f_out.write(content)
                        f_out.flush()


    def delete(self):
        ''' 删除 '''
        ''' 分布式代码 '''
        if SYSTEM_Mode == 'DISTRIBUTED':
            sock = self.create_socket()
            command = "delete " + self.params_data.delete_chunk + " "
            sock.send(command.encode('utf-8'))
            content = sock.recv(30)
            print('DataServer-'+ str(self.server_id) + ' ' + content.decode('utf-8'))

        ''' 非分布式代码 '''
        if SYSTEM_Mode == 'LOCAL':
            read_path = (DATA_NODE_DIR % (self.server_id,)) + os.path.sep + self.params_data.delete_chunk
            if os.path.exists(read_path):
                os.remove(read_path)


    def read(self):
        ''' 根据偏移和数量 读取chunk '''
        ''' 分布式代码 '''
        if SYSTEM_Mode == 'DISTRIBUTED':
            sock = self.create_socket()
            command = "read " + self.params_data.read_chunk + ' ' + str(self.params_data.read_count)
            sock.send(command.encode('utf-8'))
            content = sock.recv(self.params_data.read_count, socket.MSG_WAITALL)
            sock.close()
            print('DataServer-'+ str(self.server_id) + ':\n' + content.decode('utf-8'))

        ''' 非分布式代码 '''
        if SYSTEM_Mode == 'LOCAL':
            read_path = (DATA_NODE_DIR % (self.server_id,)) + os.path.sep + self.params_data.read_chunk

            with open(read_path, 'r') as f_in:
                f_in.seek(self.params_data.read_offset)
                content = f_in.read(self.params_data.read_count)
                print(content)
    
    
    def fetch(self):
        ''' 下载文件 '''
        ''' 分布式代码 '''
        if SYSTEM_Mode == 'DISTRIBUTED':
            sock = self.create_socket()
            command = "fetch " + self.params_data.fetch_chunks + ' ' + str(self.params_data.fetch_size)
            sock.send(command.encode('utf-8'))
            content = []
            sock.settimeout(TIME_OUT)
            for i in range(0, math.ceil(self.params_data.fetch_size/512)):
                receive = sock.recv(512, socket.MSG_WAITALL)
                content.append(receive.decode('utf-8'))
            with open(self.params_data.fetch_savepath, 'a+') as f:
                f.write(''.join(content))
                f.flush()
            sock.close()


    def create_socket(self):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, BUFSIZE)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, BUFSIZE)
            sock.settimeout(TIME_OUT)
            sock.connect((DATA_NODE_ADDR[self.server_id], DATA_NODE_PORT))
        except Exception as e:
            print("DataNode ", self.server_id, "[Error]: {0}".format(e))
            return None
        return sock

