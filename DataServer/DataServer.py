#!/usr/bin/python
# -*- coding: utf-8 -*-
import socket
import os
import json, math

PORT = 20000  # 随机的端口号
BUFSIZE = 10*1024*1024  # 将缓冲区大小设置为 2MB
SHARE_DIR = '/home/hadoop'

def run():
    sock = create_socket()
    print('[info]：Server start at port: {0}'.format(PORT))

    while True:
        print('[info]：wait for connection...')
        conn, addr = sock.accept()
        print('[info]：Connected by {0}:{1}'.format(addr[0], addr[1]))
        receive_socket(conn)
        conn.close()
    sock.close()


def create_socket():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(('192.168.0.128', PORT))
    sock.listen(5)
    sock.setsockopt(
            socket.SOL_SOCKET,
            socket.SO_SNDBUF,
            BUFSIZE)
    sock.setsockopt(
            socket.SOL_SOCKET,
            socket.SO_RCVBUF,
            BUFSIZE)
    return sock

    
def receive_socket(conn):
    try:
        
        res = conn.recv(40)
        cmds = res.decode('utf-8').split()

        if cmds[0] == 'read':
            file_name = cmds[1]
            read_count = int(cmds[2])
            read_path = SHARE_DIR + os.path.sep + file_name
            with open(read_path, 'r') as f:
                content = f.read(read_count)
                conn.send(content.encode('utf-8'))
                print("----------" + file_name + "----------read successful---------")
        elif cmds[0] == 'save':
            file_name = cmds[1]
            num = int(cmds[2])
            content = []
            for i in range(0, num):
                receive = conn.recv(512, socket.MSG_WAITALL).decode('utf-8')
                content.append(receive)
            content = ' '.join(cmds[3:]) + ''.join(content)
            with open(SHARE_DIR + os.path.sep + file_name, 'w') as f_out:
                f_out.write(content)
                f_out.flush()
            print("----------" + file_name + "----------save successful---------")

        elif cmds[0] == 'delete':
            read_path = SHARE_DIR + os.path.sep + cmds[1]
            if os.path.exists(read_path):
                os.remove(read_path)
                conn.send((cmds[1] + ' delete successful').encode('utf-8'))
                print("----------" + read_path + "----------delete successful---------")
            else:
                conn.send(cmds[1] + ' chunk not esist')
        elif cmds[0] == 'fetch':
            file_name = cmds[1]
            read_count = int(cmds[2])
            with open(SHARE_DIR + os.path.sep + file_name, 'r') as f:
                for i in range(0, int(math.ceil(read_count/512))):
                    f.seek(512*i, 0)
                    content = f.read(512)
                    conn.send(content.encode('utf-8'))
            print("----------" + file_name + "----------fetch successful---------")
    except Exception as e:
        print("[Error]: {0}".format(e))


if __name__ == "__main__":
    run()

