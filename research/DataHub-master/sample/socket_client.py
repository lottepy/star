# -*- coding: utf-8 -*-
# from socket import *
import socket

HOST = '192.168.10.123'
PORT = 15566
BUFSIZ = 1024
ADDR = (HOST,PORT)
end = '\n'
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect((HOST, PORT))
    s.sendall(b'CN_10_600000\n')
    completed_data = ''
    n = 0
    while True:
        data = s.recv(1024)
        print(n, repr(data))
        n += 1

# while True:
# 	tcpCliSock = socket(AF_INET,SOCK_STREAM)
# 	tcpCliSock.connect(ADDR)
# 	data = input('> ')
# 	if not data:
# 		break
# 	tcpCliSock.send('%s\r\n' % data)
# 	data = tcpCliSock.recv(BUFSIZ)
# 	if not data:
# 		break
# 	print(data.strip())
	# tcpCliSock.close()

