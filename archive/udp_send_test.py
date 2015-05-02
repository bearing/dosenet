import socket
import time

UDP_IP = "192.168.1.101"
UDP_PORT = 5005
MESSAGE = "Hello, World!"

print "UDP target IP:", UDP_IP
print "UDP target port:", UDP_PORT
print "message:", MESSAGE

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # UDP

while True:
        sock.sendto(MESSAGE, (UDP_IP, UDP_PORT))
        print "Message sent"
        time.sleep(1)

