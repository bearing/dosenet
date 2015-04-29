import socket;
import cust_crypt as ccrypt;

key_file_lst=['/home/testNE170/.ssh/id_rsa'];
pe=ccrypt.public_d_encrypt(key_file_lst=key_file_lst);

udp_ip="192.168.1.101";
udp_port=5000;

sock=socket.socket(socket.AF_INET,socket.SOCK_DGRAM);
sock.bind((udp_ip,udp_port));

while True:
    data,addr=sock.recvfrom(2048);
    print pe.decrypt_message((data,));
