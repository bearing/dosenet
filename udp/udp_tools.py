import socket
import cust_crypt as ccrypt;

class custSocket:
    def __init__(self,ip,port,decrypt):
        self.ip=ip;
        self.port=port;
        self.decrypt=decrypt;
        if( not decrypt.private_key ):
            print 'Warning: You will not be able to decrypt messages';
        self.sock=socket.socket(socket.AF_INET, socket.SOCK_DGRAM);
        self.sock.bind((ip,port));
    def listen(self):
        data,addr=self.sock.recvfrom(1024);
        data=self.decrypt_data(data);
        return data;
    def decrypt_data(self,data):
        return self.decrypt.decrypt_message((data,));
