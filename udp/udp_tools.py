import socket
import cust_crypt as ccrypt

class custSocket:
    def __init__(self,ip,port,decrypt):
        self.ip = ip
        self.port = port
        self.decrypt = decrypt
        #if( not decrypt.private_key ):
        #    print 'Warning: You will not be able to decrypt messages'
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) #UDP ONLY
        try:
            self.sock.bind((ip,port))
        except Exception, e:
            print '\n\t\t ~~ IP address already in use ~~'
            print '\t\t ~~~~ This script is probably running already ~~~~'
            print '\t ~~ If not, there\'s some network issue. Good luck... ~~\n'
            raise e

    def listen(self):
        data, addr = self.sock.recvfrom(1024)
        data = self.decrypt_data(data)
        return data

    def decrypt_data(self,data):
        return self.decrypt.decrypt_message((data,))
