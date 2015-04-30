#!/usr/bin/python
# Navrit Bal
# Thursday 30 April 2015
# GET IP ADDRESS OF GRIM.NUC.BERKELEY.EDU
# From http://pymotw.com/2/socket/addressing.html - Looking Up Server Addresses

import socket
for response in socket.getaddrinfo('grim.nuc.berkeley.edu', 'http'):
    # Unpack the response tuple
    family, socktype, proto, canonname, sockaddr = response
    print 'Protocol: ', proto # Should be UDP if correct, could be TCP
    print 'IP Address: ', sockaddr[0] # Format ('IP Address',port)- eg. ('128.32.192.205',80)