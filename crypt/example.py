import cust_crypt as ccrypt;

key_file_lst=['/home/testNE170/.ssh/id_rsa.pub','/home/testNE170/.ssh/id_rsa'];
pe=ccrypt.public_d_encrypt(key_file_lst=key_file_lst);

print pe.encrypt_message('yes'); #message to be sent over upd packet
print pe.decrypt_message(pe.encrypt_message('yes'));
