#!bin/bash
# From Mark's instruction document on Google Drive
echo $'\n\nHello $USER'
echo $'We are going to setup this dosimeter now...\n'
echo $'Getting Miniconda'
wget http://repo.continuum.io/miniconda/Miniconda-3.5.5-Linux-armv6l.sh
echo $'Installing Miniconda'
bash Miniconda-3.5.5-Linux-armv6l.sh
echo $'Updating PATH variable'
export PATH=~/miniconda/bin:$PATH
echo $'Getting pip'
wget https://bootstrap.pypa.io/get-pip.py
echo $'Install pip'
python get-pip.py
echo $'File cleanup'
rm Miniconda-3.5.5-Linux-armv6l.sh
rm get-pip.py
echo $'Update Miniconda and install numpy'
conda update conda
conda install numpy
echo $'Install raspberry pi packages with pip'
pip install datetime
pip install rpi.gpio
pip install Crypto
echo $'Install screen with apt-get'
sudo apt-get install screen
echo $'\n ------------ Clone GitHub repository - get ready with those GitHub login details'
cd ~
git clone https://github.com/bearing/dosenet.git
# http://www.awesomeweirdness.com/projects-diy/raspberrypi/setup-noip-client-raspberry-pi/
echo $'\n ------------ Setup Dynamic DNS service'
mkdir /home/pi/noip
cd /home/pi/noip
wget http://www.no-ip.com/client/linux/noip-duc-linux.tar.gz
tar vzxf noip-duc-linux.tar.gz
echo $'\n Cleanup'
rm noip-duc-linux.tar.gz
cd noip-2.1.9-1
echo $'\n Installation time - get ready with the ucbdosenet@gmail.com login details'
echo $'\n Make sure you use dosenetNUMBER.ddns.net eg. for raspberry01 - dosenet1.ddns.net'
sudo make
sudo make install
echo $'Start No-IP2 service'
echo $'\n -------------- INSERT /usr/local/bin/noip2 between lines fi and exit(0)'
sudo /usr/local/bin/noip2
sudo nano /etc/rc.local
sudo /usr/local/bin/noip2 -S
echo $'I recommend you restart now'
echo $': sudo reboot'
cd ~