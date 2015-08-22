![](http://i.imgur.com/dVADwI5.png =512x "DoseNet logo")
# Berkeley RadWatch Dosimeter Network
# *LBL / UC Berkeley*
### Author: Navrit Bal
##### Navrit Bal, Tigran Ter-Stepanyan, Mark Trudel, Nathan Richner
##### Joseph Curtis, Ryan Pavlovsky, Ali Hanks
##### Kai Vetter
---
## What goes where?
This is all contained in the GitHub folder, stored by convention in 'dosenet' in the user's home folder. eg. `cd ~/dosenet/ or cd ~/git/dosenet` would get you where you want to be.
This repo should always be cloned by SSH:
>		git clone git@github.com:bearing/dosenet.git

### Raspberry Pi
	udp_sender.py --test(-t) (Optional) filename(-f) (Optional[str]) --led (Optional[int]) --ip (Optional[str])

>Example:

>		sudo ./udp_sender.py -f config-files/etchhall.csv

> Must be launched as sudo (it accesses GPIO pins and low-level networking APIs)
> `sudo ./udp_sender.py`

	dosimeter.py
> Usage: 
>> **Test**: python dosimeter.py
>
> Supporting class for the UDP_sender class

	config-files/*.csv
> Each station has it's own CSV file (Headers: stationID, hash, lat, long).
	There are test CSV files.

	id_rsa_dosenet.pub
>	A **private key** used for the 'half-encrypted' stage between Raspberry Pis and the database server (GRIM).

---
### DoseNet server - Database & Listener
#### dosenet.dhcp.lbl.gov
##### Used to be grim.nuc.berkeley.edu
	makeGeoJSON.py
> Usage: 
>> **Automatic**: crontab operation
>> **Manual**: python makeGeoJSON.py

> Input: *None*
> Output: **output.geojson**
>
> Updates Plot.ly graphs via cron job operation - currently set to every 5 minutes.
> Copies (via scp) the produced GeoJSON file to Kepler - at end of run-time.

	udp_injector.py
> Usage: 
>> **Manual**: python udp_injector.py
>> **tmux**: tmux a -t UDP_injector; python udp_injector.py; Ctrl+b, d
>> **screen**: screen python udp_injector.py; Ctrl+a, d

> Input: Encrypted UDP packets from RPi's
> Output: Database entries
> 
> Data collection and storage: Listens, decrypts, parses, injects incoming UDP packets from the RPi's.

	addDosimeterToDB.py
> Usage:
>> **Manual**: 

> Input: 
> Output: 
>
> Stuff

	deleteDosimeter.py
> Usage:
>> **Manual**: 

> Input: 
> Output: 
>
> Stuff

	/mysql/backup_database.sh  all|stations|data
> BASH script that backups subsets of the database (tables) or the whole thing to .sql files in /home/dosenet/ (~/). 
   Example:

		./database_backup.sh all
		Password: ne170groupSpring2015  

   Output files: *.sql in /home/dosenet/ (~/). 

> 		backup_all_dosenet.sql
> 		backup_dosnet_dosenet.sql
> 		backup_stations_dosenet.sql

---
### DECF Kepler - Website & Drupal
	/html/*
> Copied into [Drupal interface](https://radwatch.berkeley.edu/user) for each file.

	~/.ssh/id_rsa_dosenet.pub
> Needs to be renamed or appended (>>) to authorized_keys and id_rsa.pub

	~/.ssh/id_rsa
> Rename the 'id_rsa kepler private key' to id_rsa in ~/.ssh

---
# Appendix

## Logic for demonstrating encryption code
### Successful inject

+ **RPi** - open UDP sender

		sudo ./udp_sender.py

   + Print pre-encrypted message
   + Print encrypted message
   + Print Sent to: Address @ Port

+ **DoseNet server** - open UDP injector

		No logging:  python udp_injector.py
		Log to file: ----------"----------- > udp_injector.log

   + Print encrypted message
   + Print decrypted message
   + Inject into database
      + Print if fail


### Failed injection
The script is verbose on encountering errors.

+ Database is offline

+ SQL injection

+ Invalid format
   + Random data (DDoS possibility)

+ Valid format, wrong data
   + No station database entry
   + Wrong stationID, Lat, Long, Message hash ~= database hash
