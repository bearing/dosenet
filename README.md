(Scroll down for the original readme.)

# Python 3 Migration

## Tasks
- [ ] Make everything compatible with Python 3.7 (duh.)
- [ ] Make a script `data_getter.py` to get data from the Raspberry Pis
  - also decrypts the packets
- [ ] Make a script `injector.py` that
  - [ ] gets data from the Raspberry Pis using `data_getter.py`
  - [ ] writes the acquired data to csv files
- [ ] Rewrite (?) the script that sends data to the website

## Structure
For now, let's work in the directory `[py37](py37)` to keep our new code completely isolated
from the old while also preserving the latter for easy copy-pasting. We can do something else
but this is easy and it works, so why not :stuck_out_tongue:.

---

![](http://i.imgur.com/dVADwI5.png =512x "DoseNet logo")
# Berkeley RadWatch Dosimeter Network
# *LBL / UC Berkeley*
### Author: Navrit Bal
##### Navrit Bal, Tigran Ter-Stepanyan, Mark Trudel, Nathan Richner
##### Joseph Curtis, Ryan Pavlovsky, Ali Hanks
##### Kai Vetter

---
## Please take note
This readme may not be fully up to date. (see #23)
For a to-do, see the issue list in GitHub and Waffle.

## For testing purposes
Temporarily add the repo to your path

```bash
export PYTHONPATH=`pwd`:$PYTHONPATH
```

---
## What goes where?
This is all contained in the GitHub repository, stored by convention in 'dosenet' in the user's home folder. eg. `cd ~/dosenet/` or `cd ~/git/dosenet` would get you where you want to be.
This repo should always be cloned by SSH:

	git clone git@github.com:bearing/dosenet.git

### Raspberry Pi
	udp_sender.py filename(-f) (required) --test(-t) (Opt.) (Opt.[str]) --led_counts (Opt.[int]) --led_power (Opt.[int]) --led_network (Opt.[int]) --ip (Opt.[str])

>Example:

>		sudo ./udp_sender.py -f config-files/lbl.csv

> Must be launched as sudo (it accesses GPIO pins and low-level networking APIs)
> `sudo ./udp_sender.py`

	dosimeter.py
> Usage:
>> **Test**: python dosimeter.py
>
> Supporting class for the UDP_sender class. Handles actual radiation detection logic and passes through to the sender.

	config-files/*.csv
> Each station has it's own CSV file (Headers: stationID, hash, lat, long).There are test CSV files available. This data is obtained from the database - you require database access to start sending authenticated packets from a new dosimeter. This is purposely not automated - human checks should be and are required for adding new dosimeters.
>
> Header format (static):
> stationID,message_hash,lat,long
>
> Example data (completely database (stations table) dependant:
> 1,7c756767412b5346ad79bb9a5cf56f51,37.876886,-122.252211
>
> A complete sample valid CSV file:
>
> stationID,message_hash,lat,long
>
> 1,7c756767412b5346ad79bb9a5cf56f51,37.876886,-122.252211

	ssh-keys/id_rsa_dosenet.private
>	A **private key** used for the 'half-encrypted' stage between Raspberry Pis and the database server (GRIM).

---
### DoseNet server - Database & Listener
#### dosenet.dhcp.lbl.gov
##### Used to be grim.nuc.berkeley.edu
	makeGeoJSON.py
> Usage:
>> **Automatic**: crontab operation

>> **Manual**: python makeGeoJSON.py

> Input: *Indirectly* queries database - no command line arguments accepted.

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
>
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

    inject-test-data.py
> Usage:
>> **Manual**:

> Input:

> Output:
>
> Stuff

    email_message.py
> Usage:
>> **Manual**:

> Input:

> Output:
>
> Stuff

    dosenet.sh
> Usage:
>> **Automatic (after copy)**

> Input: `start|stop|test`

> Output: Starts UDP_sender.py with options on boot to normal Linux runlevels (2, 3, 4, 5)
>
> Usage: `cp dosenet.sh /etc/init.d/`
>
    /etc/init.d/dosenet.sh start|stop|test
>
> Change configuration (`CONFIGFILE` variable) in `/etc/init.d/dosenet.sh` (could use nano, vi, vim, emacs etc.) to match the new dosimeter CSV file as in `config-files/`
>
> Eg. `CONFIGFILE=config.csv --> CONFIGFILE=lbl.csv`
>
> Then use the `update-rc.d` command detailed below to update symbolic links etc. so it starts up correctly.

    sudo update-rc.d /etc/init.d/dosenet.sh defaults
    I think there's meant to be another line here????

    ????

    ????

---
### DECF Kepler - Website & Drupal
	/html/*
> Copied into [Drupal interface](https://radwatch.berkeley.edu/user) for each file.

	~/.ssh/id_rsa_dosenet.pub
> `ssh-keys/id_rsa_dosenet.pub` - needs to be renamed or appended (>>) to `authorized_keys` and `id_rsa.pub`

	~/.ssh/id_rsa
> Rename the `ssh-keys/id_rsa kepler\ private\ key` to `~/.ssh/id_rsa`

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
