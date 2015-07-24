![](http://i.imgur.com/dVADwI5.png =512x "DoseNet logo")
# Berkeley RadWatch Dosimeter Network
# *LBNL / UC Berkeley*
---
## What goes where?
This is all contained in the GitHub folder, stored by convention in the 'dosenet' in the user's home folder. eg. `cd ~/dosenet/` would get you where you want to be.
### Raspberry Pi
	udp_sender.py --test(-t) (Optional) filename(-f) (Optional[str]) --led (Optional[int]) --ip (Optional[str])

>Example:

>		sudo ./udp_sender.py -f config-files/etchhall.csv

> Must be launched as sudo (it accesses GPIO pins and low-level networking APIs) 
> `sudo ./udp_sender.py`

	dosimeter.py
> Stuff

	config-files/*.csv
> Each station has it's own CSV file (Headers: stationID, hash, lat, long).
	There are test CSV files.

	id_rsa_dosenet.pub
>	A **private key** used for the 'half-encrypted' stage between Raspberry Pis and the database server (GRIM).

---
### GRIM - Database & Listener
	makeGeoJSON.py
>

	makeGeoJSON.sh
>

	udp_injector.py
>

	udp_injector.sh
>

	addDosimeterToDB.py
>

	deleteDosimeter.py
>

---
### DECF Kepler - Website & Drupal
	/html/*
> Copied into [Drupal interface](https://radwatch.berkeley.edu/user)

---
# Appendix

## Logic for demonstrating encryption code
### Successful inject

+ **RPi** - open UDP sender

		sudo ./udp_sender.py

   + Print pre-encrypted message
   + Print encrypted message
   + Print Sent to: Address @ Port

+ **GRIM** - open UDP injector

		No logging:  ./udp_injector.sh
		Log to file: ./udp_injector.sh > udp_injector.log 

   + Print encrypted message
   + Print decrypted message
   + Inject into database
      + Print if fail


### Failed injection
The script is verbose on encountering errors.

+ Database is offline

+ SQL injection

+ Invalid format
   + Random data

+ Valid format, wrong data
   + No station database entry
   + Wrong stationID, Lat, Long, Message hash ~= database hash
