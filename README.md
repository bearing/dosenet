![](http://i.imgur.com/dVADwI5.png =512x "DoseNet logo")
## Berkeley RadWatch Dosimeter Network
## *LBNL / UC Berkeley*
---
## What goes where?
###Raspberry Pi
	udp_sender.py
> Stuff

	dosimeter.py
> Stuff

	/config-files/*.csv
> Each station has it's own CSV file (Headers: stationID, hash, lat, long).
	There are test CSV files.

	/id_rsa_dosenet.pub
>	A **private key** used for the 'half-encrypted' stage between Raspberry Pis and the database server (GRIM).
	
---
###GRIM - Database & Listener
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
###DECF Kepler - Website & Drupal
	/html/*
> Copied into [Drupal interface](https://radwatch.berkeley.edu/user) 