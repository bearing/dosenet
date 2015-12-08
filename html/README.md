# Berkeley RadWatch Dosimeter Network Webpage
# *LBL / UC Berkeley*
### Author: Ali Hanks

---
## ToDo!
- [ ] Page detailing calibration procedure
- [ ] new article on downloadable data availability and updated plotting?

---
## What goes where?
This area is for web-development related software
+ Main html documents for each page
  + html files are not sourced directly but it is useful to have a copy maintained in the repo
+ CSS style files
  + useful to keep as separate files for scalability
+ Javascript packages
  + some functionality required across pages

> Create and maintain all pages through the Drupal interface for RadWatch

> Upload css and javascript files to kepler:

    username@kepler.berkeley.edu:/var/www/html/htdocs-nuc-groups/radwatch-7.32/sites/default/files/dosenet

>> must be member of **radwatchgrp** or **radwatchtestgrp**

## Third-party software sources:
These software packages are maintained elsewhere and may change!
Some vigilance to ensure up-to-date versions with support is required.

> **Google Maps**
>> Main Javascript API requires site-specific key

		https://console.developers.google.com/apis/credentials?project=vital-platform-115303
		
>>> Our key is: 

    AIzaSyDSaBOz47zWi5eWz12SYzSl6GMSpl8l1c8

>> Additional features

>>> + **MarkerwithLabel**

    https://google-maps-utility-library-v3.googlecode.com/svn/tags/markerwithlabel/1.1.9/src/markerwithlabel_packed.js

>>> + **MarkerClusterer**

    https://google-maps-utility-library-v3.googlecode.com/svn/trunk/markerclusterer/src/markerclusterer.js

> **DyGraphs**

>> + Used throughout RadWatch website
>> + local version maintained by main RadWatch site administrator
>> Remotely hosted version maintained here:

    https://cdnjs.cloudflare.com/ajax/libs/dygraph/1.1.1/dygraph-combined.js

>> Source can also be downloaded here: http://dygraphs.com/download.html

