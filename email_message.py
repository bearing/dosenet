#!/usr/bin/env python

import smtplib

sender = 'dosenet@lbl.gov'
receivers = ['nbal@lbl.gov','ucbdosenet@gmail.com']

message = """From: LBL DoseNet <dosenet@lbl.gov>
To: Navrit Bal <nbal@lbl.gov> DoseNet GMail <ucbdosenet@gmail.com>
MIME-Version: 1.0
Content-type: text/html
Subject: DoseNet automated message

<h1> Some DoseNet process isn't running! :( </h1>
<p> String format this later to inject the relevant process that isn't running. </p>
<p> Note: include running/last run process list. </p>
<p> stat ~/output.geojson? </p>
<p> ps aux | grep python </p>
"""

try:
   smtpObj = smtplib.SMTP('localhost')
   smtpObj.sendmail(sender, receivers, message)
   print "Successfully sent email"
except SMTPException:
   print "Error: unable to send email"
