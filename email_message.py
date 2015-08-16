#!/usr/bin/env python

import smtplib

sender = 'dosenet@lbl.gov'
receivers = ['nbal@lbl.gov','ucbdosenet@gmail.com']

message = """From: LBL DoseNet <dosenet@lbl.gov>
To: Navrit Bal <nbal@lbl.gov> DoseNet GMail <ucbdosenet@gmail.com>
MIME-Version: 1.0
Content-type: text/html
Subject: DoseNet automated message

<style>
    samp {
        background-color: #f8f8ff;
        padding: 10px;
        margin: 10px;
        border-radius: 10px;
    }
</style>
    <h1> Some DoseNet process isn't running! :( </h1>
        <p> String format this later to inject the relevant process that isn't running. </p>
        <p> Note: include running/last run process list. </p>
        <p> stat ~/output.geojson </p>
        <samp> ps aux | grep python | grep -v grep </samp>
        <p> crontab -l </p>
        <br>
        <p> Navrit Bal </p>
        <p> Maker of DoseNet. </p>
"""

try:
   smtpObj = smtplib.SMTP('localhost')
   smtpObj.sendmail(sender, receivers, message)
   print "Successfully sent email"
except SMTPException:
   print "Error: unable to send email"
