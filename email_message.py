#!/usr/bin/env python

import smtplib

sender = 'dosenet@lbl.gov'
receivers = ['nbal@lbl.gov','ucbdosenet@gmail.com']

message = """From: LBL DoseNet <dosenet@lbl.gov>
To: Navrit Bal <nbal@lbl.gov>, DoseNet GMail <ucbdosenet@gmail.com>
Subject: DoseNet automated message

This is a test e-mail message.
"""

try:
   smtpObj = smtplib.SMTP('localhost')
   smtpObj.sendmail(sender, receivers, message)
   print "Successfully sent email"
except SMTPException:
   print "Error: unable to send email"
