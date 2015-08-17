#!/usr/bin/env python

import smtplib
from subprocess import call
import subprocess
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

def send_email(process, error_message):
    spacer = "- " * 64
    stopped = process
    print spacer
    geojson = call(["stat", "output.geojson"])
    print spacer
    processes = subprocess.call('ps aux | grep python | grep -v grep', shell=True)
    print spacer
    crontab = call(["crontab","-l"])
    print spacer

    sender = 'dosenet@dosenet'
    receivers = ['nbal@lbl.gov','ucbdosenet@gmail.com']

    # Create message container - the correct MIME type is multipart/alternative.
    msg = MIMEMultipart('alternative')
    msg['Subject'] = "DoseNet automated message"
    msg['From'] = sender
    msg['To'] = receivers

    text = """Process: {}\n Error message: {}\n Navrit Bal.""".format(stopped, error_message)
    html = """\
<html>
    <head></head>
    <body>
        <h1> Some DoseNet process just stopped! :( </h1>
        <h2> Which process stopped? </h2>
            <code>{}</code>
            <br>
            <br>
            <samp>{}</samp>
        <p> Include running/last run process list. </p>
        <br>
        <h2> GeoJSON file properties </h2>
            <code> stat ~/output.geojson </code>
            <br>
            <br>
            <samp>{}</samp>
        <h2> Which Python processes are running </h2>
            <code> ps aux | grep python | grep -v grep </code>
            <br>
            <br>
            <samp>{}</samp>
        <h2> Crontab entries </h2>
            <code> crontab -l </code>
            <br>
            <br>
            <samp>{}</samp>
        <br>
        <p> Navrit Bal </p>
        <p> Maker of DoseNet. </p>
    </body>
""".format(stopped, error_message, geojson, processes, crontab)

    part1 = MIMEText(text, 'plain')
    part2 = MIMEText(html, 'html')
    msg.attach(part1)
    msg.attach(part2)

    try:
       smtpObj = smtplib.SMTP('localhost')
       smtpObj.sendmail(sender, receivers, msg.as_string())
       smtpObj.quit()
       print "Successfully sent email"
    except Exception:
       print "Error: unable to send email"

if __name__ == "__main__":
    send_email(process = "Test send_email function", error_message = "... Testing email script ...")


"""
PREPROCESSED HTML - EDIT HERE THEN USE http://premailer.dialect.ca/ to convert css to inline

    <style type="text/css">
        samp {
            background-color: #f8f8ff;
            border-radius: 10px;
        },
        code {
            background-color: #f8f8ff;
            margin: 20px auto 20px auto;
            padding-right: 20px;
            padding-left: 20px;
            padding-bottom: 7px;
            border: 2px solid #8AC007;
            border-radius: 10px;
            font-size: 1.5em;
        }
    </style>
        <h1> Some DoseNet process just stopped! :( </h1>
            <h2> Which process stopped? </h2>
                <code>{}</code>
                <br>
                <br>
                <samp>{}</samp>
            <p> Include running/last run process list. </p>
            <br>
            <h2> GeoJSON file properties </h2>
                <code> stat ~/output.geojson </code>
                <br>
                <br>
                <samp>{}</samp>
            <h2> Which Python processes are running </h2>
                <code> ps aux | grep python | grep -v grep </code>
                <br>
                <br>
                <samp>{}</samp>
            <h2> Crontab entries </h2>
                <code> crontab -l </code>
                <br>
                <br>
                <samp>{}</samp>
            <br>
            <p> Navrit Bal </p>
            <p> Maker of DoseNet. </p>

"""
