# -*- coding: utf-8 -*-
import MySQLdb as mdb
import datetime
import email_message

class SQLObject:
    def __init__(self):
        self.db = mdb.connect('127.0.0.1','ne170group','ne170groupSpring2015','dosimeter_network')
        self.verified_stations = []
        self.cursor = self.db.cursor()
        self.getVerifiedStationList()

    def __del__(self):
        self.close()
    def __exit__(self):
        self.close()
    def close(self):
        self.db.close()

    def getVerifiedStationList(self):
        try:
            self.cursor.execute("SELECT `ID`, `IDLatLongHash` FROM dosimeter_network.stations;")
            self.verified_stations = self.cursor.fetchall()
        except Exception as e:
            raise e
            msg = 'Error: Could not get list of stations from the database!'
            print msg
            email_message.send_email(process = os.path.basename(__file__), error_message = msg)

    def checkHashFromRAM(self,ID):
        # Essentially the same as doing the following in MySQL
        # "SELECT IDLatLongHash FROM stations WHERE `ID` = $$$ ;"
        try:
            for i in range(len(self.verified_stations)):
                if self.verified_stations[i][0] == ID:
                    dbHash = self.verified_stations[i][1]
                    return dbHash
        except Exception as e:
            raise e
            return False
            msg = 'Error: Could not find a station matching that ID'
            print msg
            email_message.send_email(process = os.path.basename(__file__), error_message = msg)

    def insertIntoDosenet(self,stationID,cpm,cpm_error,error_flag):
        self.cursor.execute("INSERT INTO dosnet(stationID, cpm, cpmError, errorFlag) \
                VALUES ('%s','%s','%s','%s');" %
                (stationID,cpm,cpm_error,error_flag))
        # Time is decided by the MySQL database / GRIM hence 'receiveTime' field in DB
        self.db.commit()

    def inject(self,data):
        if not self.authenticatePacket(data):
            print '~~ FAILED AUTHENTICATION ~~'
            print 'Data: ', data
            msg = str('FAILED AUTHENTICATION\n',data)
            email_message.send_email(process = os.path.basename(__file__), error_message = msg)
        else:
            data = self.parsePacket(data)
            if(data):
                self.insertIntoDosenet(
                    stationID   = data[1],
                    cpm         = data[2],
                    cpm_error   = data[3],
                    error_flag  = data[4])
            else:
                print '~~ FAILED TO INJECT/PARSE ~~'
                msg = '\t stationID: "%s", cpm: "%s", cpm_error: "%s", error_flag: "%s"' % \
                    (stationID,cpm,cpm_error,error_flag)
                print msg
                email_message.send_email(process = os.path.basename(__file__), error_message = msg)

    def getHashList(self):
        return self.verified_stations

    def authenticatePacket(self,data):
        hash_list = self.getHashList()
        msg_hash = data[:32] # Is it the correct hash length?
        if not any(str(msg_hash) in i for i in hash_list): # Verify the hash is in the list
            print 'Message Hash: ', msg_hash
            print 'Hash list: ', hash_list
            print 'Hash is not in list'
            return False
        # Ok, we think this could be a real station
        ID       = int(data.split(',')[1])
        db_hash  = self.checkHashFromRAM(ID)
        if db_hash == msg_hash: # Is it an authenticated station with a matching database entry?
            return True
        else:
            print 'ID:', ID, 'Hash from database: ', db_hash, 'Hash from message', msg_hash
            return False # Valid data, not authenticated station.

    def parsePacket(self,data):
        data = data.split(',') # Commas are our delimiters for the decrypted message
        msg_hash = data[0]
        if len(msg_hash) == 32: # All is good. Cast as known data types
            stationID   = int(data[1])
            cpm         = float(data[2])
            cpm_error   = float(data[3])
            error_flag  = int(data[4])
            return (msg_hash, stationID, cpm, cpm_error, error_flag)
        else:
            return False
            print 'Data:', data
            print 'Message hash', msg_hash
            print 'Station ID: "%s", CPM: "%s", CPM error: "%s", Error flag: "%s"' \
                % (stationID, cpm, cpm_error, error_flag)

    def getHashFromDB(self, ID):
        # RUN "SELECT IDLatLongHash FROM stations WHERE `ID` = $$$ ;"
        try:
            self.cursor.execute("SELECT IDLatLongHash FROM stations \
                            WHERE `ID` = '%s';" % (ID) )
            dbHash = self.cursor.fetchall()
            return dbHash
        except (KeyboardInterrupt, SystemExit):
            print ('User interrupted for some reason, byeeeee')
            sys.exit(0)
        except (Exception) as e:
            print str(e)
            print ('Exception: Could not get hash from database. Is GRIM online and running MySQL?')
