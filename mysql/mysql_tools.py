import MySQLdb as mdb
import datetime

class SQLObject:
    def __init__(self):
        self.db = mdb.connect('localhost','ne170group','ne170groupSpring2015','dosimeter_network')
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
        self.cursor.execute("SELECT `ID`, `IDLatLongHash` FROM dosimeter_network.stations;")
        self.verified_stations = self.cursor.fetchall()

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
            print 'Could not find a station matching that ID'

    def insertIntoDosenet(self,stationID,cpm,cpm_error,error_flag):
        self.cursor.execute("INSERT INTO dosnet(stationID, cpm, cpmError, errorFlag) \
                VALUES (%s,%s,%s,%s);",
                (stationID,cpm,cpm_error,error_flag))
        # Time is decided by the MySQL database / GRIM hence 'receiveTime' field in DB
        self.db.commit()

    def inject(self,data):
        if not self.authenticatePacket(data):
            print '~~ FAILED AUTHENTICATION'
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

    def getHashList(self):
        return self.verified_stations[:][1]

    def authenticatePacket(self,data):
        hash_list = self.getHashList()

        #Verify the hash is in the list
        msg_hash = data[:32];
        if( not msg_hash in hash_list ):
            return False

        #Ok, we think this could be a real station
        ID       = int(data.split(',')[1])
        db_hash  = self.checkHashFromRAM(ID)
        if db_hash == msg_hash:
            return True
        else:
            return False

    def parsePacket(self,data):
        data = data.split(',')
        msg_hash = data[0]
        if len(msg_hash) == 32: #All is good
            stationID   = int(data[1])
            cpm         = float(data[2])
            cpm_error   = float(data[3])
            error_flag  = int(data[4])
            return (msg_hash, stationID, cpm, cpm_error, error_flag)
        else:
            return False
            # Write error to file?

    def getHashFromDB(self, ID):
        # RUN "SELECT IDLatLongHash
        #       FROM stations
        #       WHERE `ID` = $$$ ;"
        self.cursor.execute("SELECT IDLatLongHash FROM stations \
                        WHERE `ID` = '%s';" % (ID) )
        dbHash = self.cursor.fetchall()
        return dbHash
