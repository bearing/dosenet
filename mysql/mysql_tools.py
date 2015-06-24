import MySQLdb as mdb
import datetime

class SQLObject:
    def __init__(self):
        self.db = mdb.connect('localhost','ne170group','ne170groupSpring2015','dosimeter_network')
        #self.stn_list_key = {'ID':0,'Name':1,'Lat':2,'Lon':3, 'cpmtorem':4,'cpmtousv':5,'IDLatLongHash':6}
        self.verifiedStations = []
        self.getVerifiedStationList()
        self.cursor = self.db.cursor()

    def __del__(self):
        self.close()
    def __exit__(self):
        self.close()
    def close(self):
        self.db.close()

    def getVerifiedStationList(self):
        self.verifiedStations = runSQL('SELECT `ID`, `IDLatLongHash` \
                                        FROM dosimeter_network.stations;')
    def checkHashFromRAM(self,ID):
        # Essentially the same as doing the following in MySQL
        #  "SELECT IDLatLongHash
        #       FROM stations
        #       WHERE `ID` = $$$ ;"
        try:
            for i in len(verifiedStations):
                if self.verifiedStations[i][0] == ID:
                    dbHash = self.verifiedStations[i][1]
                    return dbHash
        except Exception, e:
            raise e
            return False
        print 'Could not find a station matching that ID'

    def insertIntoDosenet(self,stationID,cpm,cpmError,errorFlag):
        runSQL('INSERT INTO dosnet(stationID, cpm, cpmError, errorFlag) \
                VALUES (%s,%s,%s,%s);',
                (stationID,cpm,cpmError,errorFlag)) 
        #Time is decided by the MySQL database hence 'receiveTime'
        self.db.commit()

    def inject(self,data):
        data = self.parsePacket(data)
        if data == False:
            pass
        else: 
            if(self.authenticatePacket(data)):
                self.insertIntoDosenet( stationID   = data[1],
                                        cpm         = data[2],
                                        cpmError    = data[3],
                                        errorFlag   = data[4])

    def authenticatePacket(self,data):
        msgHash = data[0]
        ID      = data[1]
        #dbHash  = getHashFromDB(ID)
        dbHash  = checkHashFromRAM(ID)
        if dbHash == msgHash:
            return True
        else:
            return False

    def parsePacket(self,data):
        data = data.split(',')
        msgHash = data[0]
        if len(msgHash) == 32: #All is good
            stationID   = data[1]
            cpm         = float(data[2])
            cpmError    = float(data[3])
            errorFlag   = data[4]
            return (msgHash,stationID,cpm,cpmError,errorFlag)
        else:
            return False
            # Write error to file?

    def getHashFromDB(self, ID):
        # RUN "SELECT IDLatLongHash
        #       FROM stations
        #       WHERE `ID` = $$$ ;"
        return runSQL("SELECT IDLatLongHash FROM stations \
                        WHERE `ID` = '%s';" % (ID) )

    def runSQL(self,sql):
        try:
            SQLObject.cursor.execute(sql)
        except (KeyboardInterrupt, SystemExit):
            print '.... User interrupt ....\n Byyeeeeeeee'
            sys.exit(0)
        except Exception, e:
            raise e
