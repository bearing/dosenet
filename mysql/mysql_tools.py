import MySQLdb as mdb
import datetime

class SQLObject:
    def __init__(self):
        self.db = mdb.connect('localhost','ne170group','ne170groupSpring2015','dosimeter_network')
        # self.stn_list_key = {'ID':0,'Name':1,'Lat':2,'Lon':3, 'cpmtorem':4,'cpmtousv':5,'IDLatLongHash':6}
        self.verified_stations = []
        self.getVerifiedStationList()
        self.cursor = self.db.cursor()

    def __del__(self):
        self.close()
    def __exit__(self):
        self.close()
    def close(self):
        self.db.close()

    def getVerifiedStationList(self):
        self.verified_stations = runSQL('SELECT `ID`, `IDLatLongHash` \
                                        FROM dosimeter_network.stations;')
    def checkHashFromRAM(self,ID):
        # Essentially the same as doing the following in MySQL
        # "SELECT IDLatLongHash FROM stations WHERE `ID` = $$$ ;"
        try:
            for i in len(verified_stations):
                if self.verified_stations[i][0] == ID:
                    dbHash = self.verified_stations[i][1]
                    return dbHash
        except Exception as e:
            raise e
            return False
            print ('Could not find a station matching that ID')

    def insertIntoDosenet(self,stationID,cpm,cpm_error,error_flag):
        runSQL('INSERT INTO dosnet(stationID, cpm, cpmError, errorFlag) \
                VALUES (%s,%s,%s,%s);',
                (stationID,cpm,cpm_error,error_flag)) 
        # Time is decided by the MySQL database / GRIM hence 'receiveTime' field in DB
        self.db.commit()

    def inject(self,data):
        data = self.parsePacket(data)
        if data == False:
            pass
        else: 
            if(self.authenticatePacket(data)):
                self.insertIntoDosenet( 
                    stationID   = data[1],
                    cpm         = data[2],
                    cpm_error   = data[3],
                    error_flag  = data[4])

    def authenticatePacket(self,data):
        msg_hash = data[0]
        ID       = data[1]
        db_hash  = checkHashFromRAM(ID)
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
        return runSQL("SELECT IDLatLongHash FROM stations \
                        WHERE `ID` = '%s';" % (ID) )

    def runSQL(self,sql):
        try:
            SQLObject.cursor.execute(sql)
        except (KeyboardInterrupt, SystemExit):
            print ('.... User interrupt ....\n Byyeeeeeeee')
            sys.exit(0)
        except Exception as e:
            raise e
