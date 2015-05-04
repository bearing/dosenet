import MySQLdb as mdb;
import datetime;

class SQLObject:
    def __init__(self):
        self.db=mdb.connect('localhost','ne170group','ne170groupSpring2015','dosimeter_network');
        self.stn_list_key={'ID':0,'Name':1,'Lat':2,'Lon':3, 'cpmtorem':4,'cpmtousv':5};
        self.v_stn_list=[];
        self.get_verified_station_list();
    def __del__(self):
        self.db.close();
    def get_verified_station_list(self):
        cursor=self.db.cursor();
        cursor.execute('SELECT `ID`, `Name`, `Lat`,`Long` FROM dosimeter_network.stations;');
        self.v_stn_list=cursor.fetchall();
    def insert_into_dosenet(self,time,stationID,cpm,cpmError,errorFlag):
        cursor=self.db.cursor();
        cursor.execute("""INSERT INTO dosnet(receiveTime, stationID, cpm, cpmError, errorFlag) VALUES (%s,%s,%s,%s,%s);""",(time, stationID,cpm,cpmError,errorFlag));
        self.db.commit();
    def inject(self,data):
        if(self.verify_data(data)):
            data=self.parse_data(data);
            self.insert_into_dosenet(time = data[0], stationID=data[1],cpm=data[2],cpmError = data[3], errorFlag=data[4]);
    def verify_data(self,data):
        #bare bones implementation (NEEDS TO BE REIMPLEMENTED)
        easy_integer_list=[ x[0] for x in self.v_stn_list ];
        print (int(data[0]) in easy_integer_list);
        return True;
    def parse_data(self,data):
        data=data.split(',');
        stationID=data[0];
        time =  data[1][:19];
        cpm=float(data[2]);
        cpmError = float(data[3]); 
        errorFlag = data[4];
        return (time,stationID,cpm,cpmError,errorFlag);
