import MySQLdb as mdb;

class SQLObject:
    def __init__(self):
        self.db=mdb.connect('localhost','ne170group','ne170groupSpring2015','dosimeter_network');
        self.stn_list_key={'ID':0,'Name':1,'Lat':2,'Lon':3};
        self.v_stn_list=[];
        self.get_verified_station_list();
        print self.v_stn_list[0][0];
    def __del__(self):
        self.db.close();
    def get_verified_station_list(self):
        cursor=self.db.cursor();
        cursor.execute('Select `ID`, `Name`, `Lat`,`Long` FROM dosimeter_network.stations;');
        self.v_stn_list=cursor.fetchall();
    def insert_into_dosenet(self,stationID,cpm,rem,usv,errorFlag):
        cursor=self.db.cursor();
        cursor.execute("""INSERT INTO dosnet(stationID, cpm, rem, usv, errorFlag) VALUES (%s,%s,%s,%s,%s);""",(stationID,cpm,rem,usv,errorFlag));
        db.commit();
    def inject(self,data):
        if(self.verify_data(data)):
            data=self.parse_data(data);
            self.insert_into_dosenet(stationID=data[0],cpm=data[1],rem=data[2],usv=data[3],errorFlag=data[4]);
    def verify_data(self,data):
        #bare bones implementation (NEEDS TO BE REIMPLEMENTED)
        easy_integer_list=[ x[0] for x in self.v_stn_list ];
        print (int(data[0]) in easy_integer_list);
        return False;
    def parse_data(self,data):
        stationID=1;
        time=data[2].replace(microsecond=0);
        cpm=float(new[1]);
        rem=cpm*8.76;
        usv=rem*1E-4;
        errorFlag=1;
        return (stationID,time,cpm,rem,usv,errorFlag);
