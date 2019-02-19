from mysql_tools.mysql_tools import SQLObject
from utils import timeout

def get_stations():
    DB = SQLObject()
    df = DB.getStations()
    df_sub = df[['Name','Lat','Long','display','devices']]
    print(df_sub)

def get_station(ID):
    DB = SQLObject()
    df = DB.getSingleStation(ID)
    print(df)

def get_station_hash(ID):
    DB = SQLObject()
    hash = DB.getHashFromDB(ID)
    print(hash)

def station_update(ID,column,value):
    DB = SQLObject()
    #UPDATE `dosimeter_network`.`stations` SET `Long` = '8.668740' WHERE (`ID` ='48') and (`Name` = 'Westend');
    DB.sendSingleStationChange(ID,column,value)

#@timeout(120)
def get_all_data(ID,data_type,max_time):
    @timeout(max_time)
    def sub_func(ID,data_type):
            DB = SQLObject()
            DB.getAll(ID,data_type,True)
    try:
        sub_func(ID,data_type)
    except Exception as e:
        print(e)
