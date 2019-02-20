from mysql_tools.mysql_tools import SQLObject
from utils import timeout, TimeoutError
import pandas as pd

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
    retry_counter = 0
    while retry_counter < 3:
        try:
            with timeout(max_time*(retry_counter+1)):
                DB = SQLObject()
                df = DB.getAll(ID,data_type,True)
                return df
        except (TimeoutError) as e:
            retry_counter = retry_counter + 1
            print(e)
            print("Timed out on try {}".format(retry_counter))
            pass
        except (Exception) as e:
            print(e)
            print(pd.DataFrame({}))
            break
    print(pd.DataFrame({}))
