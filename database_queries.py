from mysql_tools.mysql_tools import SQLObject


def get_stations():
    DB = SQLObject()
    df = DB.getStations()
    df_sub = df[['Name','Lat','Long']]
    print(df_sub)
