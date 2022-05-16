from time import sleep
from json import dumps
from kafka import KafkaProducer
import pickle
import datetime
import pandas as pd
import psycopg2
import yaml
from datetime import datetime, timedelta
import os

os.chdir('C:\summary_reports')

#retrieve data from pickle 
def restore_db(pickle_name):
    """
    pickle_name is a string, that is how the compressed file is named
    this function returns the data in the same format it was stored
    """
    infile = open(pickle_name,'rb')
    source= pickle.load(infile)
    print('File type:',type(source))
    infile.close()
    return source

def get_timestamp_previous_day():
    now=datetime.datetime.now()-datetime.timedelta(hours=5.5)
    before=now-datetime.timedelta(hours=24)
    return(before,now)

def get_timestamp_from_epoch(_epoch):
    return datetime.datetime.fromtimestamp(_epoch/1000).strftime('%Y-%m-%d %H:%M:%S.%f')


#pickle the dataframe for future retrieval if any
def store_db(dataframe,pickle_name):
    """
    pickle_name is a string, that is how the compressed file is named
    dataframe is name of the dataframe
    
    """
    outfile = open(pickle_name,'wb')
    pickle.dump(dataframe,outfile)
    outfile.close()
    print('Data saved as {0} successfully'.format(pickle_name))


def query_raw_data(monitor, start_time, end_time, login_detail, IST=False):
    # by default start time and endtime are UTC string in GMT zone
    # if 'ist_time' flag is True, the input values are considered as IST and converted to GMT format

    if IST:
        start_time = pd.to_datetime(start_time) - datetime.timedelta(hours=5.5)
        end_time = pd.to_datetime(end_time) - datetime.timedelta(hours=5.5)

    try:

        _user = login_detail[0]
        _pwd = login_detail[1]
        _host = login_detail[2]
        _port = login_detail[3]
        _db = login_detail[4]

        connection = psycopg2.connect(user=_user,
                                      password=_pwd,
                                      host=_host,
                                      port=_port,
                                      database=_db)

        cursor = connection.cursor()

        query = f"""
        SELECT * FROM "raw_acceleration_data" 
        WHERE "monitor_id" = '{monitor}' 
        AND "time" >= '{start_time}'
        AND "time" <= '{end_time}' 
        LIMIT 50
        """

        cursor.execute(query)
        records = cursor.fetchall()

        result = []

        for row in records:
            data = {}
            data['x_raw'] = row[1]
            data['y_raw'] = row[2]
            data['z_raw'] = row[3]
            data['block_size'] = row[4]
            data['mac'] = row[5]
            data['fw_version'] = row[6]
            data['monitor_id'] = row[7]
            data['sampling_rate'] = row[8]
            data['timestamp'] = row[9]

            result.append(data)

    except (Exception, psycopg2.Error) as error:
        print('Error while connecting to PostgreSQL', error)
        raise Exception('Error while connecting to PostgreSQL')
    finally:
        if (connection):
            cursor.close()
            connection.close()
    return result


# import os
# def load_env_variables():
#     try:
#         user = os.getenv('USER')
#         password = os.getenv('PASSWORD')
#         host = os.getenv('IP')
#         port = os.getenv('PORT')
#         database = os.getenv('DATABASE')
#         #logger.info('Login USER: {}'.format(user))
#         #logger.info('Login DATABASE: {}'.format(database))
#         return [user, password, host, port, database]
#     except Exception as e:
#         raise Exception("Login details not available in Environment variables", e)

connection = psycopg2.connect(user="iuprod",
                                          password="pratiti",
                                          host="10.0.1.253",
                                          port="5432",
                                          database="iu_timeseries")

cursor = connection.cursor()
        
query = f"""
   select * from public.diagnostic_recommendation where start_time = date_trunc('hour', current_date) - interval '1 day' order by start_time desc;

    """
cursor.execute(query)
records = cursor.fetchall()

df_diag=pd.DataFrame(records)
df_diag.columns=['id','monitor_id','created_on','start','end','odr','updated_on','misc', 'misc2', 'misc3']
print(df_diag.shape)
print(df_diag.head())

connection = psycopg2.connect(user="iuprod",
                                          password="pratiti",
                                          host="10.0.1.253",
                                          port="5432",
                                          database="iu_admin")

cursor = connection.cursor()
query = f"""
    select mo.label as monitor_label,mo.id as monitor_id, m.label as machine_name, m.id as machine_id, p.name as plant_name, o.name as org_name
from monitors mo
inner join machines m on mo.machine_id = m.id
inner join machinegroups mg on m.machine_group_id = mg.id
inner join plants p on p.id = mg.plant_id
inner join organizations o on o.id = p.organization_id
order by machine_id, monitor_id;
    """
cursor.execute(query)
records = cursor.fetchall()
result = []

for row in records:
    data = {}
    data['monitor_name'] = row[0]
    data['monitor_id'] = row[1]
    data['machine_name'] = row[2]
    data['machine_id'] = row[3]
    data['plant_name'] = row[4]
    data['org_name'] = row[5]
   
    result.append(data)

config_prod_df=pd.DataFrame(result)

#config_prod_df=restore_db('prod_db')
print(config_prod_df.head())

#append to database
diag_result=[]
for record in range(0,len(df_diag.index)):
    diag_result.append(df_diag.odr.values[record])

#concat ODR and monitor dataframe
odr_df=pd.DataFrame(diag_result)
df_diag=pd.concat([df_diag,odr_df],axis=1)

#Merge Plant data and diagnostics data based on monitor id
merge_df=config_prod_df.merge(df_diag,on="monitor_id",how='outer')
merge_df.head()

yesterday_date = (datetime.now() - timedelta(2)).strftime('%Y-%m-%d').split('-')
month_dict= {"01": "Jan", 
              "02": "Feb", 
              "03": "Mar",
              "04": "Apr",
              "05": "May",
              "06": "June",
              "07": "July",
              "08": "Aug",
              "09": "Sept",
              "10": "Oct",
              "11": "Nov",
              "12": "Dec"
              }

final_date = yesterday_date[2] + month_dict[yesterday_date[1]]
odr_filename = 'odr_results_' + final_date + '.xlsx'
#odr_filename = 'odr_results_17Nov_final.xlsx'
print(odr_filename)

merge_df.drop(["created_on","start","end","updated_on","misc","misc2", "misc3"],axis=1).to_excel(odr_filename) 

from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

gauth = GoogleAuth()           
drive = GoogleDrive(gauth)  

upload_file = odr_filename
gfile = drive.CreateFile({'parents': [{'id': '1dcM8tmCykBTOk6Q6uOYgV9sacEOk8DIo'}]})
#	Read file and set it as the content of this instance.
gfile.SetContentFile(upload_file)
gfile.Upload() # Upload the file.
