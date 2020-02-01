import json
import click
import os
import pandas as pd

from preprocess.get_csv_summary import Csv_summerizer
from pyhive import hive

def get_config():
    if os.environ['HOME'] == '/home/pi':
        # read config json file
        with open('conf/config_prod.json', 'r') as myfile:
            data = myfile.read()
    else:
        # read config json file
        with open('conf/config_dev.json', 'r') as myfile:
            data = myfile.read()
    
    return json.loads(data)

def pandas_to_hive_dtype_converter(pandas_dtype):
    """ Convert pandas datatype to hive sql datatype """
    with open('conf/pd_to_hive_dtype_map.json', 'r') as myfile:
        dtype_map = myfile.read()

    converter = json.loads(dtype_map)

    return converter[pandas_dtype]



@click.command()
@click.option("--database_name", prompt="Hive database name", help="Specify the destination Hive database that you want to import csv")
@click.option("--table_name", prompt="Hive table name", help="Specify the destination Hive table that you want to import csv")
@click.option("--csv_name", prompt="Csv name", help="The specify the storage directory.")
@click.option("--csv_dir", default="/home/pi/Hive_ETL/data/", help="The CSV file source directory.")
def load_csv_to_hive(database_name, table_name, csv_name, csv_dir):
    config = get_config()

    '''' 
        convert csv information to hive sql syntax
    '''
    # get csv summary to form sql command
    csv_info = Csv_summerizer(f"{config['data_folder']}{csv_name}")
    name_dtype_dic = csv_info.get_columns_datatype()

    hive_table_column_type_query = ''
    for column_name, dtype in name_dtype_dic.items():
        hive_table_column_type_query = hive_table_column_type_query + '`' + column_name + '`' + ' ' + pandas_to_hive_dtype_converter(dtype) + ', '

    hive_table_column_type_query = hive_table_column_type_query[:-2]

    #TODO: change way to fix first row (column name) import to hive problem
    csv_df = pd.read_csv(f"../data/{csv_name}")
    csv_df.to_csv(f"../data/{csv_name}", header=False, index=False)

    ''' 
        connect to hive with pyhive
    '''
    conn = hive.Connection(host=config['host_name'], 
                            port=config['port'], username=config['user'], 
                            password=config['password'],
                            database=database_name,
                            auth='CUSTOM')
    cur = conn.cursor()
    cur.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({hive_table_column_type_query}) row format delimited fields terminated by ','")
    print(f"LOAD DATA LOCAL INPATH '{csv_dir}{csv_name}' OVERWRITE INTO TABLE {table_name}")
    cur.execute(f"LOAD DATA LOCAL INPATH '{csv_dir}{csv_name}' OVERWRITE INTO TABLE {table_name}")
    print('data loaded')
    cur.execute(f"select * from {table_name}")
    result = cur.fetchall()
    # print(result)
    

if __name__ == '__main__':
    load_csv_to_hive()