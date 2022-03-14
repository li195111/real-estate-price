from __future__ import annotations
import os
import glob
import json
from datetime import datetime
from typing import List, Optional

import numpy as np
from pycnnum import num2cn
from dotenv import load_dotenv
import pyspark

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, regexp_replace
from pyspark.sql.types import IntegerType

from options import option
from crawler import parse_data


def taiwandate_num2str(date_number)->str:
    '''
    Convert date number to string format time
    @param date_number : Taiwan's date time number like, Ex: 108年02月15日 = 1080215
    
    @return : string format time
    '''
    y = date_number // 10000
    m = (date_number - y * 10000) // 100
    d = (date_number - y * 10000) % 100
    return datetime(year=y+1911, month=m, day=d).strftime('%Y-%m-%d')

def df2result(df:DataFrame, map_keys:List[str]=['district','building_state'], map_values:List[str]=['鄉鎮市區','建物型態'])->dict:
    '''
    Convert Spark DataFrame to JSON object result, event is according the given map_keys and map_values.
    @param df : Spark DataFrame to convert
    @param map_keys : event key name
    @param map_values : event value from DataFrame Column
    @return : dict with 'city', 'date', and 'event' with given keys and Column values.
    '''
    # Order by desc
    df = df.orderBy(df['交易年月日'].desc())
    result = {}
    for row in df.collect():
        row_data = row.asDict()
        city = row_data.get('城市')
        date = row_data.get('交易年月日')
        if not date is None:
            date_str = taiwandate_num2str(date)
            event = {k:row_data.get(v) for k,v in zip(map_keys,map_values)}
            if not city in result:
                result[city] = {date_str:[event]}
            else:
                if not date_str in result[city]:
                    result[city][date_str] = [event]
                else:
                    result[city][date_str].append(event)
    return result

def save_dataframe2json(df:DataFrame, file_name:str, save_dir:str='.')->None|dict:
    '''
    If df is given and not None type, This will convert the Spark Dataframe to json file with given filename and directory.
    @param df : Spark Dataframe.
    @param file_name : Save JSON file name.
    @param save_dir : Save JSON direcotry.
    '''
    assert not df is None, "'df' can not be 'None'."
    result = df2result(df, ['district','building_state'], ['鄉鎮市區','建物型態'])
    output = []
    for city in result:
        time_slots = []
        for date in result[city]:
            events = result[city][date]
            time_slots.append({'date':date,'events':events})
        output.append({'city':city,'time_slots':time_slots})
    with open(os.path.join(os.path.abspath(save_dir),f'{file_name}.json'),'w',encoding='utf-8') as fp:
        json.dump(output, fp)
    return output
        
def get_spark_df(spark:SparkSession, file_paths:str, code_name_map:dict)->DataFrame|None:
    '''
    This will read and merge all of the CSV files into Spark DataFrame from file paths
    @params: spark : Spark Session.
    @params: file_paths : list of csv file paths.
    @return : Spark DataFrame with merged data from input file paths.
    '''
    # Task 4. Merge Spark DF
    union_df = None
    ext_col_name = '城市'
    for file_path in file_paths:
        file_name = os.path.basename(file_path).split('.')[0]
        zone_code = file_name[0]
        zone_name = code_name_map[zone_code]
        df = spark.read.csv(file_path,header=True).cache()
        df = df.withColumn(ext_col_name, lit(zone_name))
        if union_df is None:
            union_df = df
        else:
            union_df = union_df.unionAll(df)
    if not union_df is None:
        # Cast Date Type
        union_df = union_df.withColumn("交易年月日", union_df["交易年月日"].cast(IntegerType()))
        # Convert Floor String to Number and Cast to Int
        union_df = union_df.withColumn("總樓層數N", regexp_replace('總樓層數', '層', ''))
        for i in range(1,100):
            c = num2cn(i)
            if i // 10 == 1:
                c = c[1:]
            union_df = union_df.withColumn("總樓層數N", regexp_replace('總樓層數N', rf"^{c}$", f'{i}'))
        union_df = union_df.withColumn("總樓層數N", union_df["總樓層數N"].cast(IntegerType()))
    return union_df

def task_4(union_df:DataFrame, main_purpose:str='住家用', building_state:str='住宅大樓', n=13):
    # 4. Search Condition
    part4_df:DataFrame = union_df\
        .where(union_df['主要用途']==main_purpose)\
        .where(union_df['建物型態'].contains(building_state))\
        .where(union_df['總樓層數N'] >= n)
    save_dataframe2json(part4_df, 'result-part1')

if __name__ == "__main__":
    opts = option()
    if opts.dotenv:
        # Load .env config
        load_dotenv()
        download_path = os.environ.get('download_path','./downloads')
        zip_file_name = os.environ.get('zip_file_name','download.zip')
        unzip_dir_name = os.environ.get('unzip_dir_name','datas')
        publish = os.environ.get('publish','108年第2季')
        file_format = os.environ.get('file_format','csv')
        names = os.environ.get('names','臺北市,新北市,桃園市,臺中市,高雄市').split(',')
        name_codes = os.environ.get('name_codes','A,F,H,B,E').split(',')
        category = os.environ.get('category','不動產資料')
    else:
        download_path = opts.download_path
        zip_file_name = opts.zip_file_name
        unzip_dir_name = opts.unzip_dir_name
        publish = opts.publish
        file_format = opts.file_format
        names = opts.names
        name_codes = opts.name_codes
        category = opts.category
        
    download_path = os.path.abspath(download_path)
    os.makedirs(download_path, exist_ok=True)
    zip_file_path = os.path.join(download_path,zip_file_name)
    unzip_dir = os.path.join(download_path,unzip_dir_name)
    code_name_map = {code:name for code, name in zip(name_codes,names)}
    category_map = {'不動產資料':'A','預售屋買賣':'B','不動產租賃':'C'}
    name_set = {f'{code}_lvr_land_{category_map[category]}'.lower() for code in name_codes}
    
    # Task 1.
    parse_data(publish, file_format, names, download_path, zip_file_path, unzip_dir, os.environ.get('DRIVER_PATH'))
    # Task 2.
    file_paths = [file for file in glob.glob(rf'{unzip_dir}/*.csv', recursive=True) if os.path.basename(file).split('.')[0].lower() in name_set]
    # .config("spark.sql.execution.arrow.pyspark.enabled","true") # for use toPandas
    spark = SparkSession.builder\
        .appName("realEstatePriceSession")\
        .getOrCreate()
    union_df = get_spark_df(spark, file_paths, code_name_map)
    task_4(union_df, main_purpose='住家用', building_state='住宅大樓', n=13)
    # Task 5.
    file_paths = np.random.choice(file_paths, 2)
    union_df = get_spark_df(spark, file_paths, code_name_map)
    save_dataframe2json(union_df, 'result-part2')
    spark.stop()
    if opts.api:
        os.system('python run.py')
