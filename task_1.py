import os
from datetime import datetime
import numpy as np
import pandas as pd


# directories in repository
LIB_DIR = os.path.realpath(__file__)
BASE_DIR = os.path.join(LIB_DIR, '../')
RAW_DATA_DIR = os.path.join(BASE_DIR, 'raw_data')

# find raw data files
RAW_DATA_FILES = [file for file in os.listdir(RAW_DATA_DIR)]


def convert_string_to_date(date_element):
    year = int(date_element[:4])
    month = int(date_element[4:6])
    day = int(date_element[6:8])
    minutes = int(date_element[8:10])
    seconds = int(date_element[-2:])
    date = datetime(year, month, day, minutes, seconds)
    return date


def get_elements_from_file_name(file):
    file_name = str(file)
    file_name_elements = file_name.split('_')
    return file_name_elements


def get_date_time_value(file_name_elements):
    date_element = file_name_elements[3][:12]
    date = convert_string_to_date(date_element)
    return date


def get_activity_type_from_file_name(file_name_elements):
    activity_type = file_name_elements[0]
    return activity_type


def add_data_instance_to_dict(data, df, date, activity_type):
    data['date'] = date.strftime('%Y-%m-%d')
    data['hour'] = date.hour
    data['activity_type'] = activity_type
    data['activities'] = len(df)
    return data


def put_data_from_raw_files_to_dict():
    data = {}
    list_data_dict = []
    if len(RAW_DATA_FILES) > 0:
        for file in RAW_DATA_FILES:
            df = pd.read_parquet(os.path.join(RAW_DATA_DIR, file), engine='pyarrow')
            file_name_elements = get_elements_from_file_name(file)
            date = get_date_time_value(file_name_elements)
            activity_type = get_activity_type_from_file_name(file_name_elements)
            data = add_data_instance_to_dict(data, df, date, activity_type)
            list_data_dict.append(data)
            os.remove(os.path.join(RAW_DATA_DIR, file))
            data = {}
    else:
        return "No files found in raw_data directory"
    return list_data_dict


def group_activities_by_date_and_type():
    list_data_dict = put_data_from_raw_files_to_dict()
    if isinstance(list_data_dict, list):
        df = pd.DataFrame.from_dict(list_data_dict)
        piv = pd.pivot_table(df, values=['activities'], index=['date', 'hour'],
                             columns=['activity_type'], aggfunc=np.sum, fill_value=0)
        df_piv = pd.DataFrame(piv.to_records())
        df_piv.columns = ['Date', 'hour', 'impression_count', 'click_count']
    else:
        return f"Failed to prepare data output"
    return df_piv


def save_data_to_csv():
    df = group_activities_by_date_and_type()
    try:
        df.to_csv('output.csv', sep=',', index=False, date_format='%Y-%m-%d')
    except AttributeError:
        return "Job Failed"
    return "Job Succeeded"


print(save_data_to_csv())



