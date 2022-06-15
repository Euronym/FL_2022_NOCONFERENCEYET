import json
import numpy as np
import pandas as pd
import os 

PATH_ORIGINAL_DATASET = 'datasets/original_dataset/sensors'
PATH_FOR_DATASET = 'datasets/users_json/'
PATH_TO_SAVE = 'datasets/users_csv/'

sensors = ['accelerometer', 'gyroscope']

def divide_dataset_per_use(json_file, number):
    cwd = os.getcwd()
    # get all top level tags available on the dictionary
    top_level_tags = list(json_file.keys())
    # besides the first tag, all the remaining ones describe a sensor in the device
    available_sensors = top_level_tags[1:]
    # get the user id
    user_id = top_level_tags[0]
    # user directory path
    path_user_directory = cwd + "/datasets/users/" + str(json_file[user_id])
    if not os.path.exists(path_user_directory):
        # create directory for the user
        os.mkdir(path_user_directory)
    # directory to save the first ocurrence in 
    path_user_directory_ocurrence = path_user_directory + "/" + str(number)
    # create directory for the specific ocurrence
    os.mkdir(path_user_directory_ocurrence)
    # iterate over all existent sensors 
    for sensor in available_sensors:
        # get data from dictionary
        sensor_data = json_file[sensor]
        # transform obtained data into a json
        sensor_data_json = json.dumps(sensor_data)
        with open(path_user_directory_ocurrence + "/" + sensor + ".json", 'w') as sensor_file:
            sensor_file.write(sensor_data_json)
def read_user_data(user_id, sensors): 
    user_data = {}
    user_directory_content = os.listdir(PATH_FOR_DATASET + f'/{user_id}')
    for content in user_directory_content:
        for sensor in sensors:
            user_file = PATH_FOR_DATASET + f'{user_id}/{content}/{sensor}.json'
            file_size = os.path.getsize(user_file)
            if file_size != 0:
                with open(user_file, 'r') as file:
                    file_data = json.load(file)
                user_data[sensor] = file_data
            else:
                continue
    return user_data
def build_user_dataframe(user_data):
    # the coordinates for analysis
    valid_keys = {'x','y','z'}
    sensor_dict = {'X_accelerometer': [], 'Y_accelerometer': [], 'Z_accelerometer': [], 
                    'X_gyroscope': [], 'Y_gyroscope': [], 'Z_gyroscope': []}
    for sensor in user_data.keys():
        for measurament in user_data[sensor]:
            for measurement_key in measurament.keys():
                if measurement_key in valid_keys:
                    column_name = measurement_key.upper() + "_" + str(sensor)
                    sensor_dict[column_name].append(measurament[measurement_key])
    for key in sensor_dict.keys():
        pass
    #return pd.DataFrame(sensor_dict) 
users_directory = os.listdir(PATH_ORIGINAL_DATASET)
'''
for user in users_directory:
    number = user.split('_')[1].split('.')[0]
    with open(PATH_ORIGINAL_DATASET + user) as file:
        json_file = json.load(file)
    divide_dataset_per_use(json_file, number)
'''
users_root_directory_content = os.listdir(PATH_FOR_DATASET)
for user_name in users_root_directory_content:
    data = read_user_data(user_name, sensors=sensors)
    user_dataframe = build_user_dataframe(data)
    user_dataframe.to_csv(PATH_TO_SAVE + f'{user_name}.csv', index=None, header=True, sep=',')