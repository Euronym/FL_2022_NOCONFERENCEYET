import json
import numpy as np
import pandas as pd
import os 

PATH_ORIGINAL_DATASET = 'datasets/original_dataset/sensors'
PATH_FOR_DATASET = 'datasets/users_json/'
PATH_TO_SAVE = 'datasets/users_ts/'

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
def equilibrate_data(sensor_dict):
    size_accelerometer = len(sensor_dict['X_accelerometer'])
    size_gyroscope = len(sensor_dict['X_gyroscope'])
    if size_accelerometer != size_gyroscope:
        if size_accelerometer > size_gyroscope:
            sensor_dict['X_gyroscope'].append(np.nan)
        else:
            sensor_dict['X_accelerometer'].append(np.nan)
        return equilibrate_data(sensor_dict)
    else:
        return sensor_dict
def build_user_time_series(user_data):
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

    equilibrated_dict = equilibrate_data(sensor_dict)
    return pd.DataFrame(equilibrated_dict)

'''
users_directory = os.listdir(PATH_ORIGINAL_DATASET)
for user in users_directory:
    number = user.split('_')[1].split('.')[0]
    with open(PATH_ORIGINAL_DATASET + user) as file:
        json_file = json.load(file)
    divide_dataset_per_use(json_file, number)
'''
users_root_directory_content = os.listdir(PATH_FOR_DATASET)
data = read_user_data(user_id=users_root_directory_content, sensor=sensors) 
dataframe = build_user_time_series(data)
print(dataframe)