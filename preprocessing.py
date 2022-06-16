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
                file_data['timestamp'] = content
                user_data[sensor] = file_data
            else:
                continue
    return user_data
def equilibrate_data(sensor_dict):
    size_accelerometer = len(sensor_dict['X_accelerometer'])
    size_gyroscope = len(sensor_dict['X_gyroscope'])
    if size_accelerometer != 0 and size_gyroscope != 0:
        if size_accelerometer != size_gyroscope:
            if size_accelerometer > size_gyroscope:
                sensor_dict['X_gyroscope'].append(np.nan)
            else:
                sensor_dict['X_accelerometer'].append(np.nan)
            return equilibrate_data(sensor_dict)
        else:
            return sensor_dict
    else:
        return [] 
def slice_time_series(time_series, window_size):
    count = 0
    slice, sliced_time_series = [], []
    for i in range(len(time_series)):
        count += 1
        if count == window_size and len(slice) == window_size:
            sliced_time_series.append(slice)
            slice = []
            count = 0
        slice.append(time_series[i]) 
    return sliced_time_series 
def save_as_ts(sliced_dataset, user_id):
    PATH_TO_TS = PATH_TO_SAVE + f"/{user_id}.ts"
    # this assumes all data has the same amount of windows 
    time_series_observations = []
    for i in range(len(sliced_dataset[0])):
        time_series_observation = []
        for j in range(len(sliced_dataset)):
            time_series_observation.append(sliced_dataset[j][i]) 
        time_series_observations.append(time_series_observation)
    with open(PATH_TO_TS, 'w') as file:
        for observation in time_series_observations:
            for dimension in observation:
                dimension_as_string = ",".join(map(str, dimension))
                file.write(dimension_as_string)
                file.write(":")
            file.write("\n")
def build_user_time_series(user_data):
    # the coordinates for analysis
    valid_keys = {'x','y','z'}
    sensor_dict = {'X_accelerometer': [], 'Y_accelerometer': [], 'Z_accelerometer': [], 
                    'X_gyroscope': [], 'Y_gyroscope': [], 'Z_gyroscope': [], 'timestamp': []}
    for sensor in user_data.keys():
        for measurament in user_data[sensor]:
            for measurement_key in measurament.keys():
                if measurement_key in valid_keys:
                    column_name = measurement_key.upper() + "_" + str(sensor)
                    sensor_dict[column_name].append(measurament[measurement_key])
                elif measurament == 'timestamp' and sensor == 'accelerometer':
                    sensor_dict['timestamp'].append(measurement_key[measurement_key])
    equilibrated_dict = equilibrate_data(sensor_dict)
    return equilibrated_dict
'''
users_directory = os.listdir(PATH_ORIGINAL_DATASET)
for user in users_directory:
    number = user.split('_')[1].split('.')[0]
    with open(PATH_ORIGINAL_DATASET + user) as file:
        json_file = json.load(file)
    divide_dataset_per_use(json_file, number)
'''
users_root_directory_content = os.listdir(PATH_FOR_DATASET)
for user in users_root_directory_content:
    data = read_user_data(user_id=user, sensors=sensors) 
    user_time_series = build_user_time_series(data)
    #dataframe.to_csv(f'datasets/users_csv/{user}.csv', index=None, header=True)
    save_as_ts(user_time_series)
    break