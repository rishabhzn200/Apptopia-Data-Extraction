# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# "Project Name"        :   "data"                                      #
# "File Name"           :   "ExtractHDFData"                            #
# "Author"              :   "rishabhzn200"                              #
# "Date of Creation"    :   "Aug-27-2018"                               #
# "Time of Creation"    :   "11:27 AM"                                  #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #



import h5py
# import findspark
# findspark.init()
# import pyspark as spark # only run after findspark.init()
# from pyspark.sql import SparkSession, functions as F
import sys
from datetime import datetime as dt
import pandas as pd
from tqdm import tqdm
# import os


def strtodate(x):
    try:
        return dt.strptime(x, "%Y-%m-%d")
    except:
        return None


def get_path_list_from_hdf5(g) :
    '''
    :param g: hdf5 file
    :return: return the list of all the paths/keys in the hdf5 file
    '''
    if   isinstance(g,h5py.File) or isinstance(g,h5py.Dataset) or isinstance(g,h5py.Group):
        pass
    else :
        print('Warning: Unknown item in hdf5 file', g.name)
        sys.exit("Terminating Execution")

    allpathlist = []
    if isinstance(g, h5py.Group) :
        for key, val in dict(g).items():
            subg = val
            retpathlist = []
            if 'P' not in key:
                retpathlist = get_path_list_from_hdf5(subg)
            if retpathlist == []:
                paths = [key]
            else:
                paths = list(map(lambda x: f'{key}/{x}', retpathlist))
            allpathlist.extend(paths)
    return allpathlist



def GenerateDataFromHDF(hf, pathlist, outputfilename=None):
    '''

    :param hf: hdf5 file
    :param pathlist: all the paths/keys having relevant data
    :param outputfilename: currently not being used. Can be used to save the data into csvs
    :return: It returns the generator object which can be used to retrieve the data incrementally
    '''
    # Iterate through itunes_connect path list
    for path in tqdm(pathlist):
        # newpath = path.split('/')

        # Add a '/' before the path
        newpath = '/' + path
        p = hf[newpath]

        # Create pandas dataframe
        appidsdf = pd.DataFrame(p['table'][:])

        # Get the header
        header = appidsdf.columns.values

        # Convert columns to string, except index and date column
        for col in header:
            if col != 'index':
                appidsdf[col] = appidsdf[col].apply(lambda x: x.decode("utf-8"))

            if col == 'initial_release_date':
                appidsdf[col] = appidsdf[col].apply(strtodate)
            z = 20

        # return the data incrementally
        yield appidsdf



def ExtractDataFromFile(hdfFile):
    '''

    :param hdfFile: input hdf5 file
    :return: None
    '''

    hf = h5py.File(hdfFile, 'r')

    # list(hf.keys())  this gives the list of keys within the current key

    # Write a function to get all the paths/keys
    listofpaths = get_path_list_from_hdf5(hf)
    # print(listofpaths)

    # Create the lists to store the paths
    itunes_connect_paths = []
    google_play_paths = []

    # Store itunes and google_play paths in separate lists
    for path in listofpaths:
        if 'itunes_connect' in path and 'AllData' in path:
            itunes_connect_paths.append(path)
        elif 'google_play' in path and 'AllData' in path:
            google_play_paths.append(path)

    # Log to the file
    with open('logging.text', 'a') as log:
        log.write(f'FileName: {hdfFile} ______________________________________\n')

    # Get the metdata for itunes_connect
    # Create a generator object hdf_data_itunes
    hdf_data_itunes = GenerateDataFromHDF(hf, itunes_connect_paths, outputfilename='itunes_connect_ids.csv')

    # Get the data from generator
    for hdfdata in hdf_data_itunes:
        # TODO Process data. This data is from one of the path in one hdf5 object
        mydata = hdfdata
        pass

    # Get the metdata for google_play
    # Create the generator object hdf_data_googleplay
    hdf_data_googleplay = GenerateDataFromHDF(hf, google_play_paths, outputfilename='google_play_ids.csv')

    for hdfdata in hdf_data_googleplay:
        # TODO Process data. This data is from one of the path in one hdf5 object
        mydata = hdfdata
        pass

    # End of function



if __name__ == "__main__":

    # DataFilePath: Path for all the hdf5 files
    datafilepath = './data/'

    # # Build Session
    # session = SparkSession.builder.appName("DataSpark").getOrCreate()
    #
    # # SparkContext
    #
    # sc = session.sparkContext

    for i in range(1, 1001):
        filename = f'{datafilepath}ApptopiaStoreV1_1000_{i}.h5'
        ExtractDataFromFile(filename)

    # End of main function