# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# "Project Name"        :   "PerformanceDataExtraction"                 #
# "File Name"           :   "PerformanceDataExtraction"                 #
# "Author"              :   "rishabhzn200"                              #
# "Date of Creation"    :   "Aug-27-2018"                               #
# "Time of Creation"    :   "2:22 PM"                                   #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #



"""
For all the apps in the two CSV files:
(1)     Collect data for 6 months before Nov 2015 and 1 year after (Nov 2016): Performance data.
(2)     Collect Publisher meta data.
"""


import pyspark as spark # only run after findspark.init()

from pyspark.sql import SparkSession, functions as F
from pyspark.sql import SQLContext
import sys
from datetime import datetime as dt
import pandas as pd
from tqdm import tqdm
import os
from ApptopiaAPIV2 import *


def Retry(id, store='', country = None, start_date = '2015-05-01', end_date = '2015-11-01'):
    import time
    perfdata = []
    status = 0
    for retnum in range(5):
        perfdata, status = get_estimates(id, store=store, country = country, start_date=start_date, end_date=end_date)
        if status == 200:
            return perfdata, status
        else:
            # Retry 5 times else give up
            with open('./errorfile.txt', 'a') as f:
                f.write(f'\nID = {id}\tdata = {perfdata}\tRetry Num = {retnum}\n\n')
            time.sleep(3)

    return perfdata, status


def GetEstimatesForPartitionWithIndex_Itunes(partition_id, start_date = '2015-05-01', end_date = '2015-11-01'):

    def GetPartitionData(split_index, elementIterator):

        import time

        # countries = ["US","CA","RU","AU","DE","KR","JP","CN","GB","ES","FR","IT","MX","NL","SE","BR","SG","TW","CH","HK","TH","DK","TR","NZ","BE","MY","AT","PH","IE","NO","IN","SA","IL","ID","GR","PL","CL","AR","CO","PT","AE","FI","RO","CZ","HU","KW","VE","VN","HR","EG","UA","ZA","KE","NG","BG","PK","RS","BD"]
        countries = ["US", "CA", "AT", "BE", "BG", "CZ", "DK", "FI", "FR", "DE", "GR", "HU", "IE", "IT", "NL", "PL",
                     "PT", "RO", "ES", "SE", "GB"]

        # Only execute if the split_index is equal to the partition id
        if split_index == partition_id:

            estimatedata = []
            for val in list(elementIterator):

                for country in countries:
                    # print(f"\n\nDate = {str(val.date).split(' ')[0]} == {val.id}\n")

                    # TODO Use try catch if date is not valid format
                    # start_date = str(val.date).split(' ')[0]

                    # Start Date 6 month before November 2015 and 1 year after November 2016
                    # start_date = '2015-05-01'
                    # end_date = '2015-11-01'
                    perfdata, status = get_estimates(val.id, store='itunes_connect', country = country, start_date=start_date, end_date=end_date)


                    if status != 200:
                        with open('./errorfile.txt', 'a') as f:
                            f.write(
                                '\n\n___________________________________________New Error Itunes_Connect___________________________________________\n')
                            f.write(f'\nPartition ID = {partition_id}\n')
                            f.write(f'\nID = {val.id}\tdata = {perfdata}\n\n')
                        time.sleep(1)

                        # Retry 5 times
                        perfdata, status = Retry(val.id, store='itunes_connect', country = country, start_date=start_date, end_date=end_date)
                        if status != 200:
                            # log to file
                            with open('./errorfile.txt', 'a') as f:
                                f.write(f'\nID = {id}\tdata = {perfdata}\t\n')
                                f.write(f'Failed to get the performance data for id = {val.id}')
                        else:
                            with open('./errorfile.txt', 'a') as f:
                                f.write(f'\nID = {id}\tdata = {perfdata}\t\n')
                                f.write('Successfully got the performance data')

                        # Write version data once retry is done
                        estimatedata.extend(perfdata)
                    else:
                        # If status is ok write the version data
                        estimatedata.extend(perfdata)

            yield estimatedata

    return GetPartitionData


# def GetEstimatesForPartitionWithIndex_GooglePlay(partition_id):
#
#     def GetPartitionData(split_index, elementIterator):
#
#         # countries = ["US","CA","RU","AU","DE","KR","JP","CN","GB","ES","FR","IT","MX","NL","SE","BR","SG","TW","CH","HK","TH","DK","TR","NZ","BE","MY","AT","PH","IE","NO","IN","SA","IL","ID","GR","PL","CL","AR","CO","PT","AE","FI","RO","CZ","HU","KW","VE","VN","HR","EG","UA","ZA","KE","NG","BG","PK","RS","BD"]
#         countries = ["US", "CA", "AT", "BE", "BG", "CZ", "DK", "FI", "FR", "DE", "GR", "HU", "IE", "IT", "NL", "PL",
#                      "PT", "RO", "ES", "SE", "GB"]
#
#         # Only execute if the split_index is equal to the partition id
#         if split_index == partition_id:
#             x = 20
#             # abcd =  map(lambda x: get_app_metadata(x.id, store='google_play') ,list(iterator))
#             datacountrydict = {}
#             abcd = []
#             for country in countries:
#
#                 for val in list(elementIterator):
#                     start_date = str(val.date).split(' ')[0]
#                     perfdata = get_estimates(val.id, store='google_play', country = country, start_date=start_date)
#                     print(f'\nPerf Data Data ---- \n{perfdata}\n')
#                     abcd.extend(perfdata)
#                 # datacountrydict[country] = abcd
#             # print(abcd)
#             yield abcd
#
#     return GetPartitionData




def SaveEstimateForRDD(app_estimates_rdd, hdf5storeOutput, hdpath):

    for part_id in range(app_estimates_rdd.getNumPartitions()):
        part_rdd = app_estimates_rdd.mapPartitionsWithIndex(GetEstimatesForPartitionWithIndex_Itunes(part_id, start_date = '2015-05-01', end_date = '2015-11-01')) # index and iterator will be passed internally
        data_from_part_rdd = part_rdd.collect()
        z = 20


        # Save the partial data
        # Error Handling
        pdf = None
        try:
            # Convert to Pandas DataFrame
            pdf = pd.DataFrame(data_from_part_rdd[0])

            # Change all the columns to string
            cols = pdf.columns
            for col in cols:
                pdf[col] = pdf[col].astype(str)
        except:
            # Log the error
            with open('./errorfile_perf.txt', 'a') as f:
                f.write(
                    '\n\n___________________________________________New Error Itunes_Connect___________________________________________\n')
                f.write(f'\nError ID = {part_id}\n\n')
                f.write(f'\ndata = {data_from_part_rdd}\n\n')

            pdf = pd.DataFrame()

        # Save in HDF5 files # can save in different partitions
        hdf5storeOutput.append(f'{hdpath}/{part_id}', pdf, format='table', data_columns=True, append=True)

        z = 20



if __name__ == "__main__":
    # Build Session
    session = SparkSession.builder.appName("DataSpark").getOrCreate()

    # SparkContext
    sc = session.sparkContext

    # HDF5 Store Output
    hdf5storeOutput = pd.HDFStore('./AppEstimates_BeforeNov2015.h5')

    #filenames
    filenames = ['ids_6012_151029.csv', 'ids_60126024_161104.csv']
    # Open CSV files and parallelize
    # 96643 + 223427 = 320170

    # Read the ids form the csv files
    file1_df = pd.read_csv(f'./input/{filenames[0]}')
    file2_df = pd.read_csv(f'./input/{filenames[1]}')

    # Combine the ids
    appidsdf = pd.concat([file1_df, file2_df], ignore_index=True)

    # drop the duplicate ids
    appidsdf_unique = appidsdf.drop_duplicates()

    # Create the spark dataframe from the pandas dataframe
    sparkPdf = session.createDataFrame(appidsdf_unique)

    # Repartition the dataframe
    sparkPdf_repartition = sparkPdf.repartition(1000)

    # Create the RDD
    sparkpdf_rdd = sparkPdf_repartition.rdd

    # define hdpath: path/key to save the outptu file
    hdpath = '/itunes_connect/estimates'
    SaveEstimateForRDD(sparkpdf_rdd, hdf5storeOutput, hdpath)


    # HDF5 Store Output
    # hdf5storeOutput = pd.HDFStore('./AppEstimates_Sample.h5')

    # spark_df_itunes = session.read.csv('./all_ids_id_and_date_sample/itunes_connect_all_ids.csv', inferSchema=True, header=True)
    # spark_df_gp = session.read.csv('./all_ids_id_and_date_sample/google_play_all_ids.csv', inferSchema=True, header=True)


    # For loop for itunes and google play
    stores = ['itunes_connect']#TODO activate later, 'google_play']

    # for store in stores:
    #     for path in pathlist[0:1]:
    #
    #         # create the path name to fetch the data from
    #         hdpath = f'/{store}/{path}'
    #
    #         # get the data
    #         pdf = hdf5storeInput.get(hdpath)
    #
    #         # create the spark dataframe from the data extracted
    #         sparkPdf = session.createDataFrame(pdf)
    #
    #         # Create an rdd from the dataframe after creating a few partitions
    #         app_estimates_rdd = sparkPdf.repartition(100).rdd
    #
    #         # U need to use map partitions to get the estimates data
    #         SaveEstimateForRDD(app_estimates_rdd, hdf5storeOutput, hdpath)
    #
    #         z = 20
