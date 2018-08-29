# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# "Project Name"        :   "DataExtractSpark"                          #
# "File Name"           :   "AppVersionExtraction"                      #
# "Author"              :   "rishabhzn200"                              #
# "Date of Creation"    :   "Aug-15-2018"                               #
# "Time of Creation"    :   "11:32 PM"                                  #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

import pyspark as spark # only run after findspark.init()

from pyspark.sql import SparkSession, functions as F
from pyspark.sql import SQLContext
import sys
from datetime import datetime as dt
import pandas as pd
from tqdm import tqdm
import os
from ApptopiaAPIV2 import *



def Retry(id, store='', country = None):
    import time
    versiondata = []
    status = 0
    for retnum in range(5):
        versiondata, status = get_app_version_history(id, store=store, country=country)
        if status == 200:
            return versiondata, status
        else:
            # Retry 5 times else give up
            with open('./errorfile.txt', 'a') as f:
                f.write(f'\nID = {id}\tdata = {versiondata}\tRetry Num = {retnum}\n\n')
            time.sleep(3)

    return versiondata, status

def GetVersionForPartitionWithIndex_Itunes(partition_id):

    def GetPartitionData(split_index, elementIterator):
        import time

        # countries = ["US","CA","RU","AU","DE","KR","JP","CN","GB","ES","FR","IT","MX","NL","SE","BR","SG","TW","CH","HK","TH","DK","TR","NZ","BE","MY","AT","PH","IE","NO","IN","SA","IL","ID","GR","PL","CL","AR","CO","PT","AE","FI","RO","CZ","HU","KW","VE","VN","HR","EG","UA","ZA","KE","NG","BG","PK","RS","BD"]
        #
        # countries = ["US", "CA", "RU", "AU", "DE", "KR", "JP", "CN", "GB", "ES", "FR", "IT", "MX", "NL", "SE", "BR",
        #              "SG", "TW", "CH", "HK", "TH", "DK", "TR", "NZ", "MY", "PH", "IE", "NO", "IN", "SA",
        #              "IL", "ID", "GR", "PL", "CL", "AR", "CO", "PT", "AE", "FI", "RO", "CZ", "HU", "KW", "VE", "VN",
        #              "HR", "EG", "UA", "ZA", "KE", "NG", "BG", "PK", "RS", "BD"]


        countries = ["US", "CA", "AT", "BE", "BG", "CZ", "DK", "FI", "FR", "DE", "GR", "HU", "IE", "IT", "NL", "PL", "PT", "RO", "ES", "SE", "GB"]

        # Only execute if the split_index is equal to the partition id
        if split_index == partition_id:
            x = 20
            # abcd =  map(lambda x: get_app_metadata(x.id, store='google_play') ,list(iterator))
            datacountrydict = {}
            abcd = []
            for val in elementIterator:

                for country in countries[1:]: #exclude US this time
                    versiondata, status = get_app_version_history(val.id, store='itunes_connect', country = country)
                    if status != 200:
                        with open('./errorfile.txt', 'a') as f:
                            f.write(
                                f'\n\n___________________________________________New Error Itunes_Connect___________________________________________\n')
                            f.write(f'\nPartition ID = {partition_id}\n')
                            f.write(f'\nID = {val.id}\tdata = {versiondata}\n\n')
                            # f.write(f'')
                        # print(f'\nVersion Data ---- \n{versiondata}\n')
                        time.sleep(1)

                        # Retry 5 times
                        versiondata, status = Retry(val.id, store='itunes_connect', country = country)
                        if status != 200:
                            # log to file
                            with open('./errorfile.txt', 'a') as f:
                                f.write(f'\nID = {id}\tdata = {versiondata}\t\n')
                                f.write(f'Failed to get the data for id = {val.id}')
                        else:
                            with open('./errorfile.txt', 'a') as f:
                                f.write(f'\nID = {id}\tdata = {versiondata}\t\n')
                                f.write(f'Successfully got the version data')

                        # Write version data once retry is done
                        abcd.extend(versiondata)
                    else:
                        # If status is ok write the version data
                        abcd.extend(versiondata)
                # datacountrydict[country] = abcd
            # print(abcd)
            yield abcd

    return GetPartitionData


def GetVersionForPartitionWithIndex_GooglePlay(partition_id):

    def GetPartitionData(split_index, elementIterator):
        import time
        # countries = ["US","CA","RU","AU","DE","KR","JP","CN","GB","ES","FR","IT","MX","NL","SE","BR","SG","TW","CH","HK","TH","DK","TR","NZ","BE","MY","AT","PH","IE","NO","IN","SA","IL","ID","GR","PL","CL","AR","CO","PT","AE","FI","RO","CZ","HU","KW","VE","VN","HR","EG","UA","ZA","KE","NG","BG","PK","RS","BD"]

        countries = ["US", "CA", "AT", "BE", "BG", "CZ", "DK", "FI", "FR", "DE", "GR", "HU", "IE", "IT", "NL", "PL",
                     "PT", "RO", "ES", "SE", "GB"]

        # Only execute if the split_index is equal to the partition id
        if split_index == partition_id:
            x = 20
            # abcd =  map(lambda x: get_app_metadata(x.id, store='google_play') ,list(iterator))
            datacountrydict = {}
            abcd = []
            for val in elementIterator:
                for country in countries:
                    versiondata, status = get_app_version_history(val.id, store='google_play', country = country)

                    if status != 200:
                        with open('./errorfile.txt', 'a') as f:
                            f.write(
                                f'\n\n___________________________________________New Error GooglePlay___________________________________________\n')
                            f.write(f'\nPartition ID = {partition_id}\n')
                            f.write(f'\nID = {val.id}\tdata = {versiondata}\n\n')
                            # f.write(f'')
                        # print(f'\nVersion Data ---- \n{versiondata}\n')
                        time.sleep(1)

                        # Retry 5 times
                        versiondata, status = Retry(val.id, store='google_play', country=country)
                        if status != 200:
                            # log to file
                            with open('./errorfile.txt', 'a') as f:
                                f.write(f'\nID = {id}\tdata = {versiondata}\t\n')
                                f.write(f'Failed to get the data for id = {val.id}')
                        else:
                            with open('./errorfile.txt', 'a') as f:
                                f.write(f'\nID = {id}\tdata = {versiondata}\t\n')
                                f.write(f'Successfully got the version data')

                        # Write version data once retry is done
                        abcd.extend(versiondata)
                    else:
                        # If status is ok write the version data
                        abcd.extend(versiondata)
                # datacountrydict[country] = abcd
            # print(abcd)
            yield abcd

    return GetPartitionData





if __name__ == "__main__":
    # Build Session
    session = SparkSession.builder.appName("DataSpark").getOrCreate()

    # SparkContext
    sc = session.sparkContext

    # Open CSV files and parallelize
    # pandas_df = pd.read_csv('./itunes_connect_all_ids.csv')

    # HDF5 Stores for Itunes_Connect and Google_Play
    hdf5store = pd.HDFStore('./AppVersionHistory_IC_other_p34.h5') # change
    hdf5store.close()
    # hdf5store = pd.HDFStore('./AppVersionHistory_GP.h5')
    # hdf5store.close()

    spark_df_itunes = session.read.csv('./all_ids_id_and_date_split/itunes_connect_all_ids_part3.csv', inferSchema=True, header=True) # change
    # spark_df_gp = session.read.csv('./all_ids_id_and_date_sample_1000/google_play_all_ids.csv', inferSchema=True, header=True)
    # spark_df.rdd.repartition(4)
    #
    # spark_df.show()
    # a = sc.defaultParallelism

    #TODO Repartition if required. Tune this..!!
    spark_df_itunes = spark_df_itunes.repartition(100)
    # spark_df_gp = spark_df_gp.repartition(5)

    #TODO Get all the countries. Can hardcode to use only US and EU countries
    countries = get_countries()


    # Create RDD from spark dataframes
    app_version_itunes_rdd = spark_df_itunes.rdd
    # app_version_google_play_rdd = spark_df_gp.rdd


    # 1. App Version

    # One Way. USe countries inside the function.

    app_version_itunes = {}
    app_version_google_play = {}


    for part_id in tqdm(range(app_version_itunes_rdd.getNumPartitions())):
        part_rdd = app_version_itunes_rdd.mapPartitionsWithIndex(GetVersionForPartitionWithIndex_Itunes(part_id)) # index and iterator will be passed internally
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
            with open('./errorfile.txt', 'a') as f:
                f.write(
                    f'\n\n___________________________________________New Error Itunes_Connect___________________________________________\n')
                f.write(f'Error ID = {part_id}')
                f.write(f'data = {data_from_part_rdd}\n')

            pdf = pd.DataFrame()

        # Save in HDF5 files # can save in different partitions
        hdf5store = pd.HDFStore('./AppVersionHistory_IC_other_p34.h5') # change
        hdf5store.append(f'/itunes_connect/part3/{part_id}', pdf, format='table', data_columns=True, append=True) # change
        hdf5store.close()


    # Commenting GP code for now

    # for part_id in tqdm(range(app_version_google_play_rdd.getNumPartitions())):
    #     part_rdd = app_version_google_play_rdd.mapPartitionsWithIndex(GetVersionForPartitionWithIndex_GooglePlay(part_id)) # index and iterator will be passed internally
    #     data_from_part_rdd = part_rdd.collect()
    #     z = 20
    #
    #
    #     # Save the partial data
    #     # Error Handling
    #     pdf = None
    #     try:
    #         # Convert to Pandas DataFrame
    #         pdf = pd.DataFrame(data_from_part_rdd[0])
    #
    #         # Change all the columns to string
    #         cols = pdf.columns
    #         for col in cols:
    #             pdf[col] = pdf[col].astype(str)
    #     except:
    #         # Log the error
    #         with open('./errorfile.txt', 'a') as f:
    #             f.write(
    #                 f'\n\n___________________________________________New Error Google Play___________________________________________\n')
    #             f.write(f'Error ID = {part_id}')
    #             f.write(f'data = {data_from_part_rdd}\n')
    #
    #         pdf = pd.DataFrame()
    #
    #     # Save in HDF5 files # can save in different partitions
    #     hdf5store = pd.HDFStore('./AppVersionHistory_GP.h5')
    #     hdf5store.append(f'/google_play/{part_id}', pdf, format='table', data_columns=True, append=True)
    #     hdf5store.close()
