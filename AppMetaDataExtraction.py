# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# "Project Name"        :   "DataExtractSpark"                          #
# "File Name"           :   "AppMetaDataExtraction"                     #
# "Author"              :   "rishabhzn200"                              #
# "Date of Creation"    :   "Aug-15-2018"                               #
# "Time of Creation"    :   "3:46 AM"                                   #
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


def myfunc_itunes(partition_id):

    def foreachPartition(split_index, elementIterator):

        if split_index == partition_id:

            x = 20
            # abcd =  map(lambda x: get_app_metadata(x.id, store='google_play') ,list(iterator))

            abcd = []
            for val in list(elementIterator):
                abcd.extend(get_app_metadata(val.id, store='itunes_connect'))

            # print(abcd)
            yield abcd
    return foreachPartition


def myfunc_google_play(partition_id):

    def foreachPartition(split_index, elementIterator):

        if split_index == partition_id:

            x = 20
            # abcd =  map(lambda x: get_app_metadata(x.id, store='google_play') ,list(iterator))

            abcd = []
            for val in list(elementIterator):
                abcd.extend(get_app_metadata(val.id, store='google_play'))

            # print(abcd)
            yield abcd
    return foreachPartition


if __name__ == "__main__":
    # Build Session
    session = SparkSession.builder.appName("DataSpark").getOrCreate()

    # SparkContext
    sc = session.sparkContext

    # Open CSV files and parallelize
    # pandas_df = pd.read_csv('./itunes_connect_all_ids.csv')

    # HDF5 Store
    hdf5store = pd.HDFStore('./AppMetadata.h5')

    spark_df_itunes = session.read.csv('./all_ids_id_and_date_sample/itunes_connect_all_ids.csv', inferSchema=True, header=True)
    spark_df_gp = session.read.csv('./all_ids_id_and_date_sample/google_play_all_ids.csv', inferSchema=True, header=True)
    # spark_df.rdd.repartition(4)
    #
    # spark_df.show()
    # a = sc.defaultParallelism

    # Repartition if required
    spark_df_itunes = spark_df_itunes.repartition(5)
    spark_df_gp = spark_df_gp.repartition(5)

    # Get all the countries
    countries = get_countries()

    # 1. App Metadata

    # One Way

    # # Get the data for each ID. If ids are more, we will see
    # app_metadata_itunes = spark_df_itunes.rdd.map(lambda x: get_app_metadata(x.id, store='itunes_connect'))
    #
    # # Get the data for each ID.
    # app_metadata_google_play = spark_df_gp.rdd.map(lambda x: get_app_metadata(x.id, store='google_play'))
    #
    # # Use for loop to get all the elements
    # for row in app_metadata_itunes.take(app_metadata_itunes.count()): print(row[1])
    # z = 20





    # Get the data for each ID. If ids are more, we will see
    app_metadata_itunes_rdd = spark_df_itunes.rdd#.map(lambda x: get_app_metadata(x.id, store='itunes_connect'))


    # Get the data for each ID.
    app_metadata_google_play_rdd = spark_df_gp.rdd#.map(lambda x: get_app_metadata(x.id, store='google_play'))

    # Iterate through each partitions

    # lambda x: get_app_metadata(x.id, store='itunes_connect')

    # Itunes Connect
    for part_id in range(app_metadata_itunes_rdd.getNumPartitions()):
        part_rdd = app_metadata_itunes_rdd.mapPartitionsWithIndex(myfunc_itunes(part_id), True)
        data_from_part_rdd = part_rdd.collect()
        z = 20

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
                f.write('\n\n___________________________________________New Error Itunes_Connect___________________________________________\n')
                f.write(f'Error ID = {part_id}')
                f.write(f'data = {data_from_part_rdd}\n')

            pdf = pd.DataFrame()

        # Save in HDF5 files # can save in different partitions
        hdf5store.append(f'/itunes_connect/{part_id}', pdf, format='table', data_columns=True, append=True)


    # Google Play
    for part_id in range(app_metadata_google_play_rdd.getNumPartitions()):
        part_rdd = app_metadata_google_play_rdd.mapPartitionsWithIndex(myfunc_google_play(part_id), True)
        data_from_part_rdd = part_rdd.collect()
        z = 20

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
                    '\n\n___________________________________________New Error Google Play___________________________________________\n')
                f.write(f'Error ID = {part_id}')
                f.write(f'data = {data_from_part_rdd}\n')

            pdf = pd.DataFrame()
        # Save in HDF5 files # can save in different partitions
        hdf5store.append(f'/google_play/{part_id}', pdf, format='table', data_columns=True, append=True)
