# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# "Project Name"        :   "Code"                                      #
# "File Name"           :   "DataExtraction.py"                         #
# "Author"              :   "rishabhzn200"                              #
# "Date of Creation"    :   "Jul-28-2018"                               #
# "Time of Creation"    :   "10:03 PM"                                  #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #




import http.client
import pandas as pd

from ApptopiaAPIs import *
import os
import h5py
import numpy as np


def store_data_in_hdf(key, data, filename, format='table'):
    hdf = pd.HDFStore(filename) #'./hdf5_pandas.h5')
    hdf.put(key, data, format=format, data_columns=True)
    hdf.close()
    pass


def read_data_from_hdf(filename):
    newhdf = pd.HDFStore(filename, mode='r')
    # print(newhdf.keys())

    df = newhdf.get('Categories')
    newhdf.close()
    return df


def append_data_to_hdf(key, data, filename, format='table'):
    hdf = pd.HDFStore(filename)
    hdf.put(key, data, format=format, append=True, data_columns=True)



def features_lists(conn, headers, store='itunes_connect', category_ids=None, from_='2016-05-15', to_='2016-05-16', outfile_prefix = 'featured_list'):
    '''

    :param conn: active connection
    :param headers: request headers
    :param store: It can be ''itunes_connect' or 'google_play'
    :param category_ids: list of ids within each categories
    :param from_: date start
    :param to_: date end
    :return: None
    '''


    # Step 2: Get the ids from all the categories for apps for a specified period [New Releases]

    # for index, catno in appst_category_ids.iteritems():
    #
    #     # Get the new released apps
    #     appids_app_st = get_new_released_apps(conn, headers, store[0], date_from='2018-06-28', date_to='2018-07-04', country='US',
    #                           category_id=catno)
    #     # print(catno)
    # z = 20
    #
    # for index, catno in playst_category_ids.iteritems():
    #     # Get the new released apps
    #     appids_play_st = get_new_released_apps(conn, headers, store[1], date_from='2018-06-28', date_to='2018-07-04',
    #                                           country='US',
    #                                           category_id=catno)
    #     print(catno)

    # Step 2: Get the ids from all the categories for apps for a specified period [Featured Apps]
    # get all the ids for all the dates. Have tried for 1 date. Do it for all the dates.

    # Once you get the ids, save it and then start other api calls for each one fo them

    # Step: For all featured apps for a single date

    dates = get_date_range(from_=from_, to_=to_)

    datelistids = {}
    for date in dates:

        play_st_id_dict = {}
        for index, catno in category_ids.iteritems():

            if catno == 7:
                x = 20

            try:
                appids_app_st = get_featured_apps(conn, headers, store=store, date=date, country='US',
                                              category_id=catno)
            except:
                appids_app_st = []
                conn = http.client.HTTPSConnection("integrations.apptopia.com")
            # appids_app_st : list of dict {'name': , 'app_ids':[]}

            if appids_app_st == []:
                play_st_id_dict[catno] = []

            else:
                appids_app_st_df = pd.DataFrame(appids_app_st)
                print('**', str(catno))
                print(appids_app_st_df.head(4))
                idSeries = appids_app_st_df['app_ids']

                idlist = idSeries.tolist()

                idlistnew = []
                for lst in idlist:
                    idlistnew.extend(lst)

                play_st_id_dict[catno] = idlistnew

            datelistids[date] = play_st_id_dict

    z = 20

    # Step 3: Find the app id specific data - app_metadata, version, sdk, estimates. Metadata - Done. For itunes
    # app_metadata_filename = './' + outfile_prefix + store + '_app_metadata_file'
    # app_metadata_df = pd.DataFrame()
    #
    # for date in dates:
    #     for index, catno in category_ids.iteritems():
    #
    #         appidlist = datelistids[date][catno]
    #         for appid in appidlist:
    #             app_metadata = get_app_metadata(conn, headers, appid, store=store)
    #             # app_metadata[appid] = appid
    #             series = pd.Series([])
    #             if app_metadata != []:
    #                 series = pd.Series(app_metadata[0], index=app_metadata[0].keys())
    #             app_metadata_df[appid] = series
    #             z = 20
    #
    #     app_metadata_df.to_csv(app_metadata_filename + '.csv', mode='a', header=True)
    #     # app_sdks_df.to_hdf(app_sdk_filename + '.h5', "APP_SDK", format='table', data_columns=True)
    #     app_metadata_df = pd.DataFrame()
    #
    # print(app_metadata_df.head(4))
    #
    # # Step 4: Find app version history
    # app_version_filename = './' + outfile_prefix + store + '_app_version_hist_file'
    # app_version_history_df = pd.DataFrame()
    #
    # for date in dates:
    #     for index, catno in category_ids.iteritems():
    #
    #         appidlist = datelistids[date][catno]
    #         for appid in appidlist:
    #             app_version_history = get_app_version_history(conn, headers, appid, store=store)
    #             series = pd.Series([])
    #             if app_version_history != []:
    #                 series = pd.Series(app_version_history[0], index=app_version_history[0].keys())
    #             app_version_history_df[appid] = series
    #
    #             z = 20
    #
    #     app_version_history_df.to_csv(app_version_filename + '.csv', mode='a', header=True)
    #     # app_sdks_df.to_hdf(app_sdk_filename + '.h5', "APP_SDK", format='table', data_columns=True)
    #     app_version_history_df = pd.DataFrame()



    # print(app_version_history_df.head(4))

    # # Step 5: Get app estimates
    # app_estimates_filename = './' + outfile_prefix + store + '_app_estimates_file'
    # app_estimates_df = pd.DataFrame()
    #
    # for date in dates:
    #     for index, catno in category_ids.iteritems():
    #
    #         appidlist = datelistids[date][catno]
    #         for appid in appidlist:
    #             app_estimates = get_estimates(conn, headers, appid, store=store, start_date=dates[0], end_date=dates[-1])
    #             app_estimates_df[appid] = pd.Series(app_estimates)
    #
    #             z = 20
    #
    #     app_estimates_df.to_csv(app_estimates_filename + '.csv', mode='a', header=True)
    #     # app_sdks_df.to_hdf(app_sdk_filename + '.h5', "APP_SDK", format='table', data_columns=True)
    #     app_estimates_df = pd.DataFrame()

    # print(app_estimates_df.head(3))



    # # Step 6: Get app sdks
    app_sdk_filename = './'+outfile_prefix+store+'_app_sdkfile'  #, store
    app_sdks_df = pd.DataFrame()
    for date in dates:
        for index, catno in category_ids.iteritems():

            appidlist = datelistids[date][catno]
            for appid in appidlist:
                try:
                    app_sdks = get_app_sdks(conn, headers, appid, store=store)
                except:
                    conn = http.client.HTTPSConnection("integrations.apptopia.com")
                    app_sdks = get_app_sdks(conn, headers, appid, store=store)
                app_sdks_df[appid] = pd.Series(app_sdks)

                # app_sdks_df.to_csv(app_sdk_filename + '_1.csv', mode='a', header=True)

                x = 0

        app_sdks_df.to_csv(app_sdk_filename + '.csv', mode='a', header=True)
        # app_sdks_df.to_hdf(app_sdk_filename + '.h5', "APP_SDK", format='table', data_columns=True)
        app_sdks_df = pd.DataFrame()

    z = 0



if __name__ == "__main__":
    # appid = 'com.mindcandy.sevensecondchallenge'
    # get_app_metadata(appid, store='google_play')
    #
    # get_app_metadata('651510680')
    #
    # get_app_version_history('651510680', store='itunes_connect', country='US')
    #
    # get_estimates('651510680')
    #
    # get_app_sdks('651510680')

    # category_data = get_categories()
    #
    # category_df = pd.DataFrame(category_data)
    #
    # hdf = pd.HDFStore('./hdf5_pandas.h5')
    # hdf.put("Categories", category_df, format='table', data_columns=True)
    # hdf.close()


    # newhdf = pd.HDFStore('./hdf5_pandas.h5', mode='r')
    # print(newhdf.keys())
    #
    # df = newhdf.get('Categories')
    #
    # newhdf.close()

    #Categories

    # print(df.head(10))
    #
    # a = 20

    # get_featured_apps()



    # get_countries()

    # Step 0: Basic Set up
    # conn = http.client.HTTPSConnection("integrations.apptopia.com")
    #
    # headers = {
    #     'authorization': "eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJhdWQiOiJpbnRlZ3JhdGlvbnMuYXBwdG9waWEuY29tIiwiZXhwIjoxNTMzODg0MTY5LCJpYXQiOjE1MzI2NzQ1NjksImlzcyI6ImludGVncmF0aW9ucy5hcHB0b3BpYS5jb20iLCJqdGkiOiJiZjIyMzYxMi02ZDIyLTQ5YjctYjFkNy1mODUxNzM5NjIwMTIiLCJuYmYiOjE1MzI2NzQ1NjgsInBlbSI6eyJnZW5lcmFsX2VuZHBvaW50cyI6MSwiaW50ZWdyYXRpb25zIjoxMCwicmF3X2VuZHBvaW50cyI6MzQzNTk3MzgzNjd9LCJzdWIiOiJDbGllbnQ6NTMiLCJ0eXAiOiJhY2Nlc3MifQ.VU83aItpDQB275l3f-utXAvrhq07Uz0dVWKBNQTOwbsAKhYD0XGF5S8-pHeGtPIrr0KfTxMTrKheDYk8jOrLqQ"
    # }

    store = ['itunes_connect', 'google_play']

    # Step 1: Get all the categories for apple and google

    # itunes
    categories_app_st = get_categories(store='itunes_connect')

    # google play
    categories_play_st = get_categories(store='google_play')


    categories_app_st_df = pd.DataFrame(categories_app_st)
    categories_play_st_df = pd.DataFrame(categories_play_st)

    # Saving category files one time
    if not os.path.isfile('./appst_categories.csv'):
        categories_app_st_df.to_csv('./appst_categories.csv')
        categories_play_st_df.to_csv('./playst_categories.csv')

    # Extract ids from categories
    appst_category_ids = categories_app_st_df['id']
    playst_category_ids = categories_play_st_df['id']



    # Featured apps for itunes.
    # features_lists(conn, headers, store='', from_='2016-05-15', to_='2016-05-16')
    features_lists(conn, headers, store='itunes_connect', category_ids=appst_category_ids, from_='2016-05-15', to_='2016-05-16', outfile_prefix = 'featured_list')

    # Featured apps for Google play
    features_lists(conn, headers, store='google_play', category_ids=playst_category_ids, from_='2016-05-15', to_='2016-05-16', outfile_prefix = 'featured_list')




    z = 20