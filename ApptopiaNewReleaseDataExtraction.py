# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# "Project Name"        :   "Code"                                      #
# "File Name"           :   "ApptopiaNewReleaseDataExtraction"          #
# "Author"              :   "rishabhzn200"                              #
# "Date of Creation"    :   "Jul-31-2018"                               #
# "Time of Creation"    :   "12:36 PM"                                  #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #


import pandas as pd
import os
import datetime as dt
from ApptopiaAPIV2 import *
import HDF5Helper as h5


def dump_data_to_file(listobj, filename, store='', type=None, hdf5store=None, country=None):
    '''

    :param listobj:
    :param filename:
    :param store:
    :param type:
    :param hdf5store:
    :param country:
    :return:
    '''

    if type == 'csv':
        list_df = pd.DataFrame(listobj)
        if os.path.isfile(filename):
            list_df.to_csv(filename, mode='a', header=False)
        else:
            list_df.to_csv(filename)
    elif type == 'hdf5':
        outputfilename = f'/{store}'
        if country is None:
            outputfilename = f'{outputfilename}/{filename}'
        else:
            outputfilename = f'{outputfilename}/{country}/{filename}'

        list_df = pd.DataFrame(listobj)
        # list_df = list_df.convert_objects()

        if filename == 'metadata':
            list_df['category_ids'] = list_df['category_ids'].astype(str)
            list_df['other_stores'] = list_df['other_stores'].astype(str)
            list_df['screenshot_urls'] = list_df['screenshot_urls'].astype(str)

        if filename == 'version':
            list_df['versions'] = list_df['versions'].astype(str)

        if filename == 'estimate':
            list_df['breakout'] = list_df['breakout'].astype(str)

        hdf5helper = h5.HDF5Helper()
        hdf5Obj = hdf5helper.GetHDF5Store(hdf5store)
        hdf5Obj.append(outputfilename, list_df, format='table', append=True, min_itemsize=5000, data_columns=True)



def save_app_metadata(store='', new_rls_ids = None, filename=None, hdf5store=None):

    st_metadata_list = []
    for index, id in new_rls_ids.iteritems():
        st_metadata = get_app_metadata(id, store=store)
        if len(st_metadata) > 1:
            print('This should not be happening. Length of metadata is > 1.')
        st_metadata_list.extend(st_metadata)

        if len(st_metadata_list) == 100:
            # Dump the data to the file
            dump_data_to_file(st_metadata_list, filename, store=store, type='hdf5', hdf5store=hdf5store)

            # Empty the list which is already dumped
            st_metadata_list = []

    if len(st_metadata_list) > 0:
        dump_data_to_file(st_metadata_list, filename, store=store, type='hdf5', hdf5store=hdf5store)


def save_app_version_history(store='',  new_rls_ids = None, filename=None, hdf5store=None):

    # Get the list of all the countries
    countries = get_countries()

    st_ver_hist_list = []
    for country in countries[:4]: #TODO Remove the debug statement
        for index, id in new_rls_ids.iteritems():
        # ctry = None

            # ctry=country
            st_ver_hist = get_app_version_history(id, store=store, country=country)
            # Check length
            if len(st_ver_hist) > 1:
                print("Version history list returning list of length greater than 1")

            # TODO Filter out the list which are empty. Don't add them to the list
            if st_ver_hist == []:
                continue
            st_ver_hist_list.extend(st_ver_hist)

            # if length of list is greater than 100. Dump it and then clear
            # if len(st_ver_hist_list) == 100:
        dump_data_to_file(st_ver_hist_list, filename, store=store, type='hdf5', hdf5store=hdf5store, country=country)
        st_ver_hist_list = []

        z = 20

    # if len(st_ver_hist_list) > 0:
    #     dump_data_to_file(st_ver_hist_list, filename, store=store, type='hdf5', hdf5store=hdf5store, country=country)
    #     app_st_ver_hist_list = []


def save_app_estimates(store='', new_rls_ids=None, new_rls_df=None, filename=None, hdf5store=None):
    countries = get_countries()

    current_date = dt.date.today() + dt.timedelta(-1)
    sm, sd = '{:02d}'.format(current_date.month), '{:02d}'.format(current_date.day)
    curr_date_str = f'{current_date.year}-{sm}-{sd}'

    # - For App Store
    st_estimates_list = []
    for country in countries[:4]: #TODO remove the debug statement
        for index, id in new_rls_ids.iteritems():
            # for dateindex, rls_date in app_st_new_rls_date.iteritems():
            # rls_date = app_st_new_rls_df['id']
            rls_date = new_rls_df.loc[new_rls_df['appid'] == id, 'initial_release_date']

            # TODO rls_date was getting more than one result, meaning duplicate ids in new release csv file. Check[Because of categories]
            # [(k, rls_date)] = rls_date.iteritems()
            rls_date = list(rls_date.iteritems())[0][1]  # Can return multiple results for initial_release_date. Select the first one
            st_estimate = get_estimates(id, store=store, country=country, start_date=rls_date,
                                            end_date=curr_date_str)
            st_estimates_list.extend(st_estimate)

        # At new country save and clear the list or save per country basis
        # if index % 3 == 0:
        dump_data_to_file(st_estimates_list, filename, store=store, type='hdf5', hdf5store=hdf5store, country=country)
        st_estimates_list = []

    # if len(st_estimates_list) > 0:
    #     dump_data_to_file(st_estimates_list, filename, store=store, type='hdf5', hdf5store=hdf5store, country=country)

    z = 20


def save_app_sdks(store='', new_rls_ids=None, filename=None, hdf5store=None):
    st_sdks_list = []
    for index, id in new_rls_ids.iteritems():
        st_sdks = get_app_sdks(id, store=store)
        st_sdks_list.extend(st_sdks)

        if index == 100:
            break
        if index % 10 == 0:
            dump_data_to_file(st_sdks_list, filename, store=store, type='hdf5', hdf5store=hdf5store)
            st_sdks_list = []

    if len(st_sdks_list) > 0:
        dump_data_to_file(st_sdks_list, filename, store=store, type='hdf5', hdf5store=hdf5store)


# if __name__ == "__main__":

def NewReleaseData(apptopiastorefile=None):

    # # App Store New Releases File Name
    # app_st_file = './app_store_new_releases.csv'
    #
    # # Play Store New Releases File Name
    # play_st_file = './play_store_new_releases.csv'

    # App Store New Releases File Name
    app_st_file = '/Checkout/itunes_connect'

    # Play Store New Releases File Name
    play_st_file = '/Checkout/google_play'


    # # App Store files
    # app_st_metadata_file = './itunes/app_st_metadata_file.csv'
    # app_st_ver_hist_file = './itunes/app_st_ver_hist_file.csv'
    # app_st_estimate_file = './itunes/app_st_estimate_file.csv'
    # app_st_sdks_file = './itunes/app_st_sdks_file.csv'

    # App Store files
    app_st_metadata_file = 'metadata'
    app_st_ver_hist_file = 'version'
    app_st_estimate_file = 'estimate'
    app_st_sdks_file = 'sdks'


    # # Play Store files
    # play_st_metadata_file = './google/play_st_metadata_file.csv'
    # play_st_ver_hist_file = './google/play_st_ver_hist_file.csv'
    # play_st_estimate_file = './google/play_st_estimate_file.csv'
    # play_st_sdks_file = './google/play_st_sdks_file.csv'

    # Play Store files
    play_st_metadata_file = 'metadata'
    play_st_ver_hist_file = 'version'
    play_st_estimate_file = 'estimate'
    play_st_sdks_file = '.sdks'


    # Read the csv file as dataframe and get the ids
    # app_st_new_rls_df = pd.DataFrame.from_csv(app_st_file)
    # play_st_new_rls_df = pd.DataFrame.from_csv(play_st_file)

    hdfhelper = h5.HDF5Helper()
    hdfstore = hdfhelper.ReadHDF5Store(apptopiastorefile)

    app_st_new_rls_df = None
    app_st_new_rls_ids = None

    play_st_new_rls_df = None
    play_st_new_rls_ids = None
    if app_st_file in hdfstore:
        app_st_new_rls_df = pd.DataFrame(hdfstore[app_st_file])
        app_st_new_rls_ids = app_st_new_rls_df['appid']

    if play_st_file in hdfstore:
        play_st_new_rls_df = pd.DataFrame(hdfstore[play_st_file])
        play_st_new_rls_ids = play_st_new_rls_df['appid']

    hdfstore.close()

    # Extract the ids from App Store DF and Play Store DF



    # Extract the release dates
    #TODO rels date not used as of now
    # app_st_new_rls_date = app_st_new_rls_df['initial_release_date']
    # play_st_new_rls_date = play_st_new_rls_df['initial_release_date']

    a = 20

    # Use those ids to find the other related data

    # Step 1: Find the app_metadata
    #  - For App Store
    if app_st_new_rls_df is not None:
        pass
        # save_app_metadata(store='itunes_connect', new_rls_ids=app_st_new_rls_ids, filename=app_st_metadata_file, hdf5store=apptopiastorefile)

    # - For Play Store
    if play_st_new_rls_df is not None:
        save_app_metadata(store='google_play', new_rls_ids=play_st_new_rls_ids, filename=play_st_metadata_file, hdf5store=apptopiastorefile)




    # Step 2: Find the app version history. Per country.
    # - For app store
    if app_st_new_rls_df is not None:
        pass
        # save_app_version_history(store='itunes_connect', new_rls_ids=app_st_new_rls_ids, filename=app_st_ver_hist_file, hdf5store=apptopiastorefile)

    # - For Play Store
    if play_st_new_rls_df is not None:
        save_app_version_history(store='google_play', new_rls_ids=play_st_new_rls_ids, filename=play_st_ver_hist_file, hdf5store=apptopiastorefile)




    # Step 3: Find the estimates. Based on appid, store, country and start and end date(current date)
    # current_date = dt.date.today() + dt.timedelta(-1)
    # sm, sd = '{:02d}'.format(current_date.month), '{:02d}'.format(current_date.day)
    # curr_date_str = f'{current_date.year}-{sm}-{sd}'
    #
    # # - For App Store
    if app_st_new_rls_df is not None:
        save_app_estimates(store='itunes_connect', new_rls_ids=app_st_new_rls_ids, new_rls_df=app_st_new_rls_df, filename=app_st_estimate_file, hdf5store=apptopiastorefile)

    # - For Play Store
    if play_st_new_rls_df is not None:
        save_app_estimates(store='google_play', new_rls_ids=play_st_new_rls_ids, new_rls_df=play_st_new_rls_df, filename=play_st_estimate_file, hdf5store=apptopiastorefile)



    # Step 4: Find the app_sdks

    # - For App Store
    if app_st_new_rls_df is not None:
        save_app_sdks(store='itunes_connect', new_rls_ids=app_st_new_rls_ids, filename=app_st_sdks_file, hdf5store=apptopiastorefile)

    # - For Play Store
    if play_st_new_rls_df is not None:
        save_app_sdks(store='google_play', new_rls_ids=play_st_new_rls_ids, filename=play_st_sdks_file, hdf5store=apptopiastorefile)

    pass