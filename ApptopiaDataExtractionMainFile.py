# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# "Project Name"        :   "Code"                                      #
# "File Name"           :   "ApptopiaDataExtractionMainFile"            #
# "Author"              :   "rishabhzn200"                              #
# "Date of Creation"    :   "Aug-01-2018"                               #
# "Time of Creation"    :   "8:41 AM"                                   #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #


import pandas as pd
import numpy as np
import datetime as dt
import HDF5Helper as hdf5helper
from tqdm import tqdm

from ApptopiaAPIV2 import *
from ApptopiaNewReleases import NewReleaseIds
# from ApptopiaNewReleaseDataExtraction import NewReleaseData

def FeaturedAppDataExtractionTimePeriod():
    pass



def FeaturedAppDataExtractionPerDay():
    pass


def NewReleaseDataExtractionForMonth():

    # Finds the new ids in the given period for all the categories and for both app store and play store

    # Find the start date and the end date for a month
    end_date, start_date = dt.date.today() + dt.timedelta(-1), dt.date.today() + dt.timedelta(-30) + dt.timedelta(-1)

    # end_date, start_date = dt.date.today() + dt.timedelta(-1), dt.date.today() + dt.timedelta(-2)

    sm, sd = '{:02d}'.format(start_date.month), '{:02d}'.format(start_date.day)
    start_date_str = f'{start_date.year}-{sm}-{sd}'

    em, ed = '{:02d}'.format(end_date.month), '{:02d}'.format(end_date.day)
    end_date_str = f'{end_date.year}-{em}-{ed}'

    dates = [str(start_date_str), str(end_date_str)]

    NewReleaseIds(start_date=start_date_str, end_date=end_date_str, apptopiastorefile=apptopiastorename)


    # Extract all the data for those ids
    # This function will check each id in the store and its release date and checkout date. If both are same, then it will checkout the daa from the release date to current date
    # This is to be done only for the estimates only.
    # NewReleaseData(apptopiastorefile=apptopiastorename)


def NewReleaseDataExtractionPerDay():

    # Find the start date and the end day. Usually only one day is passed

    end_date, start_date = dt.date.today() + dt.timedelta(-1), dt.date.today() + dt.timedelta(-1)

    sm, sd = '{:02d}'.format(start_date.month), '{:02d}'.format(start_date.day)
    start_date_str = f'{start_date.year}-{sm}-{sd}'

    em, ed = '{:02d}'.format(end_date.month), '{:02d}'.format(end_date.day)
    end_date_str = f'{end_date.year}-{em}-{ed}'

    dates = [str(start_date_str), str(end_date_str)]

    NewReleaseIds(start_date=start_date_str, end_date=end_date_str, apptopiastorefile=apptopiastorename)


    # NewReleaseData(apptopiastorefile=apptopiastorename)
    pass


def AutoDownload():
    pass

def AppDiscovery():

    for store in ['google_play', 'itunes_connect']:
        next_page_token=None
        unique_id_dict = {}
        pno = 1
        while True:
            apps = app_discovery(store=store, next_page_token=next_page_token)
            print(f'{pno} discovered')

            if apps == '[]':
                # Continue with the same token number
                continue

            # Create two more tables. Id, appname, release date AND ID, Desc
            applist = []
            appIdDescList = []

            # appdict = {}
            for app in apps['result_rows']:

                if app['id'] in unique_id_dict.keys():
                    continue
                else:
                    try:
                        unique_id_dict[app['id']] += 1
                    except:
                        unique_id_dict[app['id']] = 1

                    # Add to df
                    appdict = {}
                    appdict['appid'] = app['id']
                    appdict['appname'] = app['name']
                    appdict['initial_release_date'] = app['initial_release_date']
                    applist.append(appdict)

                    # Add to desc df
                    appiddesc = {}
                    appiddesc['appid'] = app['id']
                    appiddesc['description'] = app['description']
                    appIdDescList.append(appiddesc)


            apps_df = pd.DataFrame(applist)
            appiddescdf = pd.DataFrame(appIdDescList)
            appresultrowsdf = pd.DataFrame(apps['result_rows'])

            # Remove description col from appresultrowsdf
            newappresultrowsdf = appresultrowsdf.drop(['description'], axis=1)



            df_headers = list(newappresultrowsdf)
            print(df_headers)

            for col in df_headers:
                newappresultrowsdf[col] = newappresultrowsdf[col].astype(str)

            #Convert Columns
            # appresultrowsdf['approx_size_bytes'] = appresultrowsdf['approx_size_bytes'].astype(str)
            # appresultrowsdf['category_ids'] = appresultrowsdf['category_ids'].astype(str)
            # appresultrowsdf['category_id'] = appresultrowsdf['category_id'].astype(str)
            # appresultrowsdf['offers_in_app_purchases'] = appresultrowsdf['offers_in_app_purchases'].astype(str)
            # appresultrowsdf['other_stores'] = appresultrowsdf['other_stores'].astype(str)
            # appresultrowsdf['permissions'] = appresultrowsdf['permissions'].astype(str)
            # appresultrowsdf['screenshot_urls'] = appresultrowsdf['screenshot_urls'].astype(str)


            h5 = hdf5helper.HDF5Helper()
            hdf5Obj = h5.GetHDF5Store(apptopiastorename)
            hdf5Obj.append(f'/Discovery/AppIds/{store}', apps_df, format='table', append=True, min_itemsize=1000, data_columns=True)
            hdf5Obj.append(f'/Discovery/AppIdDesc/{store}', appiddescdf, format='table', append=True, min_itemsize=50000,
                           data_columns=True)
            hdf5Obj.append(f'/Discovery/AllData/{store}', newappresultrowsdf , format='table', append=True, min_itemsize=8000, data_columns=True)
            hdf5Obj.close()

            next_page_token = apps['next_page_token']
            pno+=1

            if next_page_token == None:
                print('Normal Exit')
                break
        z = 20
        pass



if __name__ == "__main__":

    # Name for the hdf5 store
    apptopiastorename = './ApptopiaStoreV1.h5'

    # Options for extracting data are:
    # 0. Featured apps data extraction for 1 year
    # 1. Featured apps data extraction per day
    # 2. New releases data extraction for previous month
    # 3. New release data extraction on daily basis
    # 4. Auto Download the data based on the difference in the dates
    # 5. Discover all ids

    # Select an option
    option = [0,1,2,3,4,5][5]

    # Create a dictionary of option ids as keys and function definition as values
    extract_data = {0: FeaturedAppDataExtractionTimePeriod,
                    1: FeaturedAppDataExtractionPerDay,
                    2: NewReleaseDataExtractionForMonth,
                    3: NewReleaseDataExtractionPerDay,
                    4: AutoDownload,
                    5: AppDiscovery,
                    }

    # # Create and HDF5 Store to store the data
    hdfhelper = hdf5helper.HDF5Helper()
    store = hdfhelper.CreateStore(apptopiastorename)
    store.close()

    # The above store will have data in the following formats:
    # /CheckoutDates/itunes_connect ; /CheckoutDates/google_play ;
    # /itunes_connect/metadata ; /itunes_connect/version ; /itunes_connect/estimates ; /itunes_connect/sdks ;
    # /google_play/metadata ; /google_play/version ; /google_play/estimates ; /google_play/sdks

    # estimates and version will have country code as inner files



    # Call the necessary function
    extract_data[option]()
    pass