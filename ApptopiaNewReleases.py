# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# "Project Name"        :   "Code"                                      #
# "File Name"           :   "ApptopiaNewReleases"                       #
# "Author"              :   "rishabhzn200"                              #
# "Date of Creation"    :   "Jul-30-2018"                               #
# "Time of Creation"    :   "11:30 AM"                                  #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

import http.client
import ast
import pandas as pd
import os
import datetime as dt
import HDF5Helper as h5

def process_data(data):
    '''

    :param data: data in string format
    :return: data after using literal_eval
    '''

    # Pre-Process
    dataStr = data.replace(":null", ':None')

    # Evaluate
    dataStr = ast.literal_eval(dataStr)  # It is a list

    return (dataStr)


def SaveToFile(filename=None, data=None, type='hdf5', store=None, hdfsstorefile=None):

    if type == 'csv':
        if os.path.isfile(filename):
            data.to_csv(filename, mode='a', header=False)
        else:
            data.to_csv(filename)

    elif type == 'hdf5':
        h5helperObj = h5.HDF5Helper()
        hdfstore = h5helperObj.GetHDF5Store(hdfsstorefile)

        hdfstore.append(f'/Checkout/{store}', data, format='table', append=True, min_itemsize=5000, data_columns=True)

        hdfstore.close()
        pass


def get_countries():
    '''

    :return: the list of code for countries
    '''
    conn = http.client.HTTPSConnection("integrations.apptopia.com")

    payload = ""

    headers = {
        'authorization': "eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJhdWQiOiJpbnRlZ3JhdGlvbnMuYXBwdG9waWEuY29tIiwiZXhwIjoxNTMzODg0MTY5LCJpYXQiOjE1MzI2NzQ1NjksImlzcyI6ImludGVncmF0aW9ucy5hcHB0b3BpYS5jb20iLCJqdGkiOiJiZjIyMzYxMi02ZDIyLTQ5YjctYjFkNy1mODUxNzM5NjIwMTIiLCJuYmYiOjE1MzI2NzQ1NjgsInBlbSI6eyJnZW5lcmFsX2VuZHBvaW50cyI6MSwiaW50ZWdyYXRpb25zIjoxMCwicmF3X2VuZHBvaW50cyI6MzQzNTk3MzgzNjd9LCJzdWIiOiJDbGllbnQ6NTMiLCJ0eXAiOiJhY2Nlc3MifQ.VU83aItpDQB275l3f-utXAvrhq07Uz0dVWKBNQTOwbsAKhYD0XGF5S8-pHeGtPIrr0KfTxMTrKheDYk8jOrLqQ"}

    conn.request("GET", "/api/countries", payload, headers)

    res = conn.getresponse()

    if res.status != 200:
        print(f"Status is: {res.status}: Returning empty response instead of raising exception")
        return '[]'
    data = res.read()

    print(data.decode("utf-8"))

    return data.decode("utf-8")


def get_categories(store='itunes_connect'):
    '''

    :param store: itunes_connect or google_play
    :return: categories data decoded in utf-8 format
    '''

    conn = http.client.HTTPSConnection("integrations.apptopia.com")

    headers = {
        'authorization': "eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJhdWQiOiJpbnRlZ3JhdGlvbnMuYXBwdG9waWEuY29tIiwiZXhwIjoxNTMzODg0MTY5LCJpYXQiOjE1MzI2NzQ1NjksImlzcyI6ImludGVncmF0aW9ucy5hcHB0b3BpYS5jb20iLCJqdGkiOiJiZjIyMzYxMi02ZDIyLTQ5YjctYjFkNy1mODUxNzM5NjIwMTIiLCJuYmYiOjE1MzI2NzQ1NjgsInBlbSI6eyJnZW5lcmFsX2VuZHBvaW50cyI6MSwiaW50ZWdyYXRpb25zIjoxMCwicmF3X2VuZHBvaW50cyI6MzQzNTk3MzgzNjd9LCJzdWIiOiJDbGllbnQ6NTMiLCJ0eXAiOiJhY2Nlc3MifQ.VU83aItpDQB275l3f-utXAvrhq07Uz0dVWKBNQTOwbsAKhYD0XGF5S8-pHeGtPIrr0KfTxMTrKheDYk8jOrLqQ"}

    # conn.request("GET", "/api/" + store + "/app?id%5B%5D=" + appid, payload, headers)

    payload = ""

    conn.request("GET", f"/api/{store}/categories", payload, headers)

    res = conn.getresponse()
    if res.status != 200:
        print(f"Status is:  {res.status} : Returning empty response instead of raising exception")
        return '[]'
    data = res.read()

    # Pre-Process
    dataStr = process_data(data.decode("utf-8"))

    return (dataStr)



def get_new_releases(store='itunes_connect', start_date=None, end_date=None, country='US', category=None):
    '''

    :param store: itunes_connect or google_play
    :param start_date: start date
    :param end_date: end date
    :param country: country
    :param category: category id
    :return: the response data decoded in utf-8 format
    '''

    conn = http.client.HTTPSConnection("integrations.apptopia.com")

    payload = ""

    # headers = {
    #     'authorization': "eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJhdWQiOiJpbnRlZ3JhdGlvbnMuYXBwdG9waWEuY29tIiwiZXhwIjoxNTMzODg0MTY5LCJpYXQiOjE1MzI2NzQ1NjksImlzcyI6ImludGVncmF0aW9ucy5hcHB0b3BpYS5jb20iLCJqdGkiOiJiZjIyMzYxMi02ZDIyLTQ5YjctYjFkNy1mODUxNzM5NjIwMTIiLCJuYmYiOjE1MzI2NzQ1NjgsInBlbSI6eyJnZW5lcmFsX2VuZHBvaW50cyI6MSwiaW50ZWdyYXRpb25zIjoxMCwicmF3X2VuZHBvaW50cyI6MzQzNTk3MzgzNjd9LCJzdWIiOiJDbGllbnQ6NTMiLCJ0eXAiOiJhY2Nlc3MifQ.VU83aItpDQB275l3f-utXAvrhq07Uz0dVWKBNQTOwbsAKhYD0XGF5S8-pHeGtPIrr0KfTxMTrKheDYk8jOrLqQ"}

    payload = ""

    headers = {
        'authorization': "eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJhdWQiOiJpbnRlZ3JhdGlvbnMuYXBwdG9waWEuY29tIiwiZXhwIjoxNTMzODg0MTY5LCJpYXQiOjE1MzI2NzQ1NjksImlzcyI6ImludGVncmF0aW9ucy5hcHB0b3BpYS5jb20iLCJqdGkiOiJiZjIyMzYxMi02ZDIyLTQ5YjctYjFkNy1mODUxNzM5NjIwMTIiLCJuYmYiOjE1MzI2NzQ1NjgsInBlbSI6eyJnZW5lcmFsX2VuZHBvaW50cyI6MSwiaW50ZWdyYXRpb25zIjoxMCwicmF3X2VuZHBvaW50cyI6MzQzNTk3MzgzNjd9LCJzdWIiOiJDbGllbnQ6NTMiLCJ0eXAiOiJhY2Nlc3MifQ.VU83aItpDQB275l3f-utXAvrhq07Uz0dVWKBNQTOwbsAKhYD0XGF5S8-pHeGtPIrr0KfTxMTrKheDYk8jOrLqQ"}

    conn.request("GET", f"/api/{store}/new_releases?date_from={start_date}&date_to={end_date}&country_iso={country}&category_id={category}", payload,
                 headers)

    # conn.request("GET",
    #              f"/api/{store}/new_releases?date_from={start_date}&date_to={end_date}&country_iso={country}",
    #              payload,
    #              headers)

    res = conn.getresponse()
    data = res.read()

    if res.status != 200:
        print(f"Status is:  {res.status} - {store} - {country} - {category} : Returning empty response instead of raising exception")
        return '[]'

    if data.decode('utf-8') == '':
        print('Returning Empty String. Status was OK')
        return '[]'


    # print(data.decode("utf-8"))

    return data.decode("utf-8")


# if __name__ =='__main__':

def NewReleaseIds(start_date=None, end_date=None, apptopiastorefile=None):

    app_st_file = './app_store_new_releases.csv'
    play_st_file = './play_store_new_releases.csv'

    # Getting the dates
    # dates = ['2018-06-29', '2018-07-29']

    # # Other way

    # For a month
    # end_date, start_date = dt.date.today() + dt.timedelta(-1), dt.date.today() + dt.timedelta(-30) + dt.timedelta(-1)

    # For a single day, i.e. the previous day
    # end_date, start_date = dt.date.today() + dt.timedelta(-1), dt.date.today() + dt.timedelta(-2)
    #
    #
    # sm, sd = '{:02d}'.format(start_date.month) , '{:02d}'.format(start_date.day)
    # start_date_str = f'{start_date.year}-{sm}-{sd}'
    #
    # em, ed = '{:02d}'.format(end_date.month), '{:02d}'.format(end_date.day)
    # end_date_str = f'{end_date.year}-{em}-{ed}'
    #
    # dates = [str(start_date_str), str(end_date_str)]

    # Using this as a function. Get the dates from calling function

    dates = [start_date, end_date]


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

    countries = get_countries()
    countries = ast.literal_eval(countries)


    # for itunes, play store
    stores = ['itunes_connect', 'google_play']
    new_release_output_files = [app_st_file, play_st_file]
    category_ids = [appst_category_ids, playst_category_ids]

    # for play store, itunes
    # stores = ['google_play', 'itunes_connect']
    # new_release_output_files = [play_st_file, app_st_file]
    # category_ids = [playst_category_ids, appst_category_ids]


    # Start for loop
    for index, store in enumerate(stores):
        for country in countries: #TODO Remove debug statement.
            for _, catno in category_ids[index].iteritems():
                # if _ != 0 and _ != 1:
                #     break

                #TODO Remove Debug
                # if index == 1:
                #     break

                # pd.DataFrame
                new_release_dict_list = []

                new_release_data = get_new_releases(store=store, start_date=dates[0], end_date=dates[1], country=country, category=catno)
                new_release_data = new_release_data.replace(":true", ':True')
                new_release_data = new_release_data.replace(':false', ':False')

                if (new_release_data := process_data(new_release_data)) == []:
                    continue
                print('new:- ', new_release_data)

                # Iterate through each item in list
                for app in new_release_data:
                    appid = app['id']
                    appname = app['name']
                    category_id = catno
                    country_id = country
                    release_date = app['initial_release_date']
                    metadata = app
                    new_release_dict = {}
                    new_release_dict['appid'] = appid
                    new_release_dict['appname'] = appname
                    new_release_dict['category_id'] = category_id
                    new_release_dict['country'] = country
                    new_release_dict['initial_release_date'] = release_date
                    new_release_dict['checkout_date'] = release_date

                    if False: # save only in csv and not in hdf5
                        new_release_dict['metadata'] = metadata
                    new_release_dict_list.append(new_release_dict)
                new_release_df = pd.DataFrame(new_release_dict_list)

                SaveToFile(filename=new_release_output_files[index], data=new_release_df, type='hdf5', store=store, hdfsstorefile=apptopiastorefile)

                # if os.path.isfile(new_release_output_files[index]):
                #     new_release_df.to_csv(new_release_output_files[index], mode='a', header=False)
                # else:
                #     new_release_df.to_csv(new_release_output_files[index])

                # End of for loop (new_release data)
            # End of for loop (categories)
        # End of for loop (countries)
    # End of for loop (store)


