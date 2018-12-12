# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# "Project Name"        :   "Code"                                      #
# "File Name"           :   "ApptopiaAPIV2"                             #
# "Author"              :   "rishabhzn200"                              #
# "Date of Creation"    :   "Jul-30-2018"                               #
# "Time of Creation"    :   "8:40 PM"                                   #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #


import http.client
import ast
# from django.utils.http import urlquote
from requests.utils import quote

# TODO missing 1 day at last. So add the day to last day
def get_date_range(from_='', to_=''):
    import datetime

    start = datetime.datetime.strptime(from_, "%Y-%m-%d")
    end = datetime.datetime.strptime(to_, "%Y-%m-%d") + datetime.timedelta(1)
    date_generated = [start + datetime.timedelta(days=x) for x in range(0, (end - start).days)]

    dates = []
    for date in date_generated:
        dates.append(date.strftime("%Y-%m-%d"))

    return dates


def process_data(data, **kwargs):
    '''

    :param data: data in string format
    :return: data after using literal_eval
    '''

    dataStr = data

    # Pre-Process
    for key, value in kwargs.items():
        dataStr = dataStr.replace(key, value)

    # Evaluate
    dataStr = ast.literal_eval(dataStr)  # It is a list

    return (dataStr)


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

    dataStr = data.decode('utf-8')

    processed_data = process_data(dataStr, **{':null': ':None', ":true": ':True', ":false": ':False'})

    return processed_data


def get_app_metadata(appid, store='itunes_connect'):

    conn = http.client.HTTPSConnection("integrations.apptopia.com")

    payload = ""

    headers = {
        'authorization': "eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJhdWQiOiJpbnRlZ3JhdGlvbnMuYXBwdG9waWEuY29tIiwiZXhwIjoxNTMzODg0MTY5LCJpYXQiOjE1MzI2NzQ1NjksImlzcyI6ImludGVncmF0aW9ucy5hcHB0b3BpYS5jb20iLCJqdGkiOiJiZjIyMzYxMi02ZDIyLTQ5YjctYjFkNy1mODUxNzM5NjIwMTIiLCJuYmYiOjE1MzI2NzQ1NjgsInBlbSI6eyJnZW5lcmFsX2VuZHBvaW50cyI6MSwiaW50ZWdyYXRpb25zIjoxMCwicmF3X2VuZHBvaW50cyI6MzQzNTk3MzgzNjd9LCJzdWIiOiJDbGllbnQ6NTMiLCJ0eXAiOiJhY2Nlc3MifQ.VU83aItpDQB275l3f-utXAvrhq07Uz0dVWKBNQTOwbsAKhYD0XGF5S8-pHeGtPIrr0KfTxMTrKheDYk8jOrLqQ"}

    conn.request("GET", f"/api/{store}/app?id%5B%5D={appid}", payload, headers )

    res = conn.getresponse()
    data = res.read()

    # print(data.decode("utf-8"))

    dataStr = data.decode('utf-8')
    # dataStr = dataStr.replace(":true", ':True')
    # dataStr = dataStr.replace(':false', ':False')
    processed_data = process_data(dataStr, **{':null':':None', ":true":':True', ":false":':False'})

    return processed_data


def get_app_version_history(appid, store='itunes_connect', country='US'):

    conn = http.client.HTTPSConnection("integrations.apptopia.com")

    payload = ""

    headers = {
        'authorization': "eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJhdWQiOiJpbnRlZ3JhdGlvbnMuYXBwdG9waWEuY29tIiwiZXhwIjoxNTMzODg0MTY5LCJpYXQiOjE1MzI2NzQ1NjksImlzcyI6ImludGVncmF0aW9ucy5hcHB0b3BpYS5jb20iLCJqdGkiOiJiZjIyMzYxMi02ZDIyLTQ5YjctYjFkNy1mODUxNzM5NjIwMTIiLCJuYmYiOjE1MzI2NzQ1NjgsInBlbSI6eyJnZW5lcmFsX2VuZHBvaW50cyI6MSwiaW50ZWdyYXRpb25zIjoxMCwicmF3X2VuZHBvaW50cyI6MzQzNTk3MzgzNjd9LCJzdWIiOiJDbGllbnQ6NTMiLCJ0eXAiOiJhY2Nlc3MifQ.VU83aItpDQB275l3f-utXAvrhq07Uz0dVWKBNQTOwbsAKhYD0XGF5S8-pHeGtPIrr0KfTxMTrKheDYk8jOrLqQ"}

    conn.request("GET", f"/api/{store}/app_versions?id%5B%5D={appid}&country_iso={country}", payload, headers)

    res = conn.getresponse()
    data = res.read()

    dataStr = data.decode('utf-8') #.replace(":true", ':True')
    # dataStr = dataStr.replace(':false', ':False')
    processed_data = process_data(dataStr, **{':null':':None', ":true":':True', ":false":':False'})

    return processed_data


def get_estimates(appid, store='itunes_connect', country='US', start_date='2017-01-01', end_date='2018-01-20'):
    conn = http.client.HTTPSConnection("integrations.apptopia.com")
    #
    # payload = ""
    #
    headers = {
        'authorization': "eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJhdWQiOiJpbnRlZ3JhdGlvbnMuYXBwdG9waWEuY29tIiwiZXhwIjoxNTMzODg0MTY5LCJpYXQiOjE1MzI2NzQ1NjksImlzcyI6ImludGVncmF0aW9ucy5hcHB0b3BpYS5jb20iLCJqdGkiOiJiZjIyMzYxMi02ZDIyLTQ5YjctYjFkNy1mODUxNzM5NjIwMTIiLCJuYmYiOjE1MzI2NzQ1NjgsInBlbSI6eyJnZW5lcmFsX2VuZHBvaW50cyI6MSwiaW50ZWdyYXRpb25zIjoxMCwicmF3X2VuZHBvaW50cyI6MzQzNTk3MzgzNjd9LCJzdWIiOiJDbGllbnQ6NTMiLCJ0eXAiOiJhY2Nlc3MifQ.VU83aItpDQB275l3f-utXAvrhq07Uz0dVWKBNQTOwbsAKhYD0XGF5S8-pHeGtPIrr0KfTxMTrKheDYk8jOrLqQ"}

    payload = ""

    processed_data = []

    if start_date == end_date:
        dates = [start_date]
    else:
        #TODO change the end_date by 1
        dates = get_date_range(from_=start_date, to_=end_date)

    s = dates[0]
    e = None
    for index in range(0, len(dates), 180)[1:]:

        e = dates[index-1]
        d = get_date_range(from_=s, to_=e)

        # Find estimate
        conn.request("GET",
                     f"/api/{store}/estimates?id%5B%5D={appid}&country_iso={country}&date_from={s}&date_to={e}",
                     payload, headers)

        res = conn.getresponse()
        data = res.read()

        dataStr = data.decode('utf-8').replace(":true", ':True')
        dataStr = dataStr.replace(':false', ':False')
        processed_data.extend(process_data(dataStr))

        s = dates[index]

    if len(dates) % 179 != 0:
        e = dates[-1]



    d = get_date_range(from_=s, to_=e)

    # Find estimate
    conn.request("GET",
                 f"/api/{store}/estimates?id%5B%5D={appid}&country_iso={country}&date_from={s}&date_to={e}",
                 payload, headers)

    res = conn.getresponse()
    data = res.read()

    dataStr = data.decode('utf-8') #.replace(":true", ':True')
    # dataStr = dataStr.replace(':false', ':False')
    # processed_data = process_data(dataStr)
    dataStr = process_data(dataStr, **{':null': ':None', ":true": ':True', ":false": ':False'})

    processed_data.extend(dataStr)


    return processed_data



def get_app_sdks(appid, store='itunes_connect'):
    conn = http.client.HTTPSConnection("integrations.apptopia.com")

    payload = ""

    headers = {
        'authorization': "eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJhdWQiOiJpbnRlZ3JhdGlvbnMuYXBwdG9waWEuY29tIiwiZXhwIjoxNTMzODg0MTY5LCJpYXQiOjE1MzI2NzQ1NjksImlzcyI6ImludGVncmF0aW9ucy5hcHB0b3BpYS5jb20iLCJqdGkiOiJiZjIyMzYxMi02ZDIyLTQ5YjctYjFkNy1mODUxNzM5NjIwMTIiLCJuYmYiOjE1MzI2NzQ1NjgsInBlbSI6eyJnZW5lcmFsX2VuZHBvaW50cyI6MSwiaW50ZWdyYXRpb25zIjoxMCwicmF3X2VuZHBvaW50cyI6MzQzNTk3MzgzNjd9LCJzdWIiOiJDbGllbnQ6NTMiLCJ0eXAiOiJhY2Nlc3MifQ.VU83aItpDQB275l3f-utXAvrhq07Uz0dVWKBNQTOwbsAKhYD0XGF5S8-pHeGtPIrr0KfTxMTrKheDYk8jOrLqQ"}

    conn.request("GET", f"/api/{store}/app_sdks?id%5B%5D={appid}", payload, headers)

    res = conn.getresponse()

    if res.status != 200:
        raise Exception
    data = res.read()

    dataStr = data.decode('utf-8') #.replace(":true", ':True')
    # dataStr = dataStr.replace(':false', ':False')
    processed_data = process_data(dataStr, **{':null': ':None', ":true": ':True', ":false": ':False'})

    # processed_data.extend(process_data(dataStr)

    return processed_data


def app_discovery(store=None, next_page_token=None):
    conn = http.client.HTTPSConnection("integrations.apptopia.com")

    payload = ""

    headers = {
        'authorization': "eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJhdWQiOiJpbnRlZ3JhdGlvbnMuYXBwdG9waWEuY29tIiwiZXhwIjoxNTMzODg0MTY5LCJpYXQiOjE1MzI2NzQ1NjksImlzcyI6ImludGVncmF0aW9ucy5hcHB0b3BpYS5jb20iLCJqdGkiOiJiZjIyMzYxMi02ZDIyLTQ5YjctYjFkNy1mODUxNzM5NjIwMTIiLCJuYmYiOjE1MzI2NzQ1NjgsInBlbSI6eyJnZW5lcmFsX2VuZHBvaW50cyI6MSwiaW50ZWdyYXRpb25zIjoxMCwicmF3X2VuZHBvaW50cyI6MzQzNTk3MzgzNjd9LCJzdWIiOiJDbGllbnQ6NTMiLCJ0eXAiOiJhY2Nlc3MifQ.VU83aItpDQB275l3f-utXAvrhq07Uz0dVWKBNQTOwbsAKhYD0XGF5S8-pHeGtPIrr0KfTxMTrKheDYk8jOrLqQ"}

    url = f"/api/{store}/app/discovery?partition=1&total_partitions=10"
    if next_page_token is not None:
        # url1 = f'https://integrations.apptopia.com{url}&page_token={urlquote(next_page_token)}'
        url = f'https://integrations.apptopia.com{url}&page_token={quote(next_page_token)}'


    #debug
    # url='https://integrations.apptopia.com/api/itunes_connect/app/discovery?partition=1&total_partitions=10&page_token=6XcQXZDH7eFL%2Fcm%2BICWQb5Synq%2FjAsr%2BBQaFeV%2BOzrHtNGijmxGfd7J8Uc8tqtzDuYa01tGQyxT8iuO1oaEYA%2BY4Xm6kLh0c8Z%2Fu3IUpmIBPo0AMyf2VAOx3QpMQ6tdBU1Urh9MyVS7auIayr%2B9py46U%2FefKvY4bpTRJxlCyov3Rr7ZE0tqZkIjUjE3pN%2B76lZCSVSXsjKzvF6D%2BXGa%2F3Q%3D%3D'

    conn.request("GET", url, payload, headers)

    res = conn.getresponse()

    if res.status != 200:
        print(f'\n\nStatus = {res.status}\nUrl= {url}\nToken={next_page_token}\n. Skipping this batch data to avoid error\n\n')
        import time
        time.sleep(1)
        return '[]'
        # raise Exception
    data = res.read()

    dataStr = data.decode('utf-8')  # .replace(":true", ':True')
    # dataStr = dataStr.replace(':false', ':False')
    processed_data = process_data(dataStr, **{':null': ':None', ":true": ':True', ":false": ':False'})

    # processed_data.extend(process_data(dataStr)

    return processed_data


# import datetime as dt
# current_date = dt.date.today() + dt.timedelta(-1)
# sm, sd = '{:02d}'.format(current_date.month), '{:02d}'.format(current_date.day)
# curr_date_str = f'{current_date.year}-{sm}-{sd}'


# estimate = get_estimates('com.facebook.katana', store='google_play', country='US', start_date='2017-01-01', end_date=curr_date_str)