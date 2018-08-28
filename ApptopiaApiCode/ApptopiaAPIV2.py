# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# "Project Name"        :   "Code"                                      #
# "File Name"           :   "ApptopiaAPIV2"                             #
# "Author"              :   "rishabhzn200"                              #
# "Date of Creation"    :   "Jul-30-2018"                               #
# "Time of Creation"    :   "8:40 PM"                                   #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #


import http.client

# import abstract syntax trees
import ast
from requests.utils import quote

# Token to fetch the data from the API's

token = "eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJhdWQiOiJpbnRlZ3JhdGlvbnMuYXBwdG9waWEuY29tIiwiZXhwIjoxNTM1NDU5MTI3LCJpYXQiOjE1MzQyNDk1MjcsImlzcyI6ImludGVncmF0aW9ucy5hcHB0b3BpYS5jb20iLCJqdGkiOiIzNTdmNTdiNC1mMWJhLTRhZGUtYTA2MS0zY2Q3ZDBiYzkxMjMiLCJuYmYiOjE1MzQyNDk1MjYsInBlbSI6eyJnZW5lcmFsX2VuZHBvaW50cyI6MSwiaW50ZWdyYXRpb25zIjoxMCwicmF3X2VuZHBvaW50cyI6MzQzNTk3MzgzNjd9LCJzdWIiOiJDbGllbnQ6NTMiLCJ0eXAiOiJhY2Nlc3MifQ.AQYu7rMCPvVVgZVcjd-y9_cyGUy8yC47fUXSbZeFej222KL2rg_Z3nA7xqFJ2yUi7nfO2bDCGk3bq-aHP4mTiw"

# TODO missing 1 day at last. So add the day to last day. Completed..!!
def get_date_range(from_='', to_=''):
    '''

    :param from_: start date
    :param to_: end date
    :return: list of dates between the start date and the end date
    '''
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

    # print(f"RishabR: {dataStr}")

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
        'authorization': token}

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
    '''

    :param appid: application id. integer for app store and string for play store
    :param store: can be 'itunes_connect' or 'play_store'
    :return: the metadata extracted from the API
    '''

    conn = http.client.HTTPSConnection("integrations.apptopia.com")

    payload = ""

    headers = {
        'authorization': token}

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
    '''

    :param appid: application id. integer for app store and string for play store
    :param store: can be 'itunes_connect' or 'play_store'
    :param country: two letter country code.
    :return: the version history extracted from the API
    '''

    conn = http.client.HTTPSConnection("integrations.apptopia.com")

    payload = ""

    headers = {
        'authorization': token}

    conn.request("GET", f"/api/{store}/app_versions?id%5B%5D={appid}&country_iso={country}", payload, headers)

    res = conn.getresponse()

    if res.status != 200:
        return [f"'status':{res.status}"], res.status
    data = res.read()

    dataStr = data.decode('utf-8') #.replace(":true", ':True')
    # dataStr = dataStr.replace(':false', ':False')
    processed_data = process_data(dataStr, **{':null':':None', ":true":':True', ":false":':False'})

    return processed_data, res.status


def get_estimates(appid, store='itunes_connect', country='US', start_date='2017-04-01', end_date='2018-08-15'):
    '''

    :param appid: application id. integer for app store and string for play store
    :param store: can be 'itunes_connect' or 'play_store'
    :param country: two letter country code
    :param start_date: performance start date
    :param end_date: performance end date
    :return: the performance data from start date to end date extracted from the API
    '''
    conn = http.client.HTTPSConnection("integrations.apptopia.com")
    #
    # payload = ""
    #
    headers = {
        'authorization': token}

    payload = ""


    processed_data = []
    if start_date == end_date:
        dates = [start_date]
    else:
        dates = get_date_range(from_=start_date, to_=end_date)

    # API restrict the number of days which can be passsed at a time to 180. So breaking the number of days to chunk of 180 days
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

        dataStr = data.decode('utf-8')
        # dataStr = dataStr.replace(':false', ':False')
        # print(f"Rishabh: {dataStr}")
        try:
            processed_data.extend(process_data(dataStr, **{':null':':None', ":true":":True", ':false':':False'}))
        except:
            with open('./LiteralEvalError.txt', 'a') as f:
                f.write('\n\n_________________ERROR while using literal eval _________________________\n')
                f.write(f'Status={res.status}. Not adding this data to main file. Load manually later.')
                f.write(f'\nID = {appid}\tcountry = {country}\tst_date={s}\tend_date={e}\n')
                f.write(f'Data={dataStr}')

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

    # print(dataStr)

    try:
        dataStr = process_data(dataStr, **{':null': ':None', ":true": ':True', ":false": ':False'})
        processed_data.extend(dataStr)
    except:
        with open('./LiteralEvalError.txt', 'a') as f:
            f.write('\n\n_________________ERROR while using literal eval _________________________\n')
            f.write(f'Status={res.status}. Not adding this data to main file. Load manually later.')
            f.write(f'\nID = {appid}\tcountry = {country}\tst_date={s}\tend_date={e}\n')
            f.write(f'Data={dataStr}')

    return processed_data, res.status



def get_app_sdks(appid, store='itunes_connect'):
    '''

    :param appid: application id. integer for app store and string for play store
    :param store: can be 'itunes_connect' or 'play_store'
    :return: the sdk data extracted from the API
    '''
    conn = http.client.HTTPSConnection("integrations.apptopia.com")

    payload = ""

    headers = {
        'authorization': token}

    conn.request("GET", f"/api/{store}/app_sdks?id%5B%5D={appid}", payload, headers)

    res = conn.getresponse()

    if res.status != 200:
        print("\n***Error Code not 200***\n")
        raise Exception
    data = res.read()

    dataStr = data.decode('utf-8') #.replace(":true", ':True')
    # dataStr = dataStr.replace(':false', ':False')
    processed_data = process_data(dataStr, **{':null': ':None', ":true": ':True', ":false": ':False'})

    # processed_data.extend(process_data(dataStr)

    return processed_data


def app_discovery(store=None, next_page_token=None):
    '''

    :param store: can be 'itunes_connect' or 'play_store'
    :param next_page_token: token to get the next page
    :return:
    '''
    conn = http.client.HTTPSConnection("integrations.apptopia.com")

    payload = ""

    headers = {
        'authorization': token}

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

    dataStr = data.decode('utf-8')

    processed_data = process_data(dataStr, **{':null': ':None', ":true": ':True', ":false": ':False'})

    return processed_data
