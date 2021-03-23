import pandas as pd
from requests import get
import json

# wrap data request in function for easier iteration
def make_request(startdate, enddate, limit, offset):
    request_params = {
        'startdate': startdate,
        'enddate': enddate,
        'limit': limit,
        'offset': offset,
        'datasetid': 'GHCND',
        'locationid': 'FIPS:26',
        'units': 'standard',
        'includemetadata': False,
        'datatypeid': 'PRCP' # todo, does excluding snow here fuck us up?
    }

    try:
        response = get(
            'https://www.ncdc.noaa.gov/cdo-web/api/v2/data',
            params = request_params,
            headers = {'token': '<INSERT TOKEN HERE>'}
        )

        if response.json() == {}:
            return False

        return response.json()['results']
    except:
        print(response)

# Set this in one place for use throughout iteration
request_chunk_size = 1000

# Iterate through 10 years of data (2010-2019)
for year in range(0,10):
    year += 2010

    # make a container to aggregate data retrieved
    year_data = []

    # for each year, iterate through every month span
    for month in range(0, 12):
        month = '{:02d}'.format(month+1)
        startdate = '{}-{}-01'.format(year, month)
        enddate = pd.to_datetime(startdate) + pd.offsets.MonthEnd()
        enddate = enddate.strftime('%Y-%m-%d')

        print('Retrieving data for range {} through {}'.format(startdate, enddate))

        # initialize iteration for chunks of data
        offset = 0

        # get the first chunk
        data = make_request(startdate = startdate, enddate = enddate,
                            limit = request_chunk_size, offset = offset)
        while (data != False):
            # add this chunk to container
            year_data += data

            # increment offset
            offset += request_chunk_size

            # get a new chunk of data
            data = make_request(startdate = startdate, enddate = enddate,
                            limit = request_chunk_size, offset = offset)

    # Convert to a dataframe
    df = pd.DataFrame(year_data)

    # split the attributes column into separate columns
    attrs = df['attributes'].str.split(',', expand = True)
    df['measurement'] = attrs[0]
    df['quality'] = attrs[1]
    df['source'] = attrs[2]
    df['time_of_obs'] = attrs[3]

    # drop original attributes column
    df.drop(['attributes'], axis = 'columns', inplace = True)

    # finally write it out to file
    df.to_csv(
        '<PATH_TO_REPO>/Data/MI_PRCP_{}.csv'.format(year),
        index = False,
        line_terminator = '\n',
        date_format='%Y-%m-%d'
    )
