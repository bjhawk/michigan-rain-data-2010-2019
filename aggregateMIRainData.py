import pandas as pd

# a container for all data
agg_data = pd.DataFrame()

# Iterate through all files (2010-2019)
for year in range(0,10):
    year += 2010
    year_data = pd.read_csv(
        '<PATH_TO_REPO>/Data/MI_PRCP_{}.csv'.format(year),
        usecols = ['date', 'value'],
        parse_dates = ['date']
    )

    agg_year_data = year_data.groupby('date').agg({'value': 'mean'})

    # Add this data to container
    agg_data = agg_data.append(agg_year_data)

agg_data.to_csv(
    '<PATH_TO_REPO>/Data/MI_PRCP_2010-2019.csv'.format(year),
    index = True,
    line_terminator = '\n',
    date_format='%Y-%m-%d'
)