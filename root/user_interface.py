from root.share import _datasets_base_dir
from root.share import _covid_base_dir
from collections import defaultdict
from vengeance import to_datetime
import pandas as pd


def main():
    """This is the main function for building a user interface"""

    # fetch data
    df_cases = pd.read_csv(_covid_base_dir + "time_series_covid19_confirmed_US.csv")
    df_deaths = pd.read_csv(_covid_base_dir + "time_series_covid19_deaths_US.csv")
    df_schools = pd.read_csv(_datasets_base_dir + 'Public_School_Locations_-_Current.csv')

    # normalize and validate data
    df_cases_nc, df_deaths_nc, df_schools_nc = normalize_and_validate(df_cases, df_deaths, df_schools)

    # merge data
    master_df = merge_data(df_cases_nc, df_deaths_nc, df_schools_nc)

    # analyze and graph data

    pass


def merge_data(df_cases_nc, df_deaths_nc, df_schools_nc):
    # iterate with cases and append deaths and we will also append a list of number of schools and population
    schools_d = {}
    for i, row in df_schools_nc.iterrows():
        k = row['county']
        if k in schools_d:
            schools_d[k] += 1
        else:
            schools_d[k] = 1

    for i, row in df_cases_nc.iterrows():
        try:
            df_cases_nc.at[i, 'school_count'] = schools_d[row['county']]
        except KeyError:
            print(row['county'], ' has no schools?')
            df_cases_nc.at[i, 'school_count'] = 0

    # make df_deaths into a dictionary to be more easily usable
    deaths_d = defaultdict(dict)
    for i, row in df_deaths_nc.iterrows():
        k1 = row['county']
        for column in df_deaths_nc:
            deaths_d[k1][column] = row[column]

    for i, row in df_cases_nc.iterrows():
        county = row['county']
        try:
            match_d = deaths_d[county]
            population = match_d['Population']
            df_cases_nc.at[i, 'population'] = population

            for column in df_cases_nc:
                if not to_datetime(column):
                    continue

                df_cases_nc.at[i, column + '_deaths'] = match_d[column]

        except KeyError:
            print(KeyError)
            continue

    return df_cases_nc


def normalize_and_validate(df_cases, df_deaths, df_schools):
    # parse the data down to state specific
    df_cases_nc = df_cases[df_cases['Province_State'] == 'North Carolina']
    df_deaths_nc = df_deaths[df_deaths['Province_State'] == 'North Carolina']
    df_schools_nc = df_schools[df_schools['STATE'] == 'NC']

    # normalize column headers
    df_cases_nc.rename(columns={'Admin2': 'county', 'Lat': 'latitude', 'Long_': 'longitude'}, inplace=True)
    df_deaths_nc.rename(columns={'Admin2': 'county', 'Lat': 'latitude', 'Long_': 'longitude'}, inplace=True)
    df_schools_nc.rename(columns={'NMCNTY': 'county', 'LAT': 'latitude', 'LON': 'longitude'}, inplace=True)

    # format county names and delete values if longitude and latitude are 0.0
    df_cases_nc = df_cases_nc[df_cases_nc['latitude'] != 0.0]
    for i, row in df_cases_nc.iterrows():
        df_cases_nc.at[i, 'county'] = row['county'].lower().strip()

    df_deaths_nc = df_deaths_nc[df_deaths_nc['latitude'] != 0.0]
    for i, row in df_deaths_nc.iterrows():
        df_deaths_nc.at[i, 'county'] = row['county'].lower().strip()

    df_schools_nc = df_schools_nc[df_schools_nc['latitude'] != 0.0]
    for i, row in df_schools_nc.iterrows():
        df_schools_nc.at[i, 'county'] = row['county'].lower().replace('county', '').strip()

    return df_cases_nc, df_deaths_nc, df_schools_nc


if __name__ == '__main__':
    main()


