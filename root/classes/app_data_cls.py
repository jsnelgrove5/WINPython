from root.share import _datasets_base_dir
from root.share import _covid_base_dir
from collections import defaultdict
from vengeance import to_datetime
import pandas as pd


class AppData:
    def __init__(self, data_type='df', *args, **kwargs):
        # fetch data
        self.df_cases = pd.read_csv(_covid_base_dir + "time_series_covid19_confirmed_US.csv")
        self.df_deaths = pd.read_csv(_covid_base_dir + "time_series_covid19_deaths_US.csv")
        self.df_schools = pd.read_csv(_datasets_base_dir + 'Public_School_Locations_-_Current.csv')

        # normalize and validate data
        self.df_cases_nc, self.df_deaths_nc, self.df_schools_nc = None, None, None
        self.normalize_and_validate()

        # merge data
        master_df = self.merge_data()

        self.data = None
        if data_type == 'df':
            self.data = master_df
        elif data_type == 'm':
            self.data = [master_df.columns.values.tolist()] + master_df.values.tolist()

    def merge_data(self):
        # iterate with cases and append deaths and we will also append a list of number of schools and population
        schools_d = {}
        for i, row in self.df_schools_nc.iterrows():
            k = row['county']
            if k in schools_d:
                schools_d[k] += 1
            else:
                schools_d[k] = 1

        for i, row in self.df_cases_nc.iterrows():
            try:
                self.df_cases_nc.at[i, 'school_count'] = schools_d[row['county']]
            except KeyError:
                print(row['county'], ' has no schools?')
                self.df_cases_nc.at[i, 'school_count'] = 0

        # make df_deaths into a dictionary to be more easily usable
        deaths_d = defaultdict(dict)
        for i, row in self.df_deaths_nc.iterrows():
            k1 = row['county']
            for column in self.df_deaths_nc:
                deaths_d[k1][column] = row[column]

        for i, row in self.df_cases_nc.iterrows():
            county = row['county']
            try:
                match_d = deaths_d[county]
                population = match_d['Population']
                self.df_cases_nc.at[i, 'population'] = population

                for column in self.df_cases_nc:
                    if not to_datetime(column):
                        continue

                    self.df_cases_nc.at[i, column + '_deaths'] = match_d[column]

            except KeyError:
                print(KeyError)
                continue

        return self.df_cases_nc

    def normalize_and_validate(self):
        # parse the data down to state specific
        self.df_cases_nc = self.df_cases[self.df_cases['Province_State'] == 'North Carolina']
        self.df_deaths_nc = self.df_deaths[self.df_deaths['Province_State'] == 'North Carolina']
        self.df_schools_nc = self.df_schools[self.df_schools['STATE'] == 'NC']

        # normalize column headers
        self.df_cases_nc.rename(columns={'Admin2': 'county', 'Lat': 'latitude', 'Long_': 'longitude'}, inplace=True)
        self.df_deaths_nc.rename(columns={'Admin2': 'county', 'Lat': 'latitude', 'Long_': 'longitude'}, inplace=True)
        self.df_schools_nc.rename(columns={'NMCNTY': 'county', 'LAT': 'latitude', 'LON': 'longitude'}, inplace=True)

        # format county names and delete values if longitude and latitude are 0.0
        self.df_cases_nc = self.df_cases_nc[self.df_cases_nc['latitude'] != 0.0]
        for i, row in self.df_cases_nc.iterrows():
            self.df_cases_nc.at[i, 'county'] = row['county'].lower().strip()

        self.df_deaths_nc = self.df_deaths_nc[self.df_deaths_nc['latitude'] != 0.0]
        for i, row in self.df_deaths_nc.iterrows():
            self.df_deaths_nc.at[i, 'county'] = row['county'].lower().strip()

        self.df_schools_nc = self.df_schools_nc[self.df_schools_nc['latitude'] != 0.0]
        for i, row in self.df_schools_nc.iterrows():
            self.df_schools_nc.at[i, 'county'] = row['county'].lower().replace('county', '').strip()

