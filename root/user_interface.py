from root.share import _base_dir
import pandas as pd


def main():
    df_cases = pd.read_csv(_base_dir + "time_series_covid19_confirmed_US.csv")
    print(df_cases.tail())

    df_deaths = pd.read_csv(_base_dir + "time_series_covid19_deaths_US.csv")
    df_deaths_nc = df_deaths[df_deaths['Province_State'] == 'North Carolina']
    print(df_deaths.tail())


if __name__ == '__main__':
    main()