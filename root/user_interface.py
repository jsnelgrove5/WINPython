from root.classes.app_data_cls import AppData


def main():
    """This is the main function for building a user interface"""

    app_data = AppData(data_type='df')

    data = app_data.data

    t_1_cases = 0
    t_2_cases = 0
    for i, row in data.iterrows():
        print(row['Combined_Key'], row['5/11/20'] - row['5/10/20'])
        t_1_cases += row['5/11/20']
        t_2_cases += row['5/10/20']
    print(t_1_cases - t_2_cases)


if __name__ == '__main__':
    main()


