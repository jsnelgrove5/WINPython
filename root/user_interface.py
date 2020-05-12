from root.classes.app_data_cls import AppData


def main():
    """This is the main function for building a user interface"""

    app_data = AppData(data_type='df')

    data = app_data.data

    print(data)


if __name__ == '__main__':
    main()


