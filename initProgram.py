from Service.DataCleaner import DataCleaner
import json
from Utils.SparkSessionManager import SparkSessionManager

class InitProgram:  
    def __main__(self):
        spark = SparkSessionManager.create_session()
        dt_cleaner = DataCleaner(spark)
        with open('settings.json', 'r') as file:
            data = json.load(file)

        local_settings = data['Local']
        filepath = local_settings['filepath']
        filename = local_settings['filename']
        output = local_settings['output']

        dt_cleaner.clean_data_from_source(filepath, filename, output)

if __name__ == "__main__":
    main_instance = InitProgram()
    main_instance.__main__()