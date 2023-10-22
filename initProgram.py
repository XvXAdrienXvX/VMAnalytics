from Service.DataCleaner import DataCleaner
from Service.DataReader import DataReader
from Service.VMAnalyzer import VMAnalyzer
import json
from Utils.SparkSessionManager import SparkSessionManager

class InitProgram:  
    def __main__(self):
        spark = SparkSessionManager.create_session()
        dt_reader = DataReader(spark)
        dt_cleaner = DataCleaner(spark)
        with open('settings.json', 'r') as file:
            data = json.load(file)

        local_settings = data['Local']
        filepath = local_settings['filepath']
        filename = local_settings['filename']
        output = local_settings['output']

        dt_cleaner.clean_data_from_source(filepath, filename, output)
        #csv_file, vmSchema = dt_reader.read_data(filepath, filename, output)
        #dt_analyzer = VMAnalyzer(spark, csv_file, vmSchema)
       # dt_analyzer.pre_processing_pipeline()

if __name__ == "__main__":
    main_instance = InitProgram()
    main_instance.__main__()