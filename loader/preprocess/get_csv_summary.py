import pandas as pd


class Csv_summerizer():
    """
        Get csv information
    """
    def __init__(self, csv_dir):
        self.dir = csv_dir
        self.csv_df = pd.read_csv(self.dir)

    def get_header(self):
        print(self.csv_df.columns.values.tolist())
        return self.csv_df.columns.values.tolist()

    def get_columns_datatype(self):
        columnsDataTypeDict = dict(self.csv_df.dtypes)

        for column, column_dtype in columnsDataTypeDict.items():
            columnsDataTypeDict[column] = column_dtype.name

        # print(columnsDataTypeDict)
        return columnsDataTypeDict