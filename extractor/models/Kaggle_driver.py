import kaggle

class Kaggle_Api:
    def __init__(self):
        kaggle.api.authenticate()

    def download_dataset(self, datasetName: str, dir: str):
        """ Download Kaggle dataset to @dir by @name"""
        kaggle.api.dataset_download_files(datasetName, path=dir, unzip=True)
