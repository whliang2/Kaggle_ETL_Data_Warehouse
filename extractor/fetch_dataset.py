import click
import kaggle
from models.Kaggle_driver import Kaggle_Api

@click.command()
@click.option("--name", prompt="Kaggle Dataset Name", help="Kaggle dataset name.")
@click.option("--dir", default="../data/unprocessed", help="The specify the storage directory.")
def fetch_dataset(name, dir):
    """Fetch kaggle data to specific directory"""
    try:
        api = Kaggle_Api()
        api.download_dataset(name, dir)
    except:
        print("An Error occurred! Please check dataset name and exist of the directory that you want to store")
   

if __name__ == '__main__':
    fetch_dataset()