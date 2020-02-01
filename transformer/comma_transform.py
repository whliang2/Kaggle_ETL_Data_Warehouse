import pandas as pd
import click


@click.command()
@click.option("--name", prompt="CSV Name", help="Type Your CSV Name.")
@click.option("--dir", default="../data", help="The specify the storage directory.")
def CommaTransform (name, dir):
    df = pd.read_csv(f"{dir}/unprocessed/{name}")
    Col_list = df.columns
    for col in Col_list:
        df[col] = df[col].astype(str)
        df[col] = df[col].replace({',': '-'}, regex=True)
        
    df.to_csv(f"{dir}/{name}", index=None)


if __name__ == '__main__':
    CommaTransform()





