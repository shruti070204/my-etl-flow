from prefect import task, flow
import polars as pl
#read csv using polars
@task
def read(file_path: str)->pl.DataFrame:
    print(f"Reading file: {file_path}")
    df=pl.read_csv(file_path)
    print("\nData preview:")
    print(df)
    return df
#count number of rows
@task
def count(df: pl.DataFrame):
    row=df.shape[0] #shape return(rows,cols)
    print(f"\nNumber of rows: {row}")
#takes file path as a parameter
@flow(name="polars-csv-flow")
def process_csv(file: str):
    df= read(file)
    count(df)
#run the flow with tthe csv
if __name__=="__main__":
    process_csv("people.csv")