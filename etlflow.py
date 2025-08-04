from prefect import flow,task
import polars as pl
#extract(read data)
@task
def extract_data(file_path: str):
    df=pl.read_csv(file_path)
    print("CSV data: \n",df)
    return df
#trasform(count rows)
@task
def transform_data(df: pl.DataFrame):
    row_count=df.shape[0]
    return row_count
#load(print the result)
@task
def load(count: int):
    print(f"Total rows in CSV: {count}")
#main flow (conect everything)
@flow(name="etl-example-flow")
def etl_flow():
    file_path="data.csv"
    data= extract_data(file_path)
    row_count=transform_data(data)
    load(row_count)
#run the flow
if __name__=="__main__":
    etl_flow()