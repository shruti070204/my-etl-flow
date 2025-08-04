from prefect import flow, task
import polars as pl

@task
def read(file: str)->pl.DataFrame:
    print(f"\nReading: {file}")
    return pl.read_csv(file)

@task
def save(df: pl.DataFrame,output: str):
    df.write_csv(output)
    print(f"\nsaved to {output}")

@flow(name="etl-flow")
def etlpipeline(file: str, output: str):
    df=read(file)
    save(df,output)

if __name__=="__main__":
    etlpipeline("data1.csv","result.csv")