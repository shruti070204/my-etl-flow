from prefect import flow, task
import polars as pl
#load data into memory
@task
def read_csv(file_path: str):
    df=pl.read_csv(file_path)
    print("\n Original data: ")
    print(df)
    return df
#apply conditions to select only the rows you want
@task
def filter(df: pl.DataFrame):
    filtered=df.filter(df["age"]>30)
    print("\nFiltered data(age>30): ")
    print(filtered)
    return filtered
#store the clean data for future use
@task
def save(df: pl.DataFrame,output:str):
    df.write_csv(output)
    print(f"\nSaved filtered data to : {output}")

@flow(name="csv_pipeline")
def csv_pipeline():
    input="people.csv"
    output="filter.csv"
    df=read_csv(input)
    filtered=filter(df)
    save(filtered,output)

if __name__=="__main__":
    csv_pipeline()