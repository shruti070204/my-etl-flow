from prefect import flow, task
import polars as pl

@task
def read_count_rows(file_path: str):
    df=pl.read_csv(file_path)
    print(f"\n---Data from {file_path}---")
    print(df)
    return df.shape[0]

@flow(name="process-multiple-csv")
def process_multiple_csv():
    csv_files=["data1.csv","data2.csv","data3.csv"]
    results=[]
    #submit all tasks in parallel
    for file in csv_files:
        future=read_count_rows.submit(file)
        results.append(future)
    #wait for all task and get results
    total_rows=0
    for r in results:
        row_count=r.result()
        print(f"{row_count} rows")
        total_rows+=row_count

    print(f"\n Total rows from all files: {total_rows}")

if __name__=="__main__":
    process_multiple_csv()