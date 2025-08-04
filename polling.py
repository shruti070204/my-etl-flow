import os
import time
import shutil
from prefect import flow,task
import polars as pl

WATCH_FOLDER="input_data"
PROCESSED_FOLDER="processed_files"

os.makedirs(WATCH_FOLDER,exist_ok=True)
os.makedirs(PROCESSED_FOLDER,exist_ok=True)

@task
def process_file(file):
    print(f"Processing file: {file}")
    
    # 🟡 EXTRACT
    try:
        df = pl.read_csv(file)
        print("Extracted Data:\n", df)
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return

    # 🟡 TRANSFORM: For example, filter rows where age > 26
    try:
        filtered_df = df.filter(pl.col("age") > 26)
        print("Transformed Data (age > 26):\n", filtered_df)
    except Exception as e:
        print(f"Error transforming data: {e}")
        return

    # 🟡 LOAD: Just printing for now (could be saving to DB or another file)
    try:
        print("Loaded result:\n", filtered_df)
    except Exception as e:
        print(f"Error loading data: {e}")

    print(f"File processed: {file}")
@flow
def polling_etl_flow():
    seen_files=set()

    while True:
        files= os.listdir(WATCH_FOLDER)
        new_files=[f for f in files if f not in seen_files]
        for filename in new_files:
            file_path=os.path.join(WATCH_FOLDER,filename)
            process_file(file_path)
            seen_files.add(filename)
            try:
            #move processed files
                shutil.move(file_path,os.path.join(PROCESSED_FOLDER,filename))
            except Exception as e:
                print(f"Error moving the file {filename}: {e}")
        time.sleep(60) #poll every 60 secs

if __name__=="__main__":
    polling_etl_flow()   