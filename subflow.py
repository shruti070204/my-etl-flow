from prefect import flow, task
import polars as pl

@task 
def read(file: str)-> pl.DataFrame:
    print(f"\nReading :{file}")
    df= pl.read_csv(file) #read csv file into polars dataframe
    print(df) #print preview of raw data
    return df #returns the raw data dataframe to be used later

@flow(name="data validation subflow")#this is subflow(a reusable flow inside the main one)
def  validate_data(df: pl.DataFrame)-> pl.DataFrame: #accepts the raw dataframe and applies cleaning steps
    print("\nRunning validation....")
    df=df.rename({col: col.strip() for col in df.columns}) #removes any extra spaces form columns names
    df=df.filter(~df["name"].is_null()) #remones row where name column in null
    def is_numeric(val: str)->bool: #helper function to check if string value can be converted to a number
        try:
            float(val)
            return True
        except:
            return False
    df=df.filter(df["age"].map_elements(is_numeric,return_dtype=pl.Boolean)) #filters rows where age column in NAN, uses map_element to apply the is _numeric() funtion element wise
    print("\nCleaned data:") #prints and return clean dataframe
    print(df)
    return df

@task 
def count(df: pl.DataFrame): #simply counts and retirn number of row left after cleaning data
    print(f"\nTotal rows:{df.shape[0]}")

@task
def save(df: pl.DataFrame,output:str):
    df.write_csv(output) #saves clean dataframe to csv file
    print(f"\nSaved to: {output}") #confirmation

@flow(name="main ETL flow with subflow")
def etl_pipiline(file: str, output: str):
    df= read(file) #task: read og csv
    validate=validate_data(df) #subflow: clean/ validate data
    count(validate) #task: count clead rows
    save(validate,output) #task: save cleaned csv

if __name__=="__main__":
    etl_pipiline("people.csv","clean.csv")