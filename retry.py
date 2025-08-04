from prefect import task, flow
import random
import time
#sitmulate API call with retry
@task(retries=3,retry_delay_seconds=2)
def extract():
    print("Trying to fetch data from fake API..")
    #simulate API failure randomly
    if random.random()<0.5:
        print("API failed! Retrying....")
        raise Exception("API Error")
    print("API successful")
    return {"name":"Shruti","city":"Mumbai","age":21}
#transform data
@task
def transform(data: dict):
    print("Transforming data.....")
    data["name"]=data["name"].upper()
    return data
#load data(simply print or save)
@task
def load(data:dict):
    print("Loading data.....")
    print(f"Final data: {data}")
#putting all together in a flow
@flow(name="etl-with-retries")
def etl_flow():
    raw_data=extract()
    clean_data=transform(raw_data)
    load(clean_data)
#run the flow
if __name__=="__main__":
    etl_flow()