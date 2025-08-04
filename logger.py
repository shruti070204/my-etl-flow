from prefect import flow, task, get_run_logger

@task
def load():
    logger=get_run_logger()
    logger.info("Starting load data")
    try:
        data=[1,2,3,4,5]
        logger.info(f"Data loaded:{data}")
        return data
    except Exception as e:
        logger.error(f"Failed to load data{e}")
        raise

@task
def process(data):
    logger=get_run_logger()
    logger.info("Starting to process data...")
    processed=[i*10 for i in data]
    logger.info(f"data processed: {processed}")
    return processed

@task
def save(data):
    logger=get_run_logger()
    logger.info("Saving data in database....")
    logger.info(f"Data saved: {data}")

@flow
def pipeline():
    logger=get_run_logger()
    logger.info("ETL Flow Started")
    raw=load()
    clean=process(raw)
    save(clean)
    logger.info("ETL Flow completed")

if __name__=="__main__":
    pipeline()