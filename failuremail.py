# failuremail.py

def get_data():
    return {"name": "shruti", "age": 21}

from prefect import task, flow, get_run_logger
import smtplib
from email.mime.text import MIMEText
import yagmail

def send_alert(subject, message):
    try:
        receiver = "shrutimahadik2102@gmail.com"
        sender = "shrutimahadik2102@gmail.com"
        password = "zzzx ejhg mjhc pjue"

        msg = MIMEText(message)
        msg["Subject"] = subject
        msg["From"] = sender
        msg["To"] = receiver

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(sender, password)
            server.sendmail(sender, receiver, msg.as_string())
        print("mail sent")
    except Exception as e:
        print(f"Failed sending mail: {e}")

    try:
        yag = yagmail.SMTP(user=sender, password=password)
        yag.send(to=receiver, subject=subject, contents=message)
        print("mail sent")
    except Exception as e:
        print(f"failed sending mail: {str(e)}")

@task
def extract():
    logger = get_run_logger()
    logger.info("Extracting data...........")
    return get_data()

@flow(name="etl")
def etl():
    try:
        extract()
    except Exception as e:
        send_alert(
            subject="ETL flow failed",
            message=f"etl flow failed with error:\n{str(e)}"
        )
        raise

if __name__ == "__main__":
    import sys
    if "pytest" not in sys.modules:
        etl()
