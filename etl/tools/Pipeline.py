import luigi
import sqlite3
import sys
import pandas as pd
import datetime as dt
from configparser import ConfigParser
from tools.Email import send_email

config = ConfigParser()
config.read('config.ini')


class PipelineLog:
    def __init__(self, etl_name, task_name, load_date, start_time):
        self.etl_name = etl_name
        self.task_name = task_name
        self.load_date = load_date
        self.start_time = start_time

    def task_insert(self):
        try:
            conn = sqlite3.connect(config["DEFAULT"]["DB_PATH"])
            cur = conn.cursor()
            query = """
                    INSERT INTO PipelineTasks (etl_name, task_name, load_date, start_time, success)
                    VALUES ('{}', '{}', '{}', '{}', '{}')
                    """.format(self.etl_name, self.task_name, self.load_date, self.start_time, 0)
            cur.execute(query)
            conn.commit()
            conn.close()
        except Exception as e:
            sys.exit(e)

    def update_task_insert(self):
        try:
            end_time = dt.datetime.today()
            conn = sqlite3.connect(config["DEFAULT"]["DB_PATH"])
            cur = conn.cursor()
            query = """
                    UPDATE PipelineTasks
                    SET success = 1,
                    end_time = '{}'
                    WHERE etl_name = '{}' AND task_name = '{}' AND load_date = '{}' AND start_time = '{}'
                    """.format(end_time, self.etl_name, self.task_name, self.load_date, self.start_time)
            cur.execute(query)
            conn.commit()
            conn.close()
        except Exception as e:
            sys.exit(e)

    def send_error_email(self, error_msg):
        try:
            email_list = config["DEFAULT"]["ERROR_EMAIL_LIST"]
            emails = email_list.split(",")
            for email in emails:
                send_email(email, self.etl_name, self.task_name, self.load_date, str(error_msg))

        except Exception as e:
            sys.exit(e)


class PipelineTarget(luigi.Target):

    def __init__(self, etl_name, task_name, load_date):
        self.etl_name = etl_name
        self.task_name = task_name
        self.load_date = load_date

    def exists(self):
        try:
            conn = sqlite3.connect(config["DEFAULT"]["DB_PATH"])
            query = """
                    SELECT 1 FROM PipelineTasks WHERE etl_name = '{}'
                    AND task_name = '{}' AND load_date = '{}' AND success = 1
                    """.format(self.etl_name, self.task_name, self.load_date)

            result = pd.read_sql(sql=query, con=conn)
            if result.empty:
                return False
            else:
                return True
        except Exception as e:
            sys.exit(e)

