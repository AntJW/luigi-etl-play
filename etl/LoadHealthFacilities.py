import luigi
import sys
import pandas as pd
import datetime as dt
import sqlite3
from tools.Pipeline import PipelineLog, PipelineTarget
from configparser import ConfigParser

config = ConfigParser()
config.read('config.ini')


class LoadFacilityDimensions(luigi.Task):
    load_date = dt.date.today()
    start_datetime = dt.datetime.today()
    pl = PipelineLog("LoadHealthFacilities", "LoadFacilityDimensions", load_date, start_datetime)

    def run(self):
        self.pl.task_insert()
        try:
            conn = sqlite3.connect(config["DEFAULT"]["DB_PATH"])

            # Pulls data from city gov api into pandas DataFrame, then transforms it
            city_data = pd.read_json(config["DEFAULT"]["CITY_API"])
            city_df = city_data[["name_1", "name_2", "city", "latitude", "longitude", "street_1", "street_2", "zip"]]
            city_df.drop_duplicates(["name_1", "name_2", "street_1"], inplace=True)
            city_df.fillna("None", inplace=True)
            city_df["active"] = 1

            # Pulls existing Facilities from local database into pandas DataFrame
            existing_data = pd.read_sql(sql="SELECT * FROM HealthFacilities", con=conn)
            existing_data.rename(columns={"name_1": "o_name_1", "name_2": "o_name_2", "city": "o_city",
                                          "latitude": "o_latitude", "longitude": "o_longitude",
                                          "street_1": "o_street_1", "street_2": "o_street_2", "zip": "o_zip",
                                          "active": "o_active"},
                                 inplace=True)

            # Identify and insert new records, by performing left join
            left_merge = pd.merge(left=city_df, left_on=["name_1", "name_2", "street_1"], right=existing_data,
                                  right_on=["o_name_1", "o_name_2", "o_street_1"], how="left")
            new_records = left_merge[left_merge["o_name_1"].isnull()]
            new_records[["name_1", "name_2", "city", "latitude", "longitude", "street_1", "street_2", "zip",
                         "active"]].to_sql(name="HealthFacilities", con=conn, if_exists="append", index=False)

            # Identify and remove deactivated records, by performing right join
            right_merge = pd.merge(left=city_df, left_on=["name_1", "name_2", "street_1"], right=existing_data,
                                   right_on=["o_name_1", "o_name_2", "o_street_1"], how="right")
            deactivated_records = right_merge[right_merge["name_1"].isnull()]
            for index, record in deactivated_records.iterrows():
                query = """
                        UPDATE HealthFacilities
                        SET
                        active = '{}'
                        WHERE name_1 = '{}' AND name_2 = '{}' AND street_1 = '{}'
                        """.format(0, record["o_name_1"], record["o_name_2"], record["o_street_1"])
                cur = conn.cursor()
                cur.execute(query)
                conn.commit()
        except Exception as e:
            self.pl.send_error_email(e)
            sys.exit(e)

        self.pl.update_task_insert()

    def output(self):
        return PipelineTarget("LoadHealthFacilities", "LoadFacilityDimensions", self.load_date)


if __name__ == "__main__":
    luigi.run()
