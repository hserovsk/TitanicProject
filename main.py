import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col
import sqlite3
from sql_functions import *
from pyspark.sql.types import StringType, DateType, FloatType, LongType
import pandas as pd
from pandas import DataFrame

pd.set_option('expand_frame_repr', False)

# File location and type
file = 'nested_titanic_data.json'
file_type = "json"

# CSV options
infer_schema = "false"
first_row_is_header = "false"
delimiter = ","


class TitanicSpark:
    def __init__(self, file, file_type="json", infer_schema="false", first_row_is_header="false", delimiter=","):
        self.file = file
        self.file_type = file_type
        self.infer_schema = infer_schema
        self.first_row_is_header = first_row_is_header
        self.delimiter = delimiter
        try:
            self.spark = SparkSession.builder.appName("TitanicDE").getOrCreate()
            self.spark.sparkContext.setLogLevel('WARN')
        except Exception as e:
            print(f"Exception occured while creating Spark Session: {str(e)}")
            raise

    def load_data(self):
        "Method that loads data from json file (from current directory/file) to df_json variable"
        try:
            df_json = self.spark.read.format(self.file_type) \
                .option("inferSchema", self.infer_schema) \
                .option("header", self.first_row_is_header) \
                .option("sep", self.delimiter) \
                .load(os.getcwd() + "/" + self.file)
            df_json.printSchema()
        except Exception as e:
            print(f"Exception occured while loading data: {str(e)}")
            raise
        return df_json

    def flatten_data(self, df_json):
        "Method that flattens structure of the dataframe before inserting it into sqlite"
        try:
            df_flatt = df_json.select(col("Timestamp"),
                                      col("numeric_columns.*"),
                                      col("string_columns.*"))

            df_flatt.printSchema()
        except Exception as e:
            print(f"Exception occured while flattening data: {str(e)}")
            raise
        return df_flatt

    def fix_data_types(self, df_flatt):
        "Method that is changing datatypes for Age column (to Long Type) and for Fare column (Float Type)"
        try:
            df_flatt2 = df_flatt \
                .withColumn("Age",
                            df_flatt["Age"]
                            .cast(LongType())) \
                .withColumn("Fare",
                            df_flatt["Fare"]
                            .cast(FloatType()))
            df_flatt2.printSchema()
        except Exception as e:
            print(f"Exception occured while fixing data types: {str(e)}")
            raise
        return df_flatt2

    def convert_to_pandas(self, df_flatt2):
        "Converting Spark DataFrame to Pandas Dataframe"
        return df_flatt2.toPandas()

    def create_sql_connection(self):
        "Method that creates connection with sqlite database and cursor object"
        try:
            conn = sqlite3.connect('titanic.db')
            cur = conn.cursor()
        except Exception as e:
            print(f"Exception occured while creating connection with database or cursor: {str(e)}")
            raise
        return conn, cur

    def execute_sql_commands(self, conn, cur, df_pd):
        """Executing SQL commands from sql_functions.py module"""
        try:
            cur.execute(QUERY_CREATE_TABLE)
            conn.commit()
            df_pd.to_sql(table_name, conn, if_exists='replace', index=False)
            cur.execute(QUERY_SELECT_UNIQUE)
            conn.commit()

            cur.execute(QUERY_CHECK_DUPLICATES)
            conn.commit()

            null_age = cur.execute(sql_null_age()).fetchall()
            null_cabin = cur.execute(sql_null_cabin()).fetchall()
            null_embarked = cur.execute(sql_null_embarked()).fetchall()
        except Exception as e:
            print(f"Exception occured during SQL operations: {str(e)}")
            raise
        return null_age, null_cabin, null_embarked

    def print_final_view(self, conn):
        "Printing final view from SQLite database"
        try:
            final_view = pd.read_sql_query(
                "SELECT Timestamp,Parch,PassengerId,Survived,Age,Cabin,Embarked,Fare,Name,Sex,Ticket FROM titanic",
                conn)
            conn.commit()
            print(final_view)
        except Exception as e:
            print(f"Exception occured while getting or printing final view: {str(e)}")
            raise

    def close_sql_connection(self, conn):
        "Method that closes connection with database"
        try:
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"Exception occured while closing SQL connection: {str(e)}")
            raise


titanic_processor = TitanicSpark(file='nested_titanic_data.json')
df_json = titanic_processor.load_data()
df_flatt = titanic_processor.flatten_data(df_json)
df_flatt2 = titanic_processor.fix_data_types(df_flatt)
df_pd = titanic_processor.convert_to_pandas(df_flatt2)
conn, cur = titanic_processor.create_sql_connection()
null_age, null_cabin, null_embarked = titanic_processor.execute_sql_commands(conn, cur, df_pd)
titanic_processor.print_final_view(conn)
titanic_processor.close_sql_connection(conn)
