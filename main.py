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
    def __init__(self, file_path):
        self.file_path = file_path

    def create_spark_session(self):
        # Creating SparkSesion
        self.spark = SparkSession.builder.appName("TitanicDE").getOrCreate()
        self.spark.sparkContext.setLogLevel('WARN')

    def load_json_file_to_dataframe(self):
        # Loading json file to DataFrame
        self.df_json = self.spark.read.format(file_type) \
            .option("inferSchema", infer_schema) \
            .option("header", first_row_is_header) \
            .option("sep", delimiter) \
            .load(os.getcwd() + "/" + file)

    #dodac tu pozniej dekorator z argumentem zeby drukowal nad schematem ### {} dataframe schema ###
    def print_dataframe_schema(self, dataframe):
        self.dataframe.printSchema()
    def flatten_data_structure(self):
        # Flattening data structure and printing it
        self.dfTitanic_Flatt = self.df_json.select(col("Timestamp"),
                                         col("numeric_columns.*"),
                                         col("string_columns.*"))
    def fixing_data_types(self):
        # Fixing data types for Age and Fare columns and printing schema after that
        self.dfTitanic_Flatt2 = self.dfTitanic_Flatt.withColumn("Age", dfTitanic_Flatt["Age"].cast('long'))
        self.dfTitanic_Flatt2 = self.dfTitanic_Flatt.withColumn("Fare", dfTitanic_Flatt["Fare"].cast('float'))

        self.dfTitanic_Flatt2 = self.dfTitanic_Flatt \
            .withColumn("Age",
                        dfTitanic_Flatt["Age"]
                        .cast(LongType())) \
            .withColumn("Fare",
                        dfTitanic_Flatt["Fare"]
                        .cast(FloatType()))

    def insert_into_sqlite(self):
        # SQL Creating Connection and Cursor for SQLite
        conn = sqlite3.connect('titanic.db')
        cur = conn.cursor()

        # Executing SQL Queries to create table
        cur.execute(QUERY_CREATE_TABLE)
        conn.commit()
        dfTitanic_Flatt_pd.to_sql(table_name, conn, if_exists='replace', index=False)
        cur.execute(QUERY_SELECT_UNIQUE)
        conn.commit()

        rows1 = cur.fetchall()

        # Functions to count null and not null values in Cabin, Age, Embarked columns
        cur.execute(QUERY_CHECK_DUPLICATES)
        conn.commit()
        null_age = cur.execute(sql_null_age()).fetchall()
        null_cabin = cur.execute(sql_null_cabin()).fetchall()
        null_embarked = cur.execute(sql_null_embarked()).fetchall()

        # Printing final view from SQL Query without Pclass and SibSp columns
        print("######### SQL FINAL VIEW ##########")
        final_view = pd.read_sql_query("SELECT Timestamp,Parch,PassengerId,Survived,Age,Cabin,Embarked,Fare,Name,Sex,Ticket FROM titanic", conn)
        conn.commit()
        print(final_view)
        print("#############################")

        # Printing null and not null values from Age, Cabin, Embarked columns
        print("""Showing null and not null number \n of records from specified column \n COLUMN NAME: \n (null_count,) \n (not_null_count,)""")
        print("AGE:")
        for value in null_age:
            print(value)
        print("CABIN:")
        for value in null_cabin:
            print(value)
        print("EMBARKED:")
        for value in null_embarked:
            print(value)
        print("#############################")

        # Closing connection to db
        conn.commit()
        conn.close()

    def process(self):
        self.create_spark_session()
        self.load_json_file_to_dataframe()
        self.flatten_data_structure()
        self.fixing_data_types()
        self.insert_into_sqlite()
        self.print_dataframe_schema(dfTitanic_Flatt2)
        self.spark.stop()


