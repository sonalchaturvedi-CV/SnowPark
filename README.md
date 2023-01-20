# SnowPark
This is a Python code to test SnowPark. 

# SNOWFLAKE SNOWPARK'S SESSION

#IMPORT SNOWFLAKE LIBRARIES

import os
import snowflake.snowpark 
from snowflake.snowpark import Session
import pandas as pd

#SNOWFLAKE CONNECTOR

#SNOWFLAKE CONNECTOR TO ESTABLISH CONNECTION

import snowflake.connector

#ESTABLISH CONNECTION WITH SNOWFLAKE USING SNOWFLAKE-SNOWPARK CONNECTOR   

conn = snowflake.connector.connect(
            user="USERNAME",
            password="PASSWORD",
            account="ACCOUNT_NAME",
            warehouse="WAREHOUSE",
            database="DATABASE_NAME",
            schema="PUBLIC",
            client_session_keep_alive="TRUE")
            
#SELECT THE DATABASE TO USE

conn.cursor().execute("USE DATABASE SNOWPARK_DEMO")

#CHECK CONNECTION BY TRYING TO CREATE TABLE AND INSERTING VALUES

conn.cursor().execute(
        "CREATE OR REPLACE TABLE "
        "test_table(NUM integer, STR string)")
conn.cursor().execute(
        "INSERT INTO test_table(NUM, STR) VALUES " + 
        "    (3, 'Three'), " + 
        "    (9, 'Nine')")
        
#TABLE ON WHICH WE PERFORM TRANSFORMAATIONS

conn.cursor().execute(
        "CREATE OR REPLACE TABLE "
        "Employee_Details(FIRST_NAME string, LAST_NAME string,  ADDRESS string, CITY string, STATE string)")
conn.cursor().execute(
        "INSERT INTO Employee_Details(FIRST_NAME, LAST_NAME,  ADDRESS, CITY, STATE) VALUES " + 
        "    ('Sonal', 'Chatur', '850 Drive', 'Dallas', 'TX'), " + 
        "    ('Hello', 'world', '850 Drive', 'Manhattan', 'New York')")
        
# PERFORMING TRANSFORMATIONS

#CREATE SESSION (SNOWPARK OBJECT)

connection_parameters = {
    "account": "ACCOUNT_NAME",
    "user": "USER_NAME",
    "password": "PASSWORD",
    "role": "ROLE",
    "warehouse": "WAREHOUSE",
    "database": "SNOWFLAKE DATABASE",
    "schema": "SCHEMA_NAME"
}
session = Session.builder.configs(connection_parameters).create()
table_df = session.table("EMPLOYEE_DETAILS")
table_df.show(50)
print(type(table_df))
us_state_to_abbrev = session.table("STATE_ABB")
us_state_to_abbrev.show()
print(type(us_state_to_abbrev))
from snowflake.snowpark.functions import col

#JOINING table_df AND us_state_to_abbrev

dfJoin = table_df.join(us_state_to_abbrev,table_df.col("STATE") == us_state_to_abbrev.col("STATE")
    ).select('FIRST_NAME', 'LAST_NAME', 'ADDRESS', 'CITY', us_state_to_abbrev.STATE_CODE.alias('STATE'))

#CHECK TRANSFORMED DATA

dfJoin.show()
print(type(dfJoin))

#WRITE TRANSFORMED DATA BACK TO SNOWFLAKE TABLE (OR A NEW ONE)

dfJoin.write.mode("overwrite").save_as_table("JOIN_TABLE")

# TEST

dfJoin2 = table_df.join(us_state_to_abbrev,table_df.col("STATE") == us_state_to_abbrev.col("STATE")
    ).select('FIRST_NAME', 'LAST_NAME', us_state_to_abbrev.STATE_CODE.alias('STATE'))
dfJoin2.show()
dfJoin2.write.mode("overwrite").save_as_table("JOIN_TABLE_TEST")
IF CHANGING TO PANDAS FOR FURTHER TRANSFORMATIONS

#IMPORT LIBRARIES FOR WRITING. TRANSFORMED DATA INTO SNOWFLAKE
from snowflake.connector.pandas_tools import write_pandas
from snowflake.connector.pandas_tools import pd_writer

#CHECK IF DATA HAS BEEN TRANSFORMED
print(table_pandas.head())

#WRITING DATA BACK TO SNOWFLAKE

write_pandas(conn, dfJoin, 'EMPLOYEE_DETAILS')

# INSTALL LIBRARIES
pip install sklearn
pip install matplotlib
pip install seaborn
pip install config
pip install pandas
pip install "snowflake-connector-python[pandas]"
pip install "snowflake-connector-python[secure-local-storage,pandas]"
