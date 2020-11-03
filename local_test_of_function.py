import datetime as dt
import json
import re
import pandas as pd
import numpy as np
import logging
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, func
from iotfunctions.db import Database
from iotfunctions.enginelog import EngineLogging

EngineLogging.configure_console_logging(logging.DEBUG)

'''
You can test functions locally before registering them on the server to
understand how they work.

Supply credentials by pasting them from the usage section into the UI.
Place your credentials in a separate file that you don't check into the repo. 

'''

with open('credentials/monitor-credentials.json', encoding='utf-8') as F:
    credentials = json.loads(F.read())
db_schema = "BLUADMIN"
entity_name = "IOT_SATELLITE"
db = Database(credentials=credentials)

'''
Import and instantiate the functions to be tested 

The local test will generate data instead of using server data.
By default it will assume that the input data items are numeric.

Required data items will be inferred from the function inputs.

The function below executes an expression involving a column called x1
The local test function will generate data dataframe containing the column x1

By default test results are written to a file named df_test_entity_for_<function_name>
This file will be written to the working directory.

'''

from custom.functions import TorquerAnomaly
#"1de58f9d-190e-4989-aa45-2ddc91b60527"
fn = TorquerAnomaly(
    wml_endpoint="https://us-south.ml.cloud.ibm.com",
    deployment_id="9ceb7e48-6099-42e2-aa48-9453179d8c87",
    apikey="mMMwlZjelJR0gPb1qVQQPVTBDVuS5fcOGoVlYrmqJ6jn",
    input_items = ["satellite_y_torquer_current", 
            "satellite_y_torquer_power", 
            "satellite_y_torquer_voltage"],
    output_item = "anomaly_score")
#df = fn.execute_local_test(db=db,db_schema=db_schema, to_csv=True)
df = db.read_table(table_name=entity_name, schema=db_schema)
#print(df)

results = TorquerAnomaly.execute(fn, df)
print(results)
results.to_csv("results.csv")
'''
Register function so that you can see it in the UI
'''
#db.register_functions([TorquerAnomaly])

