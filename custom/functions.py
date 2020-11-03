import inspect
import logging
import datetime as dt
import math
from sqlalchemy.sql.sqltypes import TIMESTAMP,VARCHAR
import numpy as np
import pandas as pd
import json
import base64
import requests

from iotfunctions.base import BasePreload
from iotfunctions.base import BaseTransformer
from iotfunctions import ui
from iotfunctions.db import Database
from iotfunctions import bif
import datetime
from sqlalchemy.util.langhelpers import ellipses_string
import urllib3
import xml.etree.ElementTree as ET

logger = logging.getLogger(__name__)


# Specify the URL to your package here.
# This URL must be accessible via pip install
PACKAGE_URL = 'git+https://github.com/Marco1402/Satellite_AnomalyDetection@main'

class TorquerAnomaly(BaseTransformer):
    
    def __init__(self, wml_endpoint, deployment_id, apikey, input_items, output_item):
        self.input_items = input_items
        input_items.sort()
        self.output_item = output_item
        self.wml_endpoint = wml_endpoint
        self.deployment_id = deployment_id
        self.apikey = apikey
        super().__init__()

    def invoke_model(self, wml_endpoint, deployment_id, apikey, input_columns, input_values):

        API_KEY = apikey
        token_response = requests.post('https://iam.ng.bluemix.net/identity/token', data={"apikey": API_KEY, "grant_type": 'urn:ibm:params:oauth:grant-type:apikey'})
        mltoken = token_response.json()["access_token"]

        payload_scoring = {"input_data": [{"fields": input_columns, "values": input_values}]}

        response_scoring = requests.post(f'{wml_endpoint}/ml/v4/deployments/{deployment_id}/predictions?version=2020-10-31', json=payload_scoring, headers={'Content-Type': 'application/json', 'Authorization': 'Bearer ' + mltoken})

        return response_scoring.json()

    def execute(self, df):
        df = df.copy()

        input_values = []
        if ("anomalycheck_" + self.output_item) not in df.columns:
            NaN = np.nan
            df['anomalycheck_' + self.output_item] = NaN
        for index, row in df.iterrows():
            if pd.isnull(row['anomalycheck_' + self.output_item]):
                score_values = ([row[self.input_items[0]], row[self.input_items[1]], row[self.input_items[2]]])
                input_values.append(score_values)
            else:
                continue
    
        results = self.invoke_model(self.wml_endpoint, self.deployment_id, self.apikey, self.input_items, input_values)

        df[self.output_item] = np.nan
        for index, row in df.iterrows():
            if pd.isnull(row['anomalycheck_' + self.output_item]):
                df[self.output_item][index] = results["predictions"][0]["values"][0][0]
                results["predictions"][0]["values"].pop(0)
                df['anomalycheck_' + self.output_item][index] = True

        return df


    @classmethod
    def build_ui(cls):
        #define arguments that behave as function inputs
        inputs = []
        inputs.append(ui.UIMultiItem(
                    name = 'input_items',
                    datatype=float,
                    description = "Data items adjust",
                    # output_item = 'output_item',
                    is_output_datatype_derived = True)
                )
        inputs.append(ui.UISingle(name='wml_endpoint',
                              datatype=str,
                              description='Endpoint to WML service where model is hosted',
                              tags=['TEXT'],
                              required=True
                              ))
        inputs.append(ui.UISingle(name='deployment_id',
                              datatype=str,
                              description='Deployment ID for WML model',
                              tags=['TEXT'],
                              required=True
                              ))
        inputs.append(ui.UISingle(name='apikey',
                              datatype=str,
                              description='IBM Cloud API Key',
                              tags=['TEXT'],
                              required=True
                              ))

        outputs=[]
        outputs.append(ui.UISingle(name='output_item', datatype=float))
        return (inputs, outputs)