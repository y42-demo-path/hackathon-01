import json
import sys
import pandas as pd

import requests

from y42.v1.decorators import data_action
import logging
from google.oauth2 import service_account
import logging
import gspread

import subprocess


@data_action
def export_to_sheet(context, assets):
    subprocess.check_call([sys.executable, '-m', 'pip3', 'install', 'gspread'])

    df = assets.ref('bechdel_movies_aggr_year_genre_bechdel')
    # turn all columns to type string
    for column in df.columns.tolist():
        df[column] = df[column].astype(str)

    # load credentials & authorize
    SCOPES = ('https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive')
    service_account_info = json.loads(context.secrets.get('Google service account'))
    my_credentials = service_account.Credentials.from_service_account_info(service_account_info, scopes=SCOPES)
    
    client = gspread.authorize(my_credentials)
    logging.info('authorized')

    # connect to sheet by ID and sync data
    sht1 = client.open_by_key('10k0VO6cSJoVHStYpztBccjI_KAUYUsjOtTHLfm1wEME')
    worksheet = sht1.get_worksheet(0)
    logging.info('connected to sheet')
    data = [df.columns.to_numpy().tolist()] + df.to_numpy().tolist() # this works when all columns are of type string
    worksheet.update(data)
    logging.info('data updated')
