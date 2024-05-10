import requests
import pandas as pd
import logging
import json
import zipfile
import io


from y42.v1.decorators import data_loader

# use the @data_loader decorator to materialize an asset
# make sure you have a corresponding .yml table definition matching the function's name
# for more information check out our docs: https://www.y42.com/docs/sources/ingest-data-using-python

@data_loader
def movie_scenes_1(context) -> pd.DataFrame:
    # Reference secrets if needed
    
    #all_secrets = context.secrets.all() # get all secrets saved within this space
    #one_secret = context.secrets.get('<SECRET_NAME>') # get the value of a specific secret saved within this space
    
    username = context.secrets.get("kaggle-username")
    key = context.secrets.get("kaggle-key")
    #creds = base64.b64encode(bytes(f"{username}:{key}", "ISO-8859-1")).decode("ascii")
    #headers = {
     #   "Authorization": f"Basic {creds}"
    #}

    # Your code goes here
    url = "https://www.kaggle.com/api/v1/datasets/download/rajathmc/cornell-moviedialog-corpus"
    response = requests.get(url, auth=(username,key))

    logging.info(response.content)
    zf = zipfile.ZipFile(io.BytesIO(response.content))

    logging.info(zf.namelist)
    return


    # Return a DataFrame which will be materialized within your data warehouse
    df = pd.DataFrame(data)

    logging.info("Data fetched and DataFrame created successfully.")

    # to learn how to set up incremental updates and more
    # please visit https://www.y42.com/docs/sources/ingest-data-using-python
    return df
