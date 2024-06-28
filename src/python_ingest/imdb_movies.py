import requests
import base64
import zipfile
import io
import pandas as pd

from y42.v1.decorators import data_loader

@data_loader
def imdb_movies(context) -> pd.DataFrame:
    #1: Preparing the URL.
    base_url = "https://www.kaggle.com/api/v1"
    owner_slug = "adriankiezun"
    dataset_slug = "imdb-dataset-2023"
    dataset_version = "1"

    url = f"{base_url}/datasets/download/{owner_slug}/{dataset_slug}?datasetVersionNumber={dataset_version}"

    #2: Encoding the credentials and preparing the request header.
    username = context.secrets.get("kaggle-username")
    key = context.secrets.get("kaggle-key")
    creds = base64.b64encode(bytes(f"{username}:{key}", "ISO-8859-1")).decode("ascii")
    headers = {
    "Authorization": f"Basic {creds}"
    }

    #3: Sending a GET request to the URL with the encoded credentials.
    response = requests.get(url, headers=headers)

    #4: Loading the response as a file via io and opening it via zipfile.
    zf = zipfile.ZipFile(io.BytesIO(response.content))

    #5: Reading the CSV from the zip file and converting it to a dataframe.
    file_name = "imdb_data.csv"
    df = pd.read_csv(zf.open(file_name))

    return df
