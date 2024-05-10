import requests
import pandas as pd
import logging
import json

from y42.v1.decorators import data_loader

# use the @data_loader decorator to materialize an asset
# make sure you have a corresponding .yml table definition matching the function's name
# for more information check out our docs: https://www.y42.com/docs/sources/ingest-data-using-python

@data_loader
def bechdel_api(context) -> pd.DataFrame:
    # Your code goes here
    url = "http://bechdeltest.com/api/v1/getAllMovies"
    response = requests.get(url)
    data = json.loads(response.content)

    # Return a DataFrame which will be materialized within your data warehouse
    df = pd.DataFrame(data)

    logging.info("Data fetched and DataFrame created successfully.")

    # to learn how to set up incremental updates and more
    # please visit https://www.y42.com/docs/sources/ingest-data-using-python
    return df
