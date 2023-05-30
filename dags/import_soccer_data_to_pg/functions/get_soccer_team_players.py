import logging
import json
import requests
import pandas as pd
import numpy as np
from pandas import json_normalize
from airflow.models import Variable

def get_soccer_team_players(soccer_team_id, endpoint):
    url = endpoint
    params = {'team': soccer_team_id}
    headers = {
    'x-rapidapi-host': "v3.football.api-sports.io",
    'x-rapidapi-key': Variable.get('soccer_secret_key')
    } 
    
    response = requests.get(url=url, params=params, headers=headers)
    df = json.loads(response.text)['response'][0]['players']
    df = json_normalize(df)
    
    # Data wrangling
    df = df.replace({np.nan: None})
    df["team_id"] = soccer_team_id
    logging.info(df)
    
    df.to_csv(f"/tmp/{soccer_team_id}.csv", sep='\t', index=False, header=False)
    logging.info(f"The file {soccer_team_id}.csv was generated successfully.")