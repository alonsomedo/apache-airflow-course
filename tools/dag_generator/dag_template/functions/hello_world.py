import requests
import logging
import csv
import gzip
import io
import json

from airflow.exceptions import AirflowException


def print_hello_world_and_name(name: str):
    
    """    
    This function prints the message "Hello world" and the name of the user 
    ----------
    name : str
        The name of the user
    Returns
    -------
        A message in the console
    """
    print(f"Hello world {name} !")