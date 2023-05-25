import os
import yaml

from os import path

def yaml_to_dict(dag_file_name=None, config_filename=None):
    """
    Parses yaml file into dict
    :param file_name: Name of file
    :return: dict
    """
    dir_name = path.dirname(__file__)
    path_to_file = path.join(dir_name, "../../dags/", f"{dag_file_name}/{config_filename}")
    with open(path_to_file) as file:
        config = yaml.safe_load(file)
    return config

def get_config(dag_file_name, config_filename):
    """
    Creates final config dict out of global and dag specific yamls
    :return: dict
    """
    dag_dict = yaml_to_dict(dag_file_name,config_filename)

    return dag_dict

def get_sql(dag_file_name, filename):
    """
    Gets sql file into string
    :param filename: Name of file
    :return: string with sql code
    """
    dir_name = os.path.dirname(__file__)
    path_to_file = os.path.join(dir_name, "../../dags/", f"{dag_file_name}/sql/{filename}")
    with open(path_to_file, 'r') as file:
        return file.read()   
    
def get_md(dag_file_name, filename):
    """
    Gets markdown file into string
    :param filename: Name of file
    :return: string with markdown code
    """
    dir_name = os.path.dirname(__file__)
    path_to_file = os.path.join(dir_name, "../../dags/", f"{dag_file_name}/{filename}")
    with open(path_to_file, 'r') as file:
        return file.read()