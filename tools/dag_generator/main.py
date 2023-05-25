import os
import shutil


class DagGeneratorManager:
    """
    :param dag_name: Name of the dag enter by the user
    :param dag_template_default_name: Name of the dag templated folder
    :param dag_template_directory_path: Path of the dag templated folder
    :param dag_destination_directory_path: Destination path where the dag will be created
    :param default_folders: Default folders according to DataOps standards
    """
    
    def __init__(self, 
                 dag_name: str, 
                 dag_template_default_name: str="dag_template", 
                 default_folders: list=["sql","schema","tools"]
                 ):
        self.dag_name = dag_name
        self.dag_template_default_name = dag_template_default_name
        self.dag_template_directory_path = os.path.dirname(__file__)
        self.dag_destination_directory_path = self.get_dag_destination_directory_path()
        self.default_folders = default_folders
        
    
    def validate_folder_existance(self):
        """
        Validate if the folder already exists in the destination path.
        :return:
        """
        exists = os.path.exists(self.dag_destination_directory_path)
        if (exists):
            raise FileExistsError(f"The {self.dag_destination_directory_path} already exists. Please choose another name for your dag.")
        
        
    def get_dag_destination_directory_path(self):
        """
        Returns the destination path for creating the new dag.
        :return destination_path: 
        """
        dir_name = self.dag_template_directory_path
        destination_path = os.path.join(dir_name, "../../dags") + "/" + self.dag_name
        return destination_path
    
    
    def copy_dag_template_folder_to_destination(self):
        """
        Copy the dag template folder to the destination path with all the required structure according to DataOps standards.
        :return:
        """
        try:
            source = self.dag_template_directory_path + "/" + self.dag_template_default_name
            destination = self.dag_destination_directory_path 
            shutil.copytree(source,destination)
            self.rename_dag_file(destination)
            self.create_default_folders(destination)
            self.rename_import_custom_function_path(destination, self.dag_name)
            print(f"The dag folder was created successfully.")
        except Exception as e:
            shutil.rmtree(destination)
            raise e
    
    def rename_import_custom_function_path(self, dag_directory, dag_name):
        """
        Function that renames the import with the correct dag_name for the custom_python_function
        :return:
        """
        dag_file_path = dag_directory + "/" + self.dag_name + ".py"
        with open(dag_file_path, 'r') as file :
            dag_file = file.read()

        dag_file = dag_file.replace('dag_template', dag_name)

        with open(dag_file_path, 'w') as file:
            file.write(dag_file)
        
    def create_default_folders(self, destination: str):
        """
        Create the default folders according to DataOps standards.
        :return:
        """
        for folder in self.default_folders:
            os.makedirs(f"{destination}/{folder}")
            
    
    def rename_dag_file(self, dag_directory: str):
        """
        Rename the main dag file according to the dag name entered by the user.
        :param dag_directory: directory of the dag on dw_airflow_jobs/dags/{dag_directory}
        :return:
        """
        os.rename(dag_directory + "/" + self.dag_template_default_name + ".py", 
                  dag_directory + "/" + self.dag_name + ".py")
    
    
    def validate_dag_naming_purpose(self):
        """
        Validates that the dag name is according to the naming convention according to DataOps standards.
        :return:
        """
        valid_purposes = ["import", "export", "process"]
        if not any([vp in self.dag_name for vp in valid_purposes]):
            raise Exception(f"The purpose is invalid. Choose a valid purpose: import, export or process for your dag name.")
    
    
    def run(self):
        """
        Execute functions in the right order to create the new dag folder according to DataOps standards.
        :return:
        """
        self.validate_dag_naming_purpose()
        self.validate_folder_existance()
        self.copy_dag_template_folder_to_destination()
             
        
if __name__ == '__main__':
    print("Dag naming convention is {purpose}_{source}_{topic}_{destination}")
    print(f"""
          Examples:
            - DAG that exports multiple event data to Amplitude: export_bq_events_amplitude
            - DAG that imports multiple amplitude events into BigQuery: import_amplitude_events_bq
            - DAG that runs a process with Databricks but returns the result to BigQuery: process_bq_autos_high_purchase_intent
            - DAG that exports BQ data into Kinesis: export_bq_module_notification_kinesis
          """)
    
    dag_name = input("Please enter your dag name: ")
    dag_generator = DagGeneratorManager(dag_name=dag_name)
    dag_generator.run()
    
    