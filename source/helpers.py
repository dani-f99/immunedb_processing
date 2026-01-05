# Import required packages
from sqlalchemy import create_engine
import importlib.metadata
import pandas as pd
import json
import os


##############################################################
# Custom function that cheeck if python packages are installed
def check_packages(package_names):
    status = {}

    n_total = len(package_names)
    n_installed = 0
    package_df = pd.DataFrame(index=package_names, columns=["installed","not_installed"], data=0)

    for pkg in package_names:
        try:
            # metadata.version returns the version string if installed
            dist_version = importlib.metadata.version(pkg)
            status[pkg] = f"Installed (v{dist_version})"
            package_df.loc[pkg, "installed"] = 1
            n_installed += 1

        except importlib.metadata.PackageNotFoundError:
            status[pkg] = "Not Installed !!!"
            package_df.loc[pkg, "not_installed"] = 1
    
    print(f"> {n_installed}/{n_total} packages are installed:")

    for i in status:
        print(f"{i} package is {status[i]}")

    return package_df


################################################################################$$##########
# Reading information from json file. Used to extract the parameters from the `config.json`.
def read_json(path:str = "config.json") -> dict:
    """
    path : str -> path of the json file
    """

    with open(path) as config:
        config_f = json.load(config)

    return config_f


#####################################################
# Creating folder according to the and program scheme
def create_folders(req_folders : list = ["temp_data", "tms_input", "reports"]):
    """
    req_folders : str -> required folders path, if subfolder exsits input '\\' between folders.
    """

    for folder in req_folders:
        if os.path.exists(folder) is False:
            os.mkdir(folder)
            print(f"> folder `{folder}` was created.")

        else:
            print(f"> folder `{folder}` exists, continuing.")


######################################################################################################
# A custom function that connects to MySQL server, execute query and returns the results as dataframe.
class sql_conn():
    def __init__ (self):
        """
        Automaticly takes sql credentials from the `config.json` file:
        username:str -> Username credentials for the MySQL server.
        password:str -> Password credentials for the MySQL server.
        adress:str -> IP adress of the MySQL server.
        port:str -> Port of the MySQL server.
        qry:str -> SQL query to be executed.
        """

        # Setting up sql credentials
        sql_cred, sql_db = read_json()["sql"], read_json()["database"]
        username, password, adress, port = sql_cred["username"], sql_cred["password"], sql_cred["adress"], sql_cred["port"]
        self.db_name = sql_db["db_name"]

        # Setting up MySQL connenction
        connection_mysql = f"mysql+pymysql://{username}:{password}@{adress}:{port}/{self.db_name}"
        self.engine = create_engine(connection_mysql)
        print(f"> Established connecntion to the {self.db_name} database.")

    def get_table(self,
                  table_name,
                  sql_qry : str = "defualt",
                  save_table:bool = True) -> pd.DataFrame:
        """
        qry : str -> string of sql input, to be executed on the sql server.
        save_table : bool -> to save the returened sql table to the raw_folder.
        """
    
        # cheeking for custom query input
        qry_bool = (sql_qry == "defualt")
        query_map = {True : f"SELECT * FROM {self.db_name}.{table_name}",
                     False : sql_qry}

        # Executing the query
        try:
            sql_qry = query_map[qry_bool]
            qry_df = pd.read_sql(sql_qry, self.engine)
            print(f"> Query executed successfully: \n-- {sql_qry} --")
        except:
            raise Exception("> Got error while exeuting the query, please verify syntax.")

        if save_table:
            save_path = os.path.join("tables_raw", self.db_name, f"{self.db_name}_{table_name}.tsv")
            qry_df.to_csv(save_path, sep='\t', index=False)

        return qry_df
    
    # Closing the connenction
    def sql_close(self):
        self.engine.dispose()
        print("> MySQL connenction terminated.") 