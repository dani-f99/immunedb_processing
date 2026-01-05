# Import packages 
from source.helpers import check_packages, create_folders,read_json
import os 

# Cheeck that required packages exists
## check_packages(["pandas", "numpy", "sqlalchemy", "pymysql"])

db_name = read_json()["database"]["db_name"]
folders_main = ["tables_" + i for i in ["output", "processed", "raw"]]
folders_db = [os.path.join(i, db_name) for i in folders_main]
create_folders(folders_main + folders_db)