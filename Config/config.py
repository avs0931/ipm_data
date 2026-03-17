import os.path

# ########################################
# App Environment
# ########################################
APP_Verbose_Mode: bool = True     # TRACE messages are allowed
APP_Debug_Mode: bool = True       # DEBUG messages are allowed

# ########################################
# DB Config params
# ########################################
DB_Server = "localhost"           # PostgreSQL server location
DB_port = "5432"                  # PostgreSQL server connection port
DB_User = "postgres"              # Default DB Server
DB_Password = "**********"        # User password
DB_Default = "test"               # Default database (schema - public)

# ########################################
# c2s Config params
# ########################################
C2S_ns_prefix = "ch"              # DDL/DML table names prefix. An empty prefix is possible.
# About templates
C2S_templates_path = r"./c2s/Templates"
C2S_template_file = r"table_template_05.csv"
# About results
C2S_upload_path = os.path.join(r"C:/Users/Public/PGData/", DB_Default, C2S_ns_prefix)
C2S_upload_sql_path = os.path.join(C2S_upload_path, "sql")     # SQL Loaders will be placed here
C2S_upload_data_path = os.path.join(C2S_upload_path, "data")   # CSV data should be placed here

# ########################################
# Datagen Config params
# ########################################
# TODO: replace this abs path with next line
DG_generation_data_path = r"C:\Users\aa.vasilyev\Desktop\DEV\Python\ipm_data\ipm_data\Data\Datagen\csv"
# DG_generation_data_path = r".\Data\Datagen\csv"
