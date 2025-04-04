# ########################################
# DB Config params
# ########################################
import os.path

DB_Server = "localhost"
DB_port = "5432"
DB_User = "postgres"
DB_Password = "QA_2ws3zxc"
DB_default_base = "test"

# ########################################
# c2s Config params
# ########################################
# TODO: replace this abs path with next line
C2S_templates_path = r"C:\Users\aa.vasilyev\Desktop\DEV\Python\ipm_data\ipm_data\c2s\Templates"
# C2S_templates_path = "./c2s/Templates"
C2S_template_file = r"table_template_01.csv"
C2S_ns_prefix = "ch"
C2S_upload_path = os.path.join(r"C:/Users/Public/PGData/", DB_default_base, C2S_ns_prefix)
C2S_upload_sql_path = os.path.join(C2S_upload_path, "sql")
C2S_upload_data_path = os.path.join(C2S_upload_path, "data")
