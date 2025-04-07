import os
import copy
import Config.config as Cfg
from App_Helpers.Logger import *
from c2s.data_table import data_table
from DB_Helpers.pg_helper import touch_db


def boil_tables(template_file: Any, ns_prefix: str = "") -> []:
    # ----------------------------------------
    # Perform boiling:
    # Scanning input file line-by-line and generate
    # table definitions
    # ----------------------------------------
    dbg_print(os.path.abspath(template_file))

    if not os.path.exists(template_file):
        raise FileNotFoundError(f"No template file found on path: '{os.path.abspath(template_file)}'")

    tables = list()
    check_tables = list()
    dt: data_table = None

    with open(template_file, mode="r", encoding="utf-8") as tf:
        # Read template file row-by row...
        for _, l in enumerate(tf):
            # Comment found - skip it
            if l.startswith("#"):
                continue

            l = l.strip("\n")
            if l.lower().startswith("table") and dt:
                # ERROR: New table starts before current processing is completed
                msg = f"Source data error: new table definition found during processing table {dt.name} at line{_}"
                raise ValueError(msg)

            elif l.lower().startswith("table"):
                # Table marker found - start ne table definition
                dt = data_table(csv_line=l, ns_prefix=ns_prefix)
                tn = dt.get_name()
                print(f"Running table: {tn} ", end="")
                if tn in check_tables:
                    dbg_print(
                        f"Warning: table '{tn}' already exists in processed data. Table def started at {_} source row");

            elif l.startswith("---"):
                # End table definition marker - store current definition
                tables.append(copy.deepcopy(dt))
                print(f"done with {dt.get_col_count()} columns")
                dt = None
            elif dt:
                # Processing column definition on current table
                dt.append_column(csv_line=l)
                if _ % 3 == 0:
                    print(".", end="")

    return tables
# end of boil_tables()


def sql_codegen(template_source: str, template_output: str, sql_dir: str, csv_dir: str, target_db_name: str, ns_prefix: str = ""):
    assert template_source, "No Template file"
    assert os.path.exists(template_source) and os.path.isfile(template_source), \
        f"Template file not found: '{template_source}'"
    assert os.path.exists(sql_dir), f"sql_dir not found: '{sql_dir}'"
    assert os.path.exists(csv_dir), f"csv_dir not found: '{csv_dir}'"

    trc_print("***************************************************************************")
    trc_print("*")
    trc_print("* SQL CODE GENERATION IS RUNNING")
    trc_print("*")
    trc_print("***************************************************************************")
    trc_print(f" * Source file: '{template_source}'")
    trc_print(f" * Output file: '{template_output}'")
    trc_print(f" * SQL Loaders: '{sql_dir}'")
    trc_print(f" * CSV schemas: '{csv_dir}'")
    trc_print(f" *   NS Prefix: '{ns_prefix}'")
    if not touch_db(target_db_name):
        wrn_print(f"Can't touch target database: '{target_db_name}'")
    else:
        trc_print(f" * DB '{target_db_name}' seems touchable")
    trc_print("***************************************************************************")

    # Generate SQL definitions
    tables = boil_tables(template_file=template_file, ns_prefix=ns_prefix)

    trc_print(f"Tables: {len(tables)}")

    # Write Templated scripts out
    with open(template_output, mode="w", encoding="utf-8") as of:
        # output DROP TABLE statements in reverse order
        print(f"\n-- Below is a list of DROP TABLE Statements in reverse order", file=of)
        for td in tables[::-1]:
            print(td.drop_table_stmt(force_comment=False), file=of)

        for td in tables:
            print(f"\n-- Table definition: {td.get_name()}", file=of)
            print(f"-- CREATE STATEMENT", file=of)
            print(td.create_stmt(), file=of)
            print("------------------\n", file=of)

    trc_print("***************************************************************************")
    trc_print(" * Data Tables Templating Done!")
    trc_print("***************************************************************************")
    trc_print(" * Generate Data Loaders and CSV Schemas")
    trc_print("***************************************************************************")

    # Write data loading scripts
    if not os.path.exists(csv_dir):
        os.mkdir(csv_dir)

    for td in tables:
        tn = td.get_name()
        # Write sql-section
        # SQL loader will be placed here:
        sql_file = os.path.join(sql_dir, f"{tn}.sql")
        # Create path to underlined data file to insert into loader
        # Note: strip out ns_prefix
        data_file_path = os.path.join(csv_dir, f"{td.get_name(use_prefix=False)}.csv")
        with open(sql_file, mode="w", encoding="utf-8") as of:
            print(f"-- Data loader for table: {tn}", file=of)
            print(f"-- Uncomment next line if you want to completely refresh current data...", file=of)
            print(f"{td.truncate_stmt()}", file=of)
            print("------------------\n", file=of)
            print("-- BEFORE LOADING NOTE!", file=of)
            print(f"-- Make sure that the prepared and filled csv-file is reachable by DB Upload directory: '{sql_dir}'",
                  file=of)
            print("------------------\n", file=of)
            print(f"{td.copy_stmt(data_file=data_file_path)}", file=of)
            of.close()

        trc_print(f"Data loader for: '{tn:<32}'  done with file: '{sql_file}'")
    # trc_print("***************************************************************************")
    # trc_print(" * Data Loaders Done!")
    # trc_print("***************************************************************************")
    # trc_print(" * Generate CSV-schemas")
    # trc_print("***************************************************************************")

        # Write csv-section
        datafile_mark = "schema"
        output_file = os.path.join(csv_dir, f"{tn}_{datafile_mark}.csv")
        with open(output_file, mode="w", encoding="utf-8") as of:
            # Get list of loadable columns
            llist = td.get_loadable()
            # Get set of indexes
            index_list = [str(i) for i in range(0, len(llist))]
            # Write out type of data - this will need to remove
            print(f"{tn}", file=of)
            # Write out column indexes - this will be need to remove
            csv_line = "\t".join(index_list)
            print(f"{csv_line}", file=of)
            # Write out headers - this is the first row of real data set
            csv_line = "\t".join(llist)
            print(f"{csv_line}", file=of)
            of.close()


# ##############################
# Local test site
if __name__ == '__main__':
    trc_print(f"Running from '{os.getcwd()}'")
    config_ns_prefix: str = Cfg.C2S_ns_prefix
    # Configure in/out file names
    # template_in: str = Cfg.C2S_template_file  # This is configured file source
    # template_in: str = r"table_template_02.csv"

    # 2025-04-07:
    # ld_xxx references: change 'dict_string_value' to 'local_id'
    # template_in: str = r"table_template_03.csv"

    # 2025-04-07:
    # 1. construction_sites renamed to construction_sites
    # 2. Remove NOT NULL constraints from primary_site references
    # template_in: str = r"table_template_04.csv"

    # 2025-04-07
    # Remove NOT NULL constraints from activated_at for tables [project_parties] and [contract_parties]
    template_in: str = r"table_template_05.csv"


    template_out: str = "_".join([config_ns_prefix, r"templates.sql"]).strip("_").lower()

    # Configure paths
    # Substitute config path with local/debug
    # template_path = Cfg.C2S_templates_path
    template_path: str = r".\Templates"
    template_file: str = os.path.join(template_path, template_in)
    template_output: str = os.path.join(Cfg.C2S_upload_sql_path, template_out)
    # Configure loader paths: sql-scripts and csv-data
    sql_scripts_dir: str = Cfg.C2S_upload_sql_path
    csv_data_dir: str = Cfg.C2S_upload_data_path

    # Test run sql_codegen
    sql_codegen(template_source=template_file,
                template_output=template_output,
                sql_dir=sql_scripts_dir,
                csv_dir=csv_data_dir,
                target_db_name="test",
                ns_prefix=config_ns_prefix)
