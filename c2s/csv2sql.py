import os
import copy
import Config.config as Cfg
from App_Helpers.Logger import *
from c2s.data_table import data_table


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


def sql_codegen(template_data_file: str):
    assert template_data_file, "No template file"
    # ----------------------------------------
    # Boilerplate
    # Generate SQL-scripts
    # ----------------------------------------

    # Set-up files
    template_file = os.path.join(Cfg.C2S_templates_path, template_data_file)
    sql_dir = Cfg.C2S_upload_sql_path
    csv_dir = Cfg.C2S_upload_data_path
    output_file = os.path.join(Cfg.C2S_upload_sql_path, r"table_templates.sql")

    # Generate SQL definitions
    tables = boil_tables(template_file=template_file, ns_prefix=Cfg.C2S_ns_prefix)

    print(f"Tables: {len(tables)}")

    # Write Creation scripts out
    with open(output_file, mode="w", encoding="utf-8") as of:
        # output DROP TABLE statements in reverse order
        print(f"\n-- Below is a list of DROP TABLE Statements in reverse order", file=of)
        for td in tables[::-1]:
            print(td.drop_table_stmt(), file=of)

        for td in tables:
            print(f"\n-- Table definition: {td.get_name()}", file=of)
            print(f"-- CREATE STATEMENT", file=of)
            print(td.create_stmt(), file=of)
            print("------------------\n", file=of)

    dbg_print(" --- ")

    # Write data loading scripts
    data_path = os.path.join(Cfg.C2S_upload_data_path)
    if not os.path.exists(data_path):
        os.mkdir(data_path)

    for td in tables:
        tn = td.get_name()
        # Write sql-section
        sql_file = os.path.join(sql_dir, f"{tn}.sql")
        data_file = os.path.join(csv_dir, f"{tn}.csv")
        with open(sql_file, mode="w", encoding="utf-8") as of:
            print(f"Data copy for: {tn:<32} ... ", end="")
            print(f"-- Data loader for table: {tn}", file=of)
            print(f"-- Uncomment next line if you want to completely refresh current data...", file=of)
            print(f"{td.truncate_stmt()}", file=of)
            print("------------------\n", file=of)
            print("-- BEFORE LOADING NOTE!", file=of)
            print(f"-- Make sure that the prepared and filled csv-file is reachable by DB Upload directory: '{sql_dir}'",
                  file=of)
            print("------------------\n", file=of)
            print(f"{td.copy_stmt(data_file=data_file)}", file=of)
            print(f" done with file: '{output_file}'")
            of.close()

        # Write csv-section
        datafile_mark = "data"
        output_file = os.path.join(data_path, f"{tn}_{datafile_mark}.csv")
        with open(output_file, mode="w", encoding="utf-8") as of:
            # Get list of loadable columns
            llist = td.get_loadable()
            # Get set of indexes
            ilist = [i for i in range(0, len(llist))]
            # Write out type of data - this will be need not remove
            print(f"{tn}", file=of)
            # Write out indexes - this will be need to remove
            csv_line = "\t".join(map(lambda x: str(x), ilist))
            print(f"{csv_line}", file=of)
            # Write out headers - this is the first row of real data set
            csv_line = "\t".join(llist)
            print(f"{csv_line}", file=of)
            of.close()


# ##############################
# Local test site
if __name__ == '__main__':
    template_file = "table_template_01.csv"
    sql_codegen(template_file)
