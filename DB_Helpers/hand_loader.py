import os
import pandas as pd
import Config.config as Cfg
from App_Helpers.Logger import *
from DB_Helpers.pg_helper import *

# Test cases - load data to db with scripts in hand-mode

# ch_bank_entity.sql
# ch_construction_sites.sql
# ch_contractor_entity.sql
# ch_contractor_personnel.sql
# ch_contract_entity.sql
# ch_contract_parties.sql
# ch_document_base.sql
# ch_ld_contract_party_types.sql
# ch_ld_contract_statuses.sql
# ch_ld_contract_types.sql
# ch_ld_control_check_types.sql
# ch_ld_control_violation_types.sql
# ch_ld_executive_doc_types.sql
# ch_ld_local_doc_contexts.sql
# ch_ld_measure_unit_guide.sql
# ch_ld_permissive_doc_types.sql
# ch_ld_project_party_types.sql
# ch_ld_project_statuses.sql
# ch_ld_project_types.sql
# ch_ld_resource_group_guide.sql
# ch_ld_resource_logistics_guide.sql
# ch_personality_entity.sql
# ch_project_entity.sql
# ch_project_parties.sql
# ch_templates.sql
script_list = [
    "ch_bank_entity.sql",
    "ch_contractor_entity.sql",
    "ch_personality_entity.sql",
    "ch_contractor_personnel.sql",
    "ch_ld_project_types.sql",
    "ch_ld_project_statuses.sql",
    "ch_ld_project_party_types.sql",
    "ch_project_entity.sql",
    "ch_project_parties.sql",
    "ch_construction_sites.sql",
    "ch_ld_contract_types.sql",
    "ch_ld_contract_statuses.sql",
    "ch_ld_contract_party_types.sql",
    "ch_contract_entity.sql",
    "ch_contract_parties.sql",
    "ch_ld_control_check_types.sql",
    "ch_ld_control_violation_types.sql",
    "ch_ld_executive_doc_types.sql",
    "ch_ld_permissive_doc_types.sql",
    "ch_ld_measure_unit_guide.sql",
    "ch_ld_resource_group_guide.sql",
    "ch_ld_resource_logistics_guide.sql",
    # Not rady to load:
    # "ch_document_base.sql",
    # "ch_ld_local_doc_contexts.sql",
]

# List of scripts in execution order


def do_load(da_name: str, sql_dir: str, script_set: [], ignore_errors: bool = False):
    assert db_name, "No DB Name"
    assert os.path.exists(sql_dir), f"Path to scripts not found: '{sql_dir}'"

    for _, sc in enumerate(script_set):
        tn = sc.split(".")[0]
        sf = os.path.join(sql_dir, sc)
        # Perform data load
        try:
            trc_print(f"\nSequence # {_:>3}")
            trc_print("* ********************************************************")
            trc_print(f"Try to load data to table: '{tn:<32}' from file: '{sf}'")
            execute_f(db_name=db_name, sql_file=sf)

            # Perform load check
            chk_stmt = f"select count(*) from {tn};"
            res = execute_one(db_name=db_name, sql_stmt=chk_stmt)
            trc_print(f"Data has been loaded with '{res[0]}' rows")
        except Exception as e:
            err_print("Data loading failed", ex=e)
            if not ignore_errors:
                break
# end of do_load()


if __name__ == "__main__":
    # Go for it!
    trc_print("Run hand loader!")

    # Direct path to scripts
    script_dir = r"C:\Users\Public\PGData\test\ch\sql"
    # scripts = [f for f in os.listdir(script_dir) if os.path.isfile(os.path.join(script_dir, f))]
    # print('",\n'.join(list(scripts)))

    # Hand list of scripts:
    # sf = "ch_bank_entity.sql"
    # sf = "ch_contractor_entity.sql"
    # sf = "ch_personality_entity.sql"
    # sf = "ch_contractor_personnel.sql"
    # sf = "ch_ld_project_types.sql"
    # sf = "ch_ld_project_statuses.sql"
    # sf = "ch_ld_project_party_types.sql"
    # sf = "ch_project_entity.sql"
    # sf = "ch_project_parties.sql"
    # sf = "ch_construction_sites.sql"
    # sf = "ch_ld_contract_types.sql"
    # sf = "ch_ld_contract_statuses.sql"
    # sf = "ch_ld_contract_party_types.sql"
    # sf = "ch_contract_entity.sql"
    # sf = "ch_contract_parties.sql"
    # sf = "ch_ld_control_check_types.sql"
    # sf = "ch_ld_control_violation_types.sql"
    # sf = "ch_ld_executive_doc_types.sql"
    # sf = "ch_ld_permissive_doc_types.sql"

    db_name = "test"
    do_load(da_name=db_name, sql_dir=script_dir, script_set=script_list)

