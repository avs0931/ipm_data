from enum import Enum
import os
import pandas as pd
import Config.config as Cfg
from App_Helpers.Logger import *
from Datagen.Datagen_issues import *

# Here - Main logic and routines to provide data generation
# and prepare files to DB Loading.
# Steps:
# =====================================================================
# 0. - Create 'command center' structure to store and manage generating data.
# =====================================================================
# 1. Perform generating:
# 1.1. Generate all 'independent' data, i.e. local dictionaries (statuses, types, etc.)
# 1.2. Load all non-fake data from file(s):
#      - bank_entity
# 1.3. Generate non-dependency data:
#      - contractor_entity
#      - personnel_entity
# 1.4. Generate one_level-dependency data (based on prev. generated):
#      - contractor_personnel
# 1.5. Generate projects
# 1.6. Generate project-parties and apply its to generated set of projects
# 1.7. Generate construction-sites and apply its to generated set of projects
# 1.8. Generate contracts and apply its to generated set of projects
# 1.9. Generate contract-parties and apply its to generated set of contracts
# --- Generation done
# =====================================================================
# 2. Perform data saving:
# 2.1. Save each data set to a file (tab-separated format)
# --- Data saving done
# =====================================================================
# 3. Perform data loading:
# 3.1. Check data loading conditions:
# 3.1.1. Re-create data schema from prepared template
#   or
# 3.1.2. Truncate all/some data to eliminate possible mistakes
#   and then
# 3.2. Loads data from prepared files into test-DB in accordance with creating order
# --- All done!
# =====================================================================
# Notices:
# --------
# For Chapter 1: following the rules:
#   - Check previously saved data. If exists - load it for further use
#  or
#   - Check data presence in 'command center' If present - use it
#  or
#   - Generate fresh new data set for each option listed.
# =====================================================================


class E_Types(Enum):
    ld_project_types = 0               # Types of project.                               Fake, local, generated.
    ld_project_statuses = 1            # Statuses of projects.                           Fake, local, generated.
    ld_project_party_types = 2         # Types of project's participants.                Fake, local, generated.
    ld_contract_types = 3              # Types of contract.                              Fake, local, generated.
    ld_contract_statuses = 4           # Statuses of contract.                           Fake, local, generated.
    ld_contract_party_types = 5        # Types of contract's participants.               Fake, local, generated.
    ld_control_check_types = 6         # Types of tech. check's routines.                Fake, local, generated.
    ld_control_violation_types = 7     # Types of tech. violations.                      Fake, local, generated.
    ld_executive_doc_types = 8         # Types of executive docs.                        Fake, local, generated.
    ld_permissive_doc_types = 9        # Types of permissive docs.                       Fake, local, generated.
    ld_measure_unit_guide = 10         # Measure Unit Guide.                             Real, local, loadable.
    ld_resource_group_guide = 11       # Resource's Guida.                               Fake, local, generated.
    ld_resource_logistics_guide = 12   # Resource's logistics params.                    Fake, local, generated.
    ld_local_doc_contexts = 13         # Types of document context.                      Fake, local, generated.
    bank_entity = 14                   # Bank offices.                                   Real, local, loadable
    construction_site = 15             # Construction Sites.                             Fake, cache, generated.
    contract_entity = 16               # Contracts Guide.                                Fake, cache, generated.
    contract_parties = 17              # Contract participants (Contract/Contractors).   Fake, cache, generated.
    contractor_entity = 18             # Contractors / Companies.                        Fake, cache, generated.
    contractor_personnel = 19          # Employees (Contractor/Personality).             Fake, cache, generated.
    document_base = 20                 # Basics meta for "document".                     Real, meta, not used.
    personality_entity = 21            # Persons (free of context).                      Fake, cache, generated.
    project_entity = 22                # Projects.                                       Fake, cache, generated.
    project_parties = 23               # Project's participants (Project / Contractors). Fake, cache, generated.

# usage:
# print(E_Types.ld_contract_party_types)
# print(E_Types(15))
# print(E_Types.ld_measure_unit_guide.name)
# print(E_Types.ld_measure_unit_guide.value)

# end of E_Types enumeration class


class DataItem:
    def __init__(self, item_type: E_Types, data_path: str):
        self._item_type: E_Types = item_type
        self._load_path: str = os.path.join(data_path, self._item_type.name + ".csv")
        self._data_path: str = self._load_path
        self._data: pd.DataFrame = self._load(self._load_path)

    def _load(self, load_path: str) -> pd.DataFrame:
        trc_print(f"Try to load data of type: '{self._item_type.name}' form: '{self._load_path}'")
        try:
            df = pd.read_csv(load_path, sep="\t", encoding="utf-8")
            if not df.empty:
                trc_print(f"Data has been successfully loaded with '{len(df)}' rows in '{len(df.columns)}' columns")
            return df
        except FileNotFoundError as e:
            err_print(err_string=f"File not found: '{load_path}'", ex=e)
        except Exception as e:
            err_print(err_string="Exception raised", ex=e)

        return pd.DataFrame()
    # end of _load()

    def get_item_type(self):
        return self._item_type.name

    def has_data(self):
        return (self._data is not None) and (not self._data.empty)

    def data_len(self):
        return len(self._data) if self.has_data() else 0

    def get_data(self):
        return self._data if self.has_data() else pd.DataFrame()

    def set_data(self, data: pd.DataFrame):
        if (data is not None) and (len(data) > 0):
            trc_print(f"Data has been successfully sets with '{len(data)}' rows in '{len(data.columns)}' columns")
            self._data = data
        else:
            wrn_print(f"There is no valid data to set for data type: '{self.get_item_type()}'. Current data leaves unchanged")

    def get_load_path(self):
        return self._load_path

    def save_data(self):
        if not self._data_path:
            wrn_print(f"Can't save data of type '{self.get_item_type()}' to file. Data path not defined")
        elif not self.has_data():
            wrn_print(f"Can't save data of type '{self.get_item_type()}' to file. There is no data")
        else:
            try:
                df = self.get_data()
                path = self.get_load_path()
                df.to_csv(path, sep="\t", index=False)
                trc_print(f"Data to type '{self.get_item_type()}' has been successfully saved with '{len(df)}' rows in '{len(df.columns)}' columns. File: '{path}'")
            except Exception as e:
                err_print(f"Saving data of type '{self.get_item_type()}' failed", ex=e)
    # end of save_data()
# end of DataItem class

class GenCC:
    """Data generator command center"""
    pass
# end of GenCC class


def data_init():
    for etype in E_Types:
        try:
            dit = DataItem(item_type=etype, data_path=Cfg.DG_generation_data_path)
            if not dit.has_data():
                trc_print(f"Try to generate data for type: '{etype}' via 'ld_by_key()' generator")
                dit.set_data(ld_by_key.generate(resource_key=etype.name))
                dit.save_data()
        except Exception as e:
            err_print(f"DataItem of type '{etype.name}' failed.", ex=e)
# end of data_init()


if __name__ == '__main__':
    # di = DataItem(item_type=E_Types.bank_entity, data_path=Cfg.DG_generation_data_path)
    data_init()
