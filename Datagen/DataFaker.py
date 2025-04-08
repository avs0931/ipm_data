from enum import Enum
import shutil
import os
import pandas as pd
import Config.config as Cfg
from App_Helpers.Logger import *
from Datagen.Datagen_issues import *

# FAKER SOURCE:
# https://faker.readthedocs.io/en/stable/locales/ru_RU.html#faker.providers.date_time.ru_RU.Provider.date_between


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
    ld_resource_group_guide = 11       # Resource's Guida.                               Real, local, loadable.
    ld_resource_logistics_guide = 12   # Resource's logistics params.                    Real, local, loadable.
    ld_local_doc_contexts = 13         # Types of document context.                      Fake, local, generated.
    bank_entity = 14                   # Bank offices.                                   Real, local, loadable
    construction_sites = 15             # Construction Sites.                            Fake, cache, generated.
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
        # This is instance-level set of unique local_id's to provide uniform
        # PK/FK relationships during data generation
        self._unique_id_set = list()

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

    def get_item_type(self) -> E_Types:
        """
        :return: E_Type enum - type of instance
        """
        return self._item_type

    def get_item_type_name(self) -> str:
        """
        :return: Name (string value) of E_Type enum - type of instance
        """
        return self._item_type.name

    def has_data(self) -> bool:
        """
        :return: True if data present. Otherwise - False
        """
        return (self._data is not None) and (not self._data.empty)

    def data_len(self) -> int:
        """
        :return: Number of items in data collection
        """
        return len(self._data) if self.has_data() else 0

    def get_data(self) -> pd.DataFrame:
        """
        :return: Data collection for instance if data present. Otherwise - empty data frame.
        """
        return self._data if self.has_data() else pd.DataFrame()

    def set_data(self, data: pd.DataFrame) -> None:
        """
        Set data collection fot instance. This call will cause erasing of current _unique_data_set collection
        :param data:
        :return: None
        """
        if (data is not None) and (len(data) > 0):
            trc_print(f"Data has been successfully sets with '{len(data)}' rows in '{len(data.columns)}' columns")
            self._data = data
            self._unique_id_set.clear()
        else:
            wrn_print(f"There is no valid data to set for data type: '{self.get_item_type_name()}'. Current data leaves unchanged")

    def get_load_path(self) -> str:
        """
        :return: Current path to data file to load/save data collection
        """
        return self._load_path

    def get_unique_id_set(self, set_length: int = 0):
        """
        Create instance-level set of unique local_id's to provide uniform PK/FK relationships during data generation
        :param set_length: number of unique local_id in selection
        :return: set (unique) of local_id's
        """
        assert self.has_data(), f"Can't provide unique id set for type: '{self.get_item_type_name()}' - there is no data"
        if set_length == 0:
            set_length = self.data_len()
        assert set_length <= self.data_len()
        if len(self._unique_id_set) == 0:
            self._unique_id_set = random.sample(list(self.get_data()["local_id"]), set_length)

        return self._unique_id_set
    # end of get_unique_id_set()

    def save_data(self, data_path: str = "") -> str:
        """
        Save current data collection to designated file.
        :param data_path: Path/File to save data collection. Default: value of get_load_path()
        :return:
        """
        if not self._data_path:
            wrn_print(f"Can't save data of type '{self.get_item_type_name()}' to file. Data path not defined")
        elif not self.has_data():
            wrn_print(f"Can't save data of type '{self.get_item_type_name()}' to file. There is no data")
        else:
            if not data_path:
                data_path = self.get_load_path()
            try:
                df = self.get_data()
                df.to_csv(data_path, sep="\t", index=False)
                trc_print(f"Data to type '{self.get_item_type_name()}' has been successfully saved with '{len(df)}' rows in '{len(df.columns)}' columns. File: '{data_path}'")
                return data_path
            except Exception as e:
                err_print(f"Saving data of type '{self.get_item_type_name()}' failed", ex=e)
        return ""
    # end of save_data()
# end of DataItem class


class GenCC:
    """Data generator command center"""
    pass
# end of GenCC class


def data_gen(item_type: E_Types, data_dict: {}) -> pd.DataFrame:
    """
    Generate data collection of given type
    :param item_type: Type of data to be generated
    :param data_dict: Container to store/retrieve typed DataItem instances with generated collection of data.
    :return: Typed data collection if success. Otherwise - empty DataFrame
    """
    assert data_dict is not None, "Data dictionary not provided"

    def _get_item_by_type(d_type: E_Types) -> DataItem:
        if d_type.name not in data_dict.keys():
            err_print(f"Can't retrieve item by key '{d_type.name}'")
            return None
        return data_dict[d_type.name]
    # end of get_item_by_type()

    def _get_data_by_type(d_type: E_Types) -> pd.DataFrame:
        # Local function
        if d_type.name not in data_dict.keys():
            err_print(f"Can't retrieve data by key '{d_type.name}'")
            return pd.DataFrame()
        return data_dict[d_type.name].get_data()
    # end of get_data_by_type()

    pt_data = pd.DataFrame()

    if item_type == E_Types.ld_project_types:
        pt_data = ld_by_key.generate(item_type.name)

    elif item_type == E_Types.ld_project_statuses:
        pt_data = ld_by_key.generate(item_type.name)

    elif item_type == E_Types.ld_project_party_types:
        pt_data = ld_by_key.generate(item_type.name)

    elif item_type == E_Types.ld_contract_types:
        pt_data = ld_by_key.generate(item_type.name)

    elif item_type == E_Types.ld_contract_statuses:
        pt_data = ld_by_key.generate(item_type.name)

    elif item_type == E_Types.ld_contract_party_types:
        pt_data = ld_by_key.generate(item_type.name)

    elif item_type == E_Types.ld_control_check_types:
        pt_data = ld_by_key.generate(item_type.name)

    elif item_type == E_Types.ld_control_violation_types:
        pt_data = ld_by_key.generate(item_type.name)

    elif item_type == E_Types.ld_executive_doc_types:
        pt_data = ld_by_key.generate(item_type.name)

    elif item_type == E_Types.ld_permissive_doc_types:
        pt_data = ld_by_key.generate(item_type.name)

    elif item_type == E_Types.ld_measure_unit_guide:
        wrn_print(f"Data with key: '{item_type.name}' is real and can't be generated")

    elif item_type == E_Types.ld_resource_group_guide:
        wrn_print(f"Data with key: '{item_type.name}' is real and can't be generated")

    elif item_type == E_Types.ld_resource_logistics_guide:
        p_itm = _get_item_by_type(E_Types.ld_resource_group_guide)
        pid_set = p_itm.get_unique_id_set()
        pt_data = ResourceLogisticGuide.generate(group_id_set=pid_set)

    elif item_type == E_Types.ld_local_doc_contexts:
        wrn_print(f"Not implemented: generation for key '{item_type.name}'")

    elif item_type == E_Types.bank_entity:
        wrn_print(f"Data with key: '{item_type.name}' is real and can't be generated")

    elif item_type == E_Types.construction_sites:
        p_itm = _get_item_by_type(E_Types.project_entity)
        if p_itm is not None and p_itm.has_data():
            max_project = 5
            pid_set = p_itm.get_unique_id_set(set_length=max_project)
            pt_data = ConstructionSite.generate(project_id_set=pid_set)
        else:
            err_print(f"Can't create data for :'{item_type.name}'")

    elif item_type == E_Types.contract_entity:
        p_itm = _get_item_by_type(E_Types.project_entity)
        if p_itm is not None and p_itm.has_data():
            max_project = 5
            pid_set = p_itm.get_unique_id_set(set_length=max_project)
            pt_data = ContractEntity.generate(project_id_set=pid_set,
                                              contract_statuses=_get_data_by_type(E_Types.ld_contract_statuses),
                                              contract_types=_get_data_by_type(E_Types.ld_contract_types))
        else:
            err_print(f"Can't create data for :'{item_type.name}'")

    elif item_type == E_Types.contract_parties:
        c_itm = _get_item_by_type(E_Types.contract_entity)
        e_itm = _get_item_by_type(E_Types.contractor_entity)
        if c_itm is not None and c_itm.has_data() \
                and e_itm is not None and e_itm.has_data():
            max_contracts = 5
            max_parties = 50
            cid_set = c_itm.get_unique_id_set(set_length=max_contracts)
            eid_set = e_itm.get_unique_id_set(set_length=max_parties)
            pt_data = ContractParties.generate(contract_id_set=cid_set, contractor_id_set=eid_set,
                                               contract_party_types=_get_data_by_type(E_Types.ld_contract_party_types))
        else:
            err_print(f"Can't create data for :'{item_type.name}'")

    elif item_type == E_Types.contractor_entity:
        pt_data = ContractorEntity.generate(bank_data=_get_data_by_type(E_Types.bank_entity))

    elif item_type == E_Types.contractor_personnel:
        e_itm = _get_item_by_type(E_Types.contractor_entity)
        if e_itm is not None and e_itm.has_data():
            max_parties = 50
            eid_set = e_itm.get_unique_id_set(set_length=max_parties)
            pt_data = ContractorPersonnel.generate(contractor_id_set=eid_set,
                                                   personality_data=_get_data_by_type(E_Types.personality_entity))
        else:
            err_print(f"Can't create data for :'{item_type.name}'")

    elif item_type == E_Types.document_base:
        wrn_print(f"Not implemented: generation for key '{item_type.name}'")

    elif item_type == E_Types.personality_entity:
        pt_data = PersonalityEntity.generate()

    elif item_type == E_Types.project_entity:
        pt_data = ProjectEntity.generate(statuses=_get_data_by_type(E_Types.ld_project_statuses),
                                         types=_get_data_by_type(E_Types.ld_project_types))

    elif item_type == E_Types.project_parties:
        p_itm = _get_item_by_type(E_Types.project_entity)
        e_itm = _get_item_by_type(E_Types.contractor_entity)
        if p_itm is not None and p_itm.has_data() \
            and e_itm is not None and e_itm.has_data():
            max_project = 5
            max_parties = 50
            pid_set = p_itm.get_unique_id_set(set_length=max_project)
            eid_set = e_itm.get_unique_id_set(set_length=max_parties)
            pt_data = ProjectParties.generate(project_id_set=pid_set,
                                              contractor_id_set=eid_set,
                                              project_party_types=_get_data_by_type(E_Types.ld_project_party_types))
        else:
            err_print(f"Can't create data for :'{item_type.name}'")

    else:
        err_print(f"Data generator for key '{item_type.name}' not found")

    return pt_data
# end of data_gen()


def restore_real(real_data_path: str, generation_path: str) -> int:
    """Copy the 'real_data' files to directory contains all generated data
    :param: real_path to directory contains real-data csv files (i.e. copy-from)
    :param: generation_path: path  to directory contains generated csv-files (i.e. copy-to)
    :return: Number of flies restored
    """
    assert os.path.exists(real_data_path), f"Path to real csv-data files not found: '{real_data_path}'"
    assert os.path.exists(generation_path), f"Path to generating csv-data files not found: '{generation_path}'"
    r_files = [f for f in os.listdir(real_data_path) if f.endswith("csv")]
    for f in r_files:
        r = os.path.join(real_data_path, f)
        if os.path.isfile(r):
            shutil.copy(r, generation_path)

    return len(r_files)
# end of restore_real()


def data_init():
    data_dict = {}

    # Set-up paths
    gen_path = Cfg.DG_generation_data_path
    real_data_path = os.path.join(gen_path, "real_data")

    trc_print(f"Restoring real data first from '{real_data_path}'")
    rf = restore_real(real_data_path=real_data_path, generation_path=gen_path)
    trc_print(f"{rf} real data files has been placed into '{gen_path}' directory\n")

    for i in range(3):
        trc_print("********************************************************************************************")
        trc_print("*")
        trc_print(f"* DATA GENERATION PHASE {i}")
        trc_print("*")
        trc_print("********************************************************************************************")

        for e_type in E_Types:
            if e_type.name in data_dict:
                continue

            trc_print("*****")
            trc_print(f"***** '{e_type.name:<32}' is running --------------------------------------------------")
            try:
                dit = DataItem(item_type=e_type, data_path=gen_path)
                if not dit.has_data():
                    trc_print(f"Try to generate data for type: '{e_type}'")
                    item_data = data_gen(item_type=e_type, data_dict=data_dict)
                    dit.set_data(item_data)
                    dit.save_data()

                if dit.has_data():
                    data_dict[e_type.name] = dit
            except Exception as e:
                err_print(f"DataItem of type '{e_type.name}' failed.", ex=e)
# end of data_init()


if __name__ == '__main__':
    # di = DataItem(item_type=E_Types.bank_entity, data_path=Cfg.DG_generation_data_path)
    data_init()
