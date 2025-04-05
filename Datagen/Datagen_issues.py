from typing import Any
import datetime
from Datagen.Integration_issues import integration_issue
import uuid
import pandas as pd
import random
from faker import Faker


# Create instance of _faker
_faker = Faker("ru-RU")

# ########################################


class FakeBase():
    # FakeBase - define some basics for all data generators
    def __init__(self):
        self._ii = integration_issue()
        self.fd = dict()
        self.fd["local_id"]: Any = uuid.uuid4()

    def get_id(self) -> uuid:
        return self.fd["local_id"]

    def get_headers(self, ii_include: bool = True):
        output = list(self.fd.keys())
        if ii_include:
            output.append(self._ii.get_headers())
        return "\t".join(output)

    def get_data(self):
        return self.fd

    def get_csv(self, ii_include: bool = True):
        output = list(map(lambda x: str(x), self.fd.values()))
        if ii_include:
            output.append(self._ii.get_csv())
        return "\t".join(output)
# end of FakeBase class


# ########################################

class ld_by_key(FakeBase):
    """ Provide unified interface to generate common local data dictionary
    """
    # Applicable string values by key
    _sv = {
        "ld_project_types": ["Строительство", "Проектирование", "Прочее"],
        "ld_project_statuses": ["Создан", "Выполняется", "Завершен", "Приостановлен", "Отменен"],
        "ld_project_party_types": ["Заказчик", "Генеральный подрядчик", "Технический заказчик", "СК Заказчика",
                                   "СК Подрядчика", "Агент", "Авторский надзор", "Надрозный орган"],
        "ld_contract_types": ["СМР", "ПНР", "Агентский", "СК Заказчика", "СК Подрядчика", "Авторский надзор",
                              "Государственный надзор"],
        "ld_contract_statuses": ["Создан", "На согласовании", "Согласован", "На выполнении", "Завершен", "Оплачен",
                                 "Приостановлен", "Отменен"],
        "ld_contract_party_types": ["Заказчик", "Генеральный подрядчик", "Технический заказчик", "СК Заказчика",
                                    "СК Подрядчика", "Агент", "Авторский надзор", "Надзорный орган"],
        "ld_control_check_types": ["Проверка 1", "Проверка 2", "Проверка 3", "Првоерка 4"],
        "ld_control_violation_types": ["Нарушение 1", "Нарушение 2", "Нарушение 3", "Нарушение 4"],
        "ld_executive_doc_types": ["ИД тип 1", "ИД тип 2", "ИД тип 3", "ИД тип 4"],
        "ld_permissive_doc_types": ["РД тип 1", "РД тип 2", "РД тип 3", "РД тип 4"],
    }
    # Applicable money values by key
    _mv = {
        "ld_project_types": [],
        "ld_project_statuses": [],
        "ld_project_party_types": [],
        "ld_contract_types": [],
        "ld_contract_statuses": [],
        "ld_contract_party_types": [],
        "ld_control_check_types": [],
        "ld_control_violation_types": [],
        "ld_executive_doc_types": [],
        "ld_permissive_doc_types": [],
    }
    # Applicable number values by key
    _nv = {
        "ld_project_types": [],
        "ld_project_statuses": [],
        "ld_project_party_types": [],
        "ld_contract_types": [],
        "ld_contract_statuses": [],
        "ld_contract_party_types": [],
        "ld_control_check_types": [],
        "ld_control_violation_types": [],
        "ld_executive_doc_types": [],
        "ld_permissive_doc_types": [],
    }

    def __init__(self, resource_key: str, s_value: str = "", m_value: int = 0, n_value: int = 0):
        super().__init__()

        assert resource_key in ld_by_key._sv \
               and resource_key in ld_by_key._mv \
               and resource_key in ld_by_key._nv, \
            f"Invalid key for common resource provider. Not found or missed: '{resource_key}'"
        self.rk = resource_key
        self.fd["dict_string_value"] = s_value
        self.fd["dict_money_value"] = m_value
        self.fd["dict_number_value"] = n_value
        self.fd["is_active"] = 'true'

    def __str__(self):
        return f'{self.rk}: {self.fd["dict_string_value"]} (sting) / {self.fd["dict_money_value"]} (money) / {self.fd["dict_number_value"]} (number)'

    @staticmethod
    def generate(resource_key: str) -> pd.DataFrame:
        """ Generate common (PK/FK relation free) data sets designated by its key.
        :param resource_key: Key of common resource to be generated
        :return: pandas.DataFrame filled with fresh new data
        in accordance with data-schema as it can be loaded from production DB
        """
        assert resource_key in ld_by_key._sv \
               and resource_key in ld_by_key._mv \
               and resource_key in ld_by_key._nv, \
            f"Invalid key for common resource provider. Not found or missed: '{resource_key}'"

        maxlen = max(
            [len(ld_by_key._sv[resource_key]), len(ld_by_key._mv[resource_key]), len(ld_by_key._nv[resource_key])])
        c = list()
        for i in range(maxlen):
            sv = ld_by_key._sv[resource_key][i] if i < len(ld_by_key._sv[resource_key]) else ""
            mv = ld_by_key._mv[resource_key][i] if i < len(ld_by_key._mv[resource_key]) else ""
            nv = ld_by_key._nv[resource_key][i] if i < len(ld_by_key._nv[resource_key]) else ""

            p = ld_by_key(resource_key=resource_key, s_value=sv, m_value=mv, n_value=nv)

            c.append(p.get_data())

        return pd.DataFrame(c)
# end of ld_by_key class


# ########################################


class PersonalityEntity(FakeBase):
    """
    Represents fake for single independent persona dosn't belonged to any employer
    """
    # PersonalityEntity
    _mf_ratio = 0.8  # male (80%) / female(20%) ratio generation

    def __init__(self):
        super().__init__()
        if random.random() < self._mf_ratio:
            self.fd["gender"] = "муж"
            self.fd["last_name"] = _faker.last_name_male()
            self.fd["first_name"] = _faker.first_name_male()
            self.fd["second_name"] = _faker.middle_name_male()
        else:
            self.fd["gender"] = "жен"
            self.fd["last_name"] = _faker.last_name_female()
            self.fd["first_name"] = _faker.first_name_female()
            self.fd["ssecond_name"] = _faker.middle_name_female()

        self.fd["birth_date"] = _faker.date_between(start_date=datetime.date(1970, 1, 1),
                                                    end_date=datetime.date(2000, 12, 31))

    def __str__(self):
        return f'{self.fd["last_name"]}: {self.fd["local_id"]}'

    @staticmethod
    def generate(maxval: int = 100) -> pd.DataFrame:
        """ Generate common (PK/FK relation free) data set about personas.
        :param maxval: Number of items to be generated
        :return: pandas.DataFrame filled with fresh new data
        in accordance with data-schema as it can be loaded from production DB
        """

        c = list()
        for i in range(maxval):
            p = PersonalityEntity()
            c.append(p.get_data())

        return pd.DataFrame(c)
# end of PersonalityEntity class


# ########################################


class ContractorEntity(FakeBase):
    """
    Represents fake for single independent company. There is no any relations with project/contract
    Pre-requisites: We need to take [local_id] from bank's data set to create FK-reference to an appropriate bank.
    """
    def __init__(self, bank_data: pd.DataFrame):
        """
        Create fresh new single instance of company
        :param bank_data: pandas.DataFrame contains bank's data set. One of these will be randomly selected as
        company's bank office service.
        """
        super().__init__()
        # Seed bank-related data
        bank_idx = random.randint(0, len(bank_data) - 1)
        bank_id = str(bank_data.at[bank_idx, "local_id"])
        bank_city = str(bank_data.at[bank_idx, "city"])
        # Gor for enitity
        self.fd["ru_legal_form"] = _faker.company_prefix()
        self.fd["ru_full_name"] = _faker.bs()
        self.fd["ru_short_name"] = self.fd["ru_full_name"]
        self.fd["residence"] = 'RUS'
        self.fd["inn"] = _faker.businesses_inn()
        self.fd["cpp"] = _faker.kpp()
        self.fd["ogrn"] = _faker.businesses_ogrn()
        self.fd["city"] = bank_city
        self.fd["legal_address"] = ", ".join([self.fd["city"], _faker.street_address()])
        self.fd["actual_address"] = ", ".join([self.fd["city"], _faker.street_address()]) \
            if random.random() < 0.5 else self.fd["legal_address"]
        self.fd["en_full_name"] = ""
        self.fd["oktmo_issues"] = "{1,2,3}"
        self.fd["okved_issues"] = "{4,5,6}"
        self.fd["bank_entity_id"] = bank_id
        self.fd["rub_account_number"] = _faker.checking_account()
        self.fd["intl_account_number"] = ""
        self.fd["intl_account_currency_code"] = ""
        if random.random() < 0.5:
            self.fd["intl_account_number"] = _faker.iban()
            self.fd["intl_account_currency_code"] = "EUR" if random.random() < 0.7 else "USD"

    def __str__(self):
        return (f'{self.fd["ru_full_name"]} / {self.fd["city"]} / inn: {self.fd["inn"]}: {self.fd["local_id"]}')

    @staticmethod
    def generate(bank_data: pd.DataFrame, max_companies: int = 100) -> pd.DataFrame:
        """
        Generate fresh new data set about companies in relation with bank office
        :param bank_data: pandas.DataFrame with a Bank's related data set
        :param max_companies: Number of companies to be generated (default is 100).
        :return: pandas.DataFrame filled with fresh new data
        in accordance with data-schema as it can be loaded from production DB
        """
        c = list()
        for i in range(max_companies):
            p = ContractorEntity(bank_data=bank_data)
            c.append(p.get_data())

        return pd.DataFrame(c)
# end of ContractorEntity class


# ########################################


class ContractorPersonnel(FakeBase):
    """
    Represent employee as randomly selected person from [personality_entity] in relation with given employer.
    Pre-Requisites: We need to take [local_id] from ContractorEntity to create FK-reference to an appropriate contractor
    """
    def __init__(self, contractor_id: uuid, personal_data: pd.DataFrame):
        """
        Create fresh new single instance of company
        :param contractor_id: id of company that new employee belongs to.
        :param personal_data: pandas.DataFrame contains personality's data set. One of these will be randomly selected
        as and employee
        """
        super().__init__()
        # Seed perssoned-related data - we will use it as employee id
        idx = random.randint(0, len(personal_data) - 1)
        emp_id = str(personal_data.at[idx, "local_id"])
        self.fd["contractor_id"] = contractor_id
        self.fd["personnel_id"] = emp_id
        self.fd["contacts"] = _faker.phone_number() if random.random() < 0.5 else _faker.email()
        self.fd["hire_date"] = _faker.date_between(start_date=datetime.date(2005, 1, 1),
                                                   end_date=datetime.date(2025, 3, 31))
        self.fd["fire_date"] = _faker.date_between(start_date=datetime.date(2005, 1, 1),
                                                   end_date=datetime.date(2025, 3, 31)) \
            if random.random() < 0.01 else ""
        self.fd["valid_to"] = ""
        self.fd["positions"] = "Прораб"
        self.fd["can_sign"] = "{Акт, Договор, Протокол, Авансовый отчет}"
        self.fd["position_name"] = "Инженер"
        self.fd["confirmation_document"] = "Диплом"
        self.fd["expiration_date"] = ""

    def __str__(self):
        return f'{self.fd["contractor_id"]} / {self.fd["personnel_id"]}: {self.fd["positions"]} - {self.fd["contacts"]} ({self.fd["can_sign"]}")'

    @staticmethod
    def generate(contractor_data: pd.DataFrame, personality_data: pd.DataFrame, max_companies: int = 1,
                 max_employees: int = 10) -> pd.DataFrame:
        """
        Generate fresh new data sets about employer / employees
        :param contractor_data: pandas.DataFrame for randomly selection of company (employer)
        :param personality_data: padnas.DataFrame for randomly selection of person as new employee
        :param max_companies: Number of companies to processing
        :param max_employees: Number of employees that will be added to each using company
        :return: pandas.DataFrame filled with fresh new data
        in accordance with data-schema as it can be loaded from production DB
        """
        assert max_companies >= 1, "Invalid contractor max value"
        assert max_companies < 100, "Contractor max too big"
        assert len(contractor_data) >= max_companies, "Contractor data is empty or too short"
        assert max_employees >= 1, "Invalid personnel max value"
        assert max_employees <= 1000, "Personnel max too big"
        assert len(personality_data) > max_employees, "Personnel data too short"

        # Take set of contracotrs
        cid_set = set()
        while len(cid_set) < max_companies:
            idx = random.randint(0, len(contractor_data) - 1)
            cid_set.add(contractor_data.at[idx, "local_id"])

        # Gor for data
        c = list()
        for cid in cid_set:
            for i in range(max_employees):
                p = ContractorPersonnel(contractor_id=cid, personal_data=personality_data)
                c.append(p.get_data())

        return pd.DataFrame(c)
# end of ContractorPersonnel class


# ########################################


class ProjectEntity(FakeBase):
    """
    Represent a single project (in context of IPM).
    Note! Project instance will be created without any reference to any kind of participants!
    """
    def __init__(self, statuses: pd.DataFrame, types: pd.DataFrame):
        """
        Create fresh new single instance of project
        with randomly selected type (of project) and current status (of project)
        :param statuses: pandas.DataFrame contains current set of allowed statuses for any project's instance
        :param types: pandas.DataFrame contains current set of allowed types for any project's instance
        """
        super().__init__()
        self.fd[
            "external_code"] = f"Внешний код проекта: {random.randint(0, 1500)} / {_faker.date_between(start_date=datetime.date(2024, 1, 1), end_date=datetime.date(2025, 2, 28))}"
        self.fd["project_name"] = f"Газопровод {_faker.city()} - {_faker.city()}"
        self.fd["project_description"] = "Описание проекта в свободной форме"
        self.fd["project_type"] = types.at[random.randint(0, len(types) - 1), "local_id"]
        self.fd["project_status"] = statuses.at[random.randint(0, len(statuses) - 1), "local_id"]
        self.fd["project_start"] = _faker.date_between(start_date=datetime.date(2024, 1, 1),
                                                       end_date=datetime.date.today())
        self.fd["project_finish"] = _faker.date_between(start_date=datetime.date(2024, 1, 1),
                                                        end_date=datetime.date.today()) if random.random() < 0.05 else ""
        self.fd["project_started_at"] = _faker.date_between(start_date=datetime.date(2024, 1, 1),
                                                            end_date=datetime.date.today()) if random.random() < 0.6 else ""

    def __str__(self):
        return f'project entity: {self.fd["external_code"]} / {self.fd["project_name"]} - {self.fd["project_type"]}, status: {self.fd["project_status"]}'

    @staticmethod
    def generate(statuses: pd.DataFrame, types: pd.DataFrame, max_projects: int = 10):
        """
        Generate fresh new data set of projects.
        :param statuses: pandas.DataFrame for randomly selection of project's statuses
        :param types:pandas.DataFrame for randomly selection of project's types
        :param max_projects: number of projects to be generated
        :return: pandas.DataFrame filled with fresh new data
        in accordance with data-schema as it can be loaded from production DB
        """
        assert max_projects > 1, "Invalid maxval!"
        assert max_projects < 100, "Maxval too big!"
        c = list()
        for i in range(max_projects):
            p = ProjectEntity(statuses=statuses, types=types)
            c.append(p.get_data())

        return pd.DataFrame(c)
# end of ProjectEntity class


# ########################################


class ProjectParties(FakeBase):
    """
    Represent any single party (participant) of project.
    Note: current _faker can provide from 2 (two) to 5 (five) project parties for each project's instance
    """
    def __init__(self, project_id: uuid, contractor_data: pd.DataFrame, project_party_types: pd.DataFrame):
        """
        Create single party (participant) of project
        :param project_id: id of project's instance that this participant will belong to
        :param contractor_data: pandas.DataFrame for randomly selection as project's party
        :param project_party_types: pandas.DataFrame contains allowed types of Contractor for randomly selection.
        """
        super().__init__()
        self.fd["project_id"] = project_id
        self.fd["contractor_id"] = contractor_data.at[random.randint(0, len(contractor_data) - 1), "local_id"]
        self.fd["party_type"] = project_party_types.at[random.randint(0, len(project_party_types) - 1), "local_id"]
        self.fd["role_description"] = f"Проектная роль № {random.randint(0, 15)}"
        self.fd["activated_at"] = _faker.date_between(start_date=datetime.date(2024, 1, 1),
                                                      end_date=datetime.date.today()) if random.random() < 0.8 else ""
        self.fd["suspended_at"] = _faker.date_between(start_date=self.fd["activated_at"],
                                                      end_date=datetime.date.today()) if self.fd["activated_at"] and \
                                                                                        self.fd[
                                                                                            "activated_at"] < datetime.date.today() and random.random() < 0.05 else ""
        self.fd["resumed_at"] = _faker.date_between(start_date=self.fd["suspended_at"],
                                                    end_date=datetime.date.today()) if self.fd["suspended_at"] and \
                                                                                      self.fd[
                                                                                          "suspended_at"] < datetime.date.today() and random.random() < 0.05 else ""
        self.fd["is_active"] = True if self.fd["activated_at"] and not self.fd["suspended_at"] else False

    def __str__(self):
        return f'project party: {self.fd["project_id"]} / {self.fd["contractor_id"]} - {self.fd["party_type"]} role: {self.fd["role_description"]}'

    @staticmethod
    def generate(project_data: pd.DataFrame, contractors: pd.DataFrame, project_party_types: pd.DataFrame,
                 max_projects: int = 10) -> pd.DataFrame:
        """
        Generate fresh new set of project/participants relations.
        :param project_data: pandas.DataFrame represents current set of projects for randomly selection
        to establish project/participants relationship
        :param contractors: pandas.DataFrame represents current set of contractor entities for randomly selection
        as project's participant
        :param project_party_types: pandas.DataFrame represents type of participation for selected participant.
        :param max_projects: Number of projects to be processed.
        :return: pandas.DataFrame filled with fresh new data
        in accordance with data-schema as it can be loaded from production DB
        """
        assert max_projects > 0, "Invalid max_project LE ZERO"
        assert max_projects < 100, "Max_project too big"
        assert len(project_data) > max_projects * 2, "Project data too short"
        assert len(contractors) > max_projects * 2, "Contractor's set too short"
        assert len(project_party_types) > 0, "project_party_types is empty"

        p_list = set()
        while len(p_list) < max_projects:
            idx = random.randint(0, len(project_data) - 1)
            p_list.add(project_data.at[idx, "local_id"])

        c = list()
        for p_id in p_list:
            # Number of contract parties is at least 2 and max of 4
            for _ in range(random.randint(2, 5)):
                p = ProjectParties(project_id=p_id, contractor_data=contractors,
                                   project_party_types=project_party_types)
                c.append(p.get_data())

        return pd.DataFrame(c)
# end of ProjectParties class


# ########################################


class ConstructionSite(FakeBase):
    """
    Represent any single indivisible construction object (in context of IPM).
    Two or more [ConstructionSites] can form a hierarchy of constructing objects at project.
    Each project should contain at least one ConstructionObject and such object will be 'primary' or 'main' and
    'root' in object's hierarchy.
    """
    def __init__(self, project_id: uuid, is_primary: bool = False, parent_id: uuid = ""):
        """
        Create fresh new construction object that belongs to given project.
        :param project_id: id of project
        :param is_primary: True if created contraction object is 'main' or 'primary' for project
        :param parent_id: reference to the 'primary' construction object (i.e. item of hierarchy of objects)
        """
        super().__init__()
        object_no = f"{random.randint(0, 1500)} / {_faker.date_between(start_date=datetime.date(2024, 1, 1), end_date=datetime.date(2025, 2, 28))}"
        self.fd["external_code"] = f"Внешний код объекта: {object_no}"
        self.fd["entity_description"] = f"Описание объекта: {object_no}"
        self.fd["project_entity_id"] = project_id
        self.fd["is_primary"] = is_primary
        self.fd["primary_construction_id"] = parent_id if not is_primary else ""

    def __str__(self):
        return f'construction site: {self.fd["external_code"]} / {self.fd["entity_description"]} - {self.fd["is_primary"]} parent: {self.fd["primary_construction_id"]}'

    @staticmethod
    def generate(project_id: uuid, max_objects: int = 10) -> pd.DataFrame:
        """
        Generate number of construction objects and assign it to the given project
        :param project_id: id of project
        :param max_objects: Number of objects to be created (this is vary by random)
        :return: pandas.DataFrame filled with fresh new data
        in accordance with data-schema as it can be loaded from production DB
        """
        assert max_objects > 1, "Invalid max_objects LE ONE"
        assert max_objects < 100, "Max_project too big"

        c = list()
        c_id: uuid = None
        for _, cc in enumerate(range(random.randint(1, max_objects))):
            if _ == 0:
                # Make first site primary
                p = ConstructionSite(project_id=project_id, is_primary=True)
                c_id = p.get_id()
            else:
                p = ConstructionSite(project_id=project_id, is_primary=False, parent_id=c_id)

            c.append(p.get_data())

        return pd.DataFrame(c)
# end of ConstructionSite class


# ########################################


class ContractEntity(FakeBase):
    """
    Represent a single contract (in context of IPM).
    Note! Contract instance will be created without any reference to any kind of participants!
    Note! Contract always assigned to project. Contract 'as is' without this assignation doesn't allowed!
    """
    def __init__(self, project_id: uuid, contract_status: uuid = "", contract_type: uuid = "", parent_id: uuid = ""):
        """
        Create fresh new instance of contract in relation with given project. Any contract can contain an amendment or
        extension or additional contract.
        :param project_id: id of project that this contract will assign.
        :param contract_status: id of allowed contract_status for this contract
        :param contract_type: id of allowed contract_type for this contract
        :param parent_id: id of 'parent' contract (i.e. contract that the creating contract will expand of amend)
        """
        super().__init__()
        object_no = f"{random.randint(0, 1500)} / {_faker.date_between(start_date=datetime.date(2024, 1, 1), end_date=datetime.date(2025, 2, 28))}"
        self.fd["project_id"] = project_id
        self.fd["contract_number"] = object_no
        self.fd["contract_date"] = _faker.date_between(start_date=datetime.date(2024, 1, 1),
                                                       end_date=datetime.date.today()) if random.random() < 0.8 else ""
        self.fd["contract_subject"] = f'{random.choice(["СМР", "ПИР", "ПНР"])} по объекту {object_no}'
        self.fd["contract_value"] = 1_000_000_000 * random.random()
        self.fd["contract_status"] = contract_status
        self.fd["status_changed_at"] = _faker.date_between(start_date=self.fd["contract_date"],
                                                           end_date=datetime.date.today()) if self.fd[
            "contract_date"] else ""
        self.fd["contract_type"] = contract_type
        self.fd["contract_signed_date"] = _faker.date_between(start_date=self.fd["contract_date"],
                                                              end_date=datetime.date.today()) if self.fd[
            "contract_date"] else ""
        self.fd["date_start"] = _faker.date_between(start_date=self.fd["contract_signed_date"],
                                                    end_date=datetime.date.today()) if self.fd[
            "contract_signed_date"] else ""
        self.fd["date_finish"] = ""
        self.fd["base_contract_id"] = parent_id

    def __str__(self):
        return f'contract: {self.fd["contract_number"]} / {self.fd["contract_subject"]} - {self.fd["contract_value"]} parent: {self.fd["base_contract_id"]}'

    @staticmethod
    def generate(project_id: uuid, contract_statuses: pd.DataFrame, contract_types: pd.DataFrame,
                 max_contracts: int = 5) -> pd.DataFrame:
        """
        Generate fresh new set of contracts assigned to given project.
        :param project_id: id of project that this set of contracts will assign.
        :param contract_statuses: pandas.DataFrame for randomly selection of contract's statuses
        :param contract_types: pandas.DataFrame for randomly selection of contract's types
        :param max_contracts: Number of contracts to be created (this is vary by random)
        :return: pandas.DataFrame filled with fresh new data
        in accordance with data-schema as it can be loaded from production DB
        """
        assert max_contracts > 1, "max_contracts LE ONE"
        assert max_contracts < 20, "max_contracts too big"

        c = list()

        for _, cc in enumerate(range(random.randint(1, max_contracts))):
            c_status: uuid = contract_statuses.at[random.randint(0, len(contract_statuses) - 1), "local_id"]
            c_type: uuid = contract_types.at[random.randint(0, len(contract_types) - 1), "local_id"]

            if _ == 0 or random.random() < 0.8:
                # First contract in the set always is root
                # OR
                # We assume that about 80% of contracts has no sub-contracts
                p = ContractEntity(project_id=project_id, contract_status=c_status, contract_type=c_type)
                c_id = p.get_id()
            else:
                p = ContractEntity(project_id=project_id, contract_status=c_status, contract_type=c_type, parent_id=c_id)

            c.append(p.get_data())

        return pd.DataFrame(c)
# end of ContractEntity class


# ########################################


# +ContractParties
class ContractParties(FakeBase):
    """
    Represent any single party (participant) of contract.
    Note: current _faker can provide from 2 (two) to 5 (five) contract parties for each project's instance
    """
    def __init__(self, contract_id, contractors_data: pd.DataFrame, contract_party_types: pd.DataFrame):
        super().__init__()
        self.fd["contract_id"] = contract_id
        self.fd["contractor_id"] = contractors_data.at[random.randint(0, len(contractors_data) - 1), "local_id"]
        self.fd["party_type"] = contract_party_types.at[random.randint(0, len(contract_party_types) - 1), "local_id"]
        self.fd["role_description"] = f"Проектная роль № {random.randint(0, 15)}"
        self.fd["activated_at"] = _faker.date_between(start_date=datetime.date(2024, 1, 1),
                                                      end_date=datetime.date.today()) if random.random() < 0.8 else ""
        self.fd["suspended_at"] = _faker.date_between(start_date=self.fd["activated_at"],
                                                      end_date=datetime.date.today()) if self.fd["activated_at"] and \
                                                                                        self.fd[
                                                                                            "activated_at"] < datetime.date.today() and random.random() < 0.05 else ""
        self.fd["resumed_at"] = _faker.date_between(start_date=self.fd["suspended_at"],
                                                    end_date=datetime.date.today()) if self.fd["suspended_at"] and \
                                                                                      self.fd[
                                                                                          "suspended_at"] < datetime.date.today() and random.random() < 0.05 else ""
        self.fd["is_active"] = True if self.fd["activated_at"] and not self.fd["suspended_at"] else False

    def __str__(self):
        return f'contract party: {self.fd["contract_id"]} / {self.fd["contractor_id"]} - {self.fd["party_type"]} role: {self.fd["role_description"]}'

    @staticmethod
    def generate(contract_data: pd.DataFrame, contractors: pd.DataFrame, contract_party_types: pd.DataFrame,
                 max_contracts: int = 10) -> pd.DataFrame:
        """
        Generate fresh new set of contract/participant relations.
        :param contract_data: pandas.DataFrame represents current set of contracts for randomly selection
        :param contractors: pandas.DataFrame represents current set of contractor entities for randomly selection
        as contract's participant
        :param contract_party_types: pandas.DataFrame represents type of participation for selected participant.
        :param max_contracts: Number of contracts to be processed
        :return: pandas.DataFrame filled with fresh new data
        in accordance with data-schema as it can be loaded from production DB
        """
        assert max_contracts > 0, "max_contracts LE ZERO"
        assert max_contracts < 10, "max_contracts too big"
        assert len(contract_data) > max_contracts * 2, "contract_data too short"
        assert len(contractors) > max_contracts * 2, "Contractor's set too short"
        assert len(contract_party_types) > 0, "contract_party_types is empty"

        p_list = set()
        while len(p_list) < max_contracts:
            idx = random.randint(0, len(contract_data) - 1)
            p_list.add(contract_data.at[idx, "local_id"])

        c = list()
        for p_id in p_list:
            # Number of contract parties is at least 2 and max of 4
            for _ in range(random.randint(2, 5)):
                p = ContractParties(contract_id=p_id, contractors_data=contractors,
                                    contract_party_types=contract_party_types)
                c.append(p.get_data())

        return pd.DataFrame(c)
# end of ContractParties class
