from Datagen.Datagen_issues import *



def _check_fb() -> None:
    # CHECK - Inherited from Jupiter notebook
    fd = FakeBase()
    print(fd.get_headers())
    print(fd.get_headers(ii_include=False))
    print(fd.get_csv())
    print(fd.get_csv(ii_include=False))
# end of _check_fb()


def _check_ld_by_key() -> None:
    # CHECK generator - Inherited from Jupiter notebook
    # rk = "missssss"
    # rk = "ld_project_types"
    # rk = "ld_project_statuses"
    # rk = "ld_project_party_types"
    # rk = "ld_contract_types"
    # rk = "ld_contract_statuses"
    # rk = "ld_contract_party_types"
    # rk = "ld_control_check_types"
    # rk = "ld_control_violation_types"
    # rk = "ld_executive_doc_types"
    rk = "ld_permissive_doc_types"

    df = ld_by_key.generate(resource_key=rk)
    print(f"genlen = {len(df)}")
    df.head()
# end of _check_ld_by_key()


def _check_personality_entity() -> None:
    # CHECK: PersonalityEntity
    pe = PersonalityEntity()
    print(pe)
    print(pe.get_headers(ii_include=False))
    print(pe.get_csv(ii_include=False))
    print()
    print(pe.get_headers())
    print(pe.get_csv())

    # CHECK generator
    df = PersonalityEntity.generate()
    print(f"gelen = {len(df)}")
    df.head()
# end of _check_personality_entity()


def _check_contractor_entity() -> None:
    # CHECK - Inherited from Jupiter notebook
    # feed related data
    banks = load_entity_data(entity_name="bank_entity")

    te = ContractorEntity(bank_data=banks)
    print(te)
    print(te.get_headers(ii_include=False))
    print(te.get_csv(ii_include=False))
    print()
    print(te.get_headers())
    print(te.get_csv())

    # CHECK generator
    df = ContractorEntity.generate(bank_data=banks, max_companies=1000)
    print(f"gelen = {len(df)}")
    df.head()
# end of _check_contractor_entity()


def _check_contractor_personnel() -> None:
    # CHECK
    # feed related data
    cid = uuid.uuid4()
    emp = PersonalityEntity.generate(maxval=10)

    te = ContractorPersonnel(contractor_id=cid, personal_data=emp)
    print(te)
    print(te.get_headers(ii_include=False))
    print(te.get_csv(ii_include=False))
    print()
    print(te.get_headers())
    print(te.get_csv())

    # # CHECK generator
    banks = load_entity_data(entity_name="bank_entity")
    cdata = ContractorEntity.generate(bank_data=banks)
    pdata = PersonalityEntity.generate()
    cpdata = ContractorPersonnel.generate(contractor_data=cdata, personality_data=pdata, max_companies=2,
                                          max_employees=10)
    print(f"gelen = {len(cpdata)}")
    cpdata.head()
# end of _check_contractor_personnel()


def _check_project_entity() -> None:
    # CHECK: ProjectEntity - Inherited from Jupiter notebook
    pst = ld_by_key.generate(resource_key="ld_project_types")
    pss = ld_by_key.generate(resource_key="ld_project_statuses")
    pe = ProjectEntity(statuses=pss, types=pst)
    print(pe)
    print(pe.get_headers(ii_include=False))
    print(pe.get_csv(ii_include=False))
    print()
    print(pe.get_headers())
    print(pe.get_csv())

    # CHECK generator
    df = ProjectEntity.generate(statuses=pss, types=pst)
    print(f"gelen = {len(df)}")
    df.head()

    # Clean-up
    del pe, df, pst, pss
# end of _check_project_entity()


def _check_project_parties() -> None:
    # CHECK: ProjectParties
    # Contractor's data
    bdt = load_entity_data(entity_name="bank_entity")
    cdt = ContractorEntity.generate(bank_data=bdt)

    # Project's data
    pst = ld_by_key.generate(resource_key="ld_project_types")
    pss = ld_by_key.generate(resource_key="ld_project_statuses")
    pdt = ProjectEntity.generate(statuses=pss, types=pst)

    # Single project_party
    p_id = pdt.at[random.randint(0, len(pdt) - 1), "local_id"]
    pe = ProjectParties(project_id=p_id, contractor_data=cdt, project_party_types=pst)
    print(pe)
    print(pe.get_headers(ii_include=False))
    print(pe.get_csv(ii_include=False))
    print()
    print(pe.get_headers())
    print(pe.get_csv())

    # # CHECK generator
    df = ProjectParties.generate(project_data=pdt, contractors=cdt, project_party_types=pst, max_projects=3)
    print(f"genlen = {len(df)}")
    df.head()
# end of _check_project_parties()


def _check_construction_site() -> None:
    # CHECK: ConstructionSite - Inherited from Jupiter notebook

    # Project's data
    pst = ld_by_key.generate(resource_key="ld_project_types")
    pss = ld_by_key.generate(resource_key="ld_project_statuses")
    pdt = ProjectEntity.generate(statuses=pss, types=pst)

    # Single ConstructionSite
    p_id = pdt.at[random.randint(0, len(pdt) - 1), "local_id"]
    pe = ConstructionSite(project_id=p_id, is_primary=(random.random() < 0.5), parent_id="test")
    print(pe)
    print(pe.get_headers(ii_include=False))
    print(pe.get_csv(ii_include=False))
    print()
    print(pe.get_headers())
    print(pe.get_csv())

    # CHECK generator
    df = ConstructionSite.generate(project_data=pdt, max_projects=3, max_objects=10)
    print(f"genlen = {len(df)}")
    df.head()
# end of _check_construction_site()


def _check_contract_entity() -> None:
    # CHECK: ContractEntity - Inherited from Jupiter notebook
    # Project's data
    pst = ld_by_key.generate(resource_key="ld_project_types")
    pss = ld_by_key.generate(resource_key="ld_project_statuses")
    pdt = ProjectEntity.generate(statuses=pss, types=pst, max_projects=25)
    p_id = pdt.at[random.randint(0, len(pdt) - 1), "local_id"]

    # Contrast's related
    css = ld_by_key.generate(resource_key="ld_contract_statuses")  # ld_contract_statuses.generate()
    cs_id = css.at[random.randint(0, len(css) - 1), "local_id"]
    cst = ld_by_key.generate(resource_key="ld_contract_types")  # ld_contract_types.generate()
    ct_id = cst.at[random.randint(0, len(cst) - 1), "local_id"]

    # Single contract
    pe = ContractEntity(project_id=p_id, contract_status=cs_id, contract_type=ct_id)
    print(pe)
    print(pe.get_headers(ii_include=False))
    print(pe.get_csv(ii_include=False))
    print()
    print(pe.get_headers())
    print(pe.get_csv())

    # CHECK generator
    df = ContractEntity.generate(project_id=pdt, contract_statuses=css, contract_types=cst)
    print(f"genlen = {len(df)}")
    df.head()
# end of _check_contract_entity()


def _check_contract_parties() -> None:
    # CHECK: ContractParties - Inherited from Jupiter notebook
    # Contractor's data
    bdt = load_entity_data(entity_name="bank_entity")
    cdt = ContractorEntity.generate(bank_data=bdt, max_companies=100)

    # Project's data
    pst = ld_by_key.generate(resource_key="ld_project_party_types")  # ld_project_party_types.generate()
    pss = ld_by_key.generate(resource_key="ld_project_statuses")  # ld_project_statuses.generate()
    pdt = ProjectEntity.generate(statuses=pss, types=pst, max_projects=25)
    p_id = pdt.at[random.randint(0, len(pdt) - 1), "local_id"]

    # Contract's data
    cst = ld_by_key.generate(resource_key="ld_contract_party_types")  # ld_project_party_types.generate()
    css = ld_by_key.generate(resource_key="ld_contract_statuses")  # ld_contract_statuses.generate()
    cet = ContractEntity.generate(project_id=pdt, contract_statuses=css, contract_types=cst)

    # Single project_party
    c_id = cet.at[random.randint(0, len(cet) - 1), "local_id"]
    pe = ContractParties(contract_id=c_id, contractors_data=cdt, contract_party_types=cst)
    print(pe)
    print(pe.get_headers(ii_include=False))
    print(pe.get_csv(ii_include=False))
    print()
    print(pe.get_headers())
    print(pe.get_csv())

    # # CHECK generator
    df = ContractParties.generate(contract_data=cet, contractors=cdt, contract_party_types=cst, max_contracts=5)
    print(f"genlen = {len(df)}")
    df.head()
# end of _check_contract_parties()
