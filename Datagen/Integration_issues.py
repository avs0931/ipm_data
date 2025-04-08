import datetime


# integration_issue
class integration_issue:
    def __init__(self):
        self.fd = dict()
        self.fd["meta_source_name"] = "avs@ipm"
        self.fd["meta_source_time_stamp"] = datetime.datetime.now()
        self.fd["meta_source_row_loaded_at"] = datetime.datetime.now()
        self.fd["meta_source_row_version"] = 0
        self.fd["meta_local_time_stamp"] = datetime.datetime.now()
        self.fd["meta_row_allowed"] = True
        self.fd["meta_row_valid_to"] = ""

    def get_headers(self):
        return "\t".join(list(self.fd.keys()))

    def get_csv(self):
        return "\t".join(list(map(lambda x: str(x), self.fd.values())))

    def get_data(self) -> {}:
        return self.fd

    def __str__(self):
        return (
            f'{self.fd["meta_source_name"]}: version: {self.fd["meta_source_row_version"]} / loaded at: {self.fd["meta_source_row_loaded_at"]}')
# end of integration_issue class


def _check_ii() -> None:
    # CHECK - Inherited from Jupiter notebook
    ii = integration_issue()
    print(ii)
    print(ii.get_headers())
    print(ii.get_csv())
    del ii
# end of _check_ii()
