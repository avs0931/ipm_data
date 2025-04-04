from c2s.data_column import data_column
from c2s.make_snake import make_snake


class data_table:

    _comment = 11

    def __init__(self, csv_line: str, ns_prefix: str = ""):
        items = csv_line.split('\t')
        self.columns = list()
        self.fkeys = list()
        self.pk = list()
        #
        self.name = make_snake(items[1])
        self.ns_prefix = ""
        if ns_prefix:
            self.ns_prefix = ns_prefix
            self.name = ns_prefix + "_" + self.name
        self.comment = items[self._comment] if items[self._comment] else ""
        self.comment = self.comment.replace("'", "")
        self.comment = self.comment.replace('"', '')

    def get_col_count(self):
        return len(self.columns)

    def get_name(self):
        return self.name

    def append_column(self, csv_line: str):
        # Parse csv-line and create column definition for it
        c = data_column(table_name=self.name, ns_prefix=self.ns_prefix, csv_line=csv_line)
        self.columns.append(c)
        if c.pk:
            self.pk.append(c.get_pk())
            if len(self.pk) > 1:
                print(f"Warning: table '{self.get_name()}' contains more than one PRIMARY KEY constraint definition")

        if c.fkdef:
            self.fkeys.append(c.get_fk())

    def get_coldefs(self) -> []:
        return list([x.get_coldef() for x in self.columns if x.is_in_use()])

    def get_colnames(self) -> []:
        return list([x.get_name() for x in self.columns])

    def get_loadable(self) -> []:
        return list([x.get_name() for x in self.columns if x.is_loadable()])

    def get_pk_defs(self) -> []:
        return list([x.get_pk() for x in self.columns if x.get_pk()])

    def get_fk_defs(self) -> []:
        return list([x.get_fk() for x in self.columns if x.get_fk()])

    def get_comment_defs(self) -> []:
        return list([x.get_comment_stmt() for x in self.columns if x.get_comment_stmt()])

    def drop_table_stmt(self, force_comment: bool = True):
        s = "-- " if force_comment else ""
        return f"{s}drop table if exists {self.name};"

    def truncate_stmt(self, force_comment: bool = True):
        # Create TRUNCATE TABLE Statement
        s = "-- " if force_comment else ""
        return f"{s}truncate table {self.name};"

    def create_stmt(self, indent: int = 4):
        # create CREATE TABLE Statement
        # Merge lists under create table
        # Col defs
        cld = self.get_coldefs() + self.get_pk_defs() + self.get_fk_defs()

        # Construct statement
        stmt = list()
        stmt.append(f"create table {self.name} (")
        # Apply lists to definition
        stmt.append(("\n" + " " * indent + ", ").join(cld))
        stmt.append(");")
        # Apply comments for table
        stmt.append(f"comment on table {self.name} is '{self.comment}';" if self.comment else "")
        # Collect all comments for columns
        stmt += self.get_comment_defs()

        return "\n".join(stmt)

    def copy_stmt(self, data_file: str):
        stmt = list()
        stmt.append(f"copy {self.name}")
        stmt.append(f"( {', '.join(self.get_loadable())})")
        stmt.append(f"from '{data_file}'")
        stmt.append(r"with (format text, header true, encoding 'utf-8', NULL '')")
        stmt.append(";")

        return "\n".join(stmt)
# end of data_table class
