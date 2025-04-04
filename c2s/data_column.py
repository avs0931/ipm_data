from c2s.make_snake import make_snake


# Class: data_column
class data_column:
    # CSV-indexes for parsing
    _item: int = 0
    _name: int = 1
    _inuse: int = 2
    _type: int = 3
    _pk: int = 4
    _notnull: int = 5
    _unique: int = 6
    _default: int = 7
    _fk_table: int = 8
    _fk_column: int = 9
    _oncopy: int = 10
    _comment: int = 11

    def __init__(self, table_name: str, ns_prefix, csv_line: str):
        items = csv_line.split("\t")
        self.table_name = table_name
        self._coldef = list()
        self.fkdef = ""
        # Check usage. If column not used - mark the whole definition as a comment
        self.in_use = items[self._inuse] == "TRUE"
        if not self.in_use:
            self._coldef.append("--")
        # Snitaize column name and make it snake_style
        self.name = make_snake(items[self._name])
        self._coldef.append(self.name)

        # Apply type
        self._coldef.append(items[self._type])
        # Apply NOT NULL constraint if given
        self._coldef.append("NOT NULL" if items[self._notnull] == "TRUE" else "")
        # Apply UNIQUE constraint if given
        self._coldef.append("UNIQUE" if items[self._unique] == "TRUE" else "")
        # Apply DEFAULT constraint if given
        self._coldef.append(f"DEFAULT {items[self._default].strip()}" if items[self._default].strip() else "")
        # Check for PK on the column
        self.pk = f"CONSTRAINT {table_name}_pk PRIMARY KEY ({self.name})" if items[self._pk] == "TRUE" else ""
        # Align comment
        self.comment = "[PRIMARY KEY] " if self.pk else ""
        self.comment += items[self._comment] if items[self._comment] else ""
        self.comment = self.comment.replace("'", "")
        self.comment = self.comment.replace('"', '')
        self._coldef.append("-- " + self.comment if self.comment else "")
        # Check loadability for the column
        self.docopy = items[self._oncopy] == "TRUE" if items[self._oncopy] else False
        # Create FK if given
        if items[self._fk_table]:
            self.fk_table = make_snake(items[self._fk_table])
            self.fk_table = ns_prefix + "_" + self.fk_table if ns_prefix else self.fk_table
            self.fk_column = make_snake(items[self._fk_column])
            self.fkdef = f"foreign key ({self.name}) references {self.fk_table}({self.fk_column})"
            self.fkdef_full = f"ALTER TABLE {self.name} ADD CONSTRAINT FK_{self.table_name}_{self.name} FOREIGN KEY (FK_{self.fk_table}_{self.fk_column}_REF_{self.table_name}_{self.name}) REFERENCES ({self._fk_column})"
        else:
            self.fkdef = self.fkdef_full = ""

    def is_in_use(self):
        return self.in_use

    def is_loadable(self):
        return self.docopy and self.in_use

    def get_coldef(self):
        return " ".join(map(lambda x: f"{x:<16}", self._coldef))

    def get_name(self):
        return self.name

    def get_pk(self):
        return self.pk

    def get_fk(self):
        return self.fkdef

    def get_fk_full(self):
        return self.fkdef_full

    def get_comment_stmt(self):
        return f"comment on column {self.table_name}.{self.name:32} is '{self.comment}';" if self.comment and self.in_use else ""

    def __str__(self):
        return " ".join(self._coldef)
# end of data_column class
