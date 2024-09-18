from pandas import DataFrame
import datetime

CREATE = "CREATE TABLE"
CREATE_IF = "CREATE TABLE IF NOT EXISTS"
INSERT = "INSERT INTO"
CONSTRAINT = "CONSTRAINT"
VALUES = "VALUES ("
NULL = "NULL"

# DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S.%f"
DATETIME_FORMAT = "%Y-%m-%d %H:%M"
DATE_FORMAT = "%Y-%m-%d"
PRESERVED_KEYWORDS = [
    "ID",
    "DATE",
    "RESULT",
    "GROUP",
    "POSITION",
    "LOCATION",
]


def parse_statements(sql: str):
    statements = []
    for statement in sql.split(";"):
        statement = statement.replace("\n", "").strip()
        while "  " in statement:
            statement = statement.replace("  ", " ")
        statements.append(statement)
    return statements


def parse_tables(statements: list[str]):
    tables = {}
    for statement in statements:
        if statement.upper().startswith(CREATE_IF):
            content = statement.replace(CREATE_IF, "").strip()
        elif statement.upper().startswith(CREATE):
            content = statement.replace(CREATE, "").strip()
        else:
            continue
        table = {}
        name, columns = content.split("(", 1)
        for column in columns.split(","):
            col = column.strip().split(" ")[0]
            if col.upper() != CONSTRAINT:
                table[col] = []
        tables[name] = table
    return tables


def parse_inserts(statements: list[str], tables: dict):
    for statement in statements:
        if statement.upper().startswith(INSERT):
            content = statement.replace(INSERT, "").strip()
        else:
            continue
        name, col_val = content.split("(", 1)
        columns = col_val.strip().split(")", 1)[0].strip().split(",")
        columns = [column.strip() for column in columns]
        values = col_val.strip().split(VALUES)[1]
        values = values.strip().rsplit(")", 1)[0].strip().split(",")
        val_dict = {}
        for idx, column in enumerate(columns):
            value = values[idx].strip()
            if value.startswith("'") and value.endswith("'"):
                value = value[1:-1]
            val_dict[column.strip()] = value
        if name in tables:
            for column in tables[name]:
                value = val_dict.get(column, None)
                if value == NULL:
                    value = None
                tables[name][column].append(value)


class PostgreSQL:
    def __init__(self, df: DataFrame, db_name: str, table_name: str):
        # Source variables
        self.df = df.fillna("")  # Replace all NaN values with an empty string
        self.db_name = db_name
        self.table_name = f"{db_name}.{table_name}"
        # Constants
        self.limit = 0
        self.tab = "    "
        self.null = "NULL"
        self.bit = "BIT"
        self.num = "NUMERIC"
        self.float = "FLOAT"
        self.datetime = "TIMESTAMP"
        self.date = "DATE"
        self.char = "CHARACTER"
        self.data_types = {
            self.bit: 1,
            self.num: 2,
            self.float: 3,
            self.datetime: 4,
            self.date: 5,
            self.char: 6,
        }
        self.preserved_keywords = PRESERVED_KEYWORDS
        # Optional constants
        self.p_key = ""
        # Script variables:
        self.fields = {}
        self.field_suffix = "_field"
        self.sql_script = ""
        # - Create schema statement
        self.schema_script = f"CREATE SCHEMA IF NOT EXISTS {self.db_name}\n"
        self.schema_script += f"{self.tab}AUTHORIZATION dbadmin;\n\n"
        # - Create table statement
        self.table_script = f"DROP TABLE IF EXISTS {self.table_name};\n"
        self.table_create = f"CREATE TABLE IF NOT EXISTS {self.table_name}\n(\n"
        # - Create insert statements
        self.insert_script = f"DELETE FROM {self.table_name};\n"

    def load_data(self):
        self.fields = self._set_source_fields()
        # self.schema_script = self._set_schema_script()
        self.insert_script = self._set_insert_script()
        self.table_script = self._set_table_script()
        self.sql_script = self.schema_script + self.table_script + self.insert_script

    def write_script(self):
        return self.sql_script

    def _set_source_fields(self):
        fields = {}
        columns = list(self.df.columns)
        # Rename fields when necessary
        for field in list(self.df.columns):
            if (
                field.upper() in self.data_types.keys()
                or field.upper() in self.preserved_keywords
            ):
                self.df.rename(columns={field: f"{field}{self.field_suffix}"}, inplace=True)
        for field in list(self.df.columns):
            if " " in field:
                self.df.rename(columns={field: field.replace(" ", "_")}, inplace=True)
        # Set field details (length & type)
        for field in list(self.df.columns):
            length = self.df[field].astype(str).str.len().max()
            fields[field] = {"length": length, "type": ""}
        return fields

    # def _set_schema_script(self):
    #     return self.schema_script

    def _set_table_script(self):
        script = self.table_script
        # Write create table statement script
        script += self.table_create
        for field, data_info in self.fields.items():
            script += f"{self.tab}{field} {data_info["type"]}"
            if data_info["type"] == self.char:
                script += f"({data_info["length"]}),\n"
            else:
                script += ",\n"
        table_key = self.table_name.split(".")[-1] + "_pkey"
        if self.p_key == "":
            self.p_key = list(self.fields.keys())[0]
        return script + f"CONSTRAINT {table_key} PRIMARY KEY ({self.p_key}));\n\n"

    def _set_insert_script(self):
        script = self.insert_script
        # Get values and write insert statements for each row within the data
        for _, row in self.df.iterrows():
            insert_str = f"INSERT INTO {self.table_name}(\n"
            field_str = f"{self.tab}"
            val_str = f"{self.tab}VALUES ("
            for field, val in row.items():
                field_str += f"{field}, "
                val_str += f"{self._validate_data_type(str(val), field)}"
            insert_str += f"{field_str[:-2]})\n"
            insert_str += f"{val_str[:-2]});\n"
            script += insert_str
        return script

    def _validate_data_type(self, value: str, field: str):
        if value == "" or value == None:
            value = self.null
        # NULL
        if value == self.null:
            self._compare_data_type(field, self.bit)
            return f"{value}, "
        # Numeric
        elif value == "0" or (
            value.startswith("0") == False
            and (value.isdigit() or (value.startswith("-") and value[1:].isdigit()))
        ):
            self._compare_data_type(field, self.num)
            return f"{value}, "
        # Float
        elif "." in value and (
            value.replace(".", "", 1).isdigit()
            or (value.startswith("-") and value.replace(".", "", 1)[1:].isdigit())
        ):
            self._compare_data_type(field, self.float)
            return f"{value}, "
        # Datetime
        elif is_datetime(value):
            self._compare_data_type(field, self.datetime)
            return f"'{value}', "
        # Date
        elif is_date(value):
            self._compare_data_type(field, self.date)
            return f"'{value}', "
        # Character
        else:
            self._compare_data_type(field, self.char)
            return f"'{value}', "

    def _compare_data_type(self, field: str, val_type: str):
        d_type = self.fields[field].get("type", "")
        # Undefined
        if d_type == "":
            self.fields[field]["type"] = val_type
        # Differences
        elif self.data_types[val_type] > self.data_types[d_type]:
            self.fields[field]["type"] = val_type
        # NULL
        elif val_type == self.bit and d_type != self.bit:
            self.fields[field]["type"] = d_type


def is_datetime(date_str: str):
    # Check if given string is of datetime format
    result = True
    try:
        res = bool(datetime.datetime.strptime(date_str, DATETIME_FORMAT))
    except ValueError:
        result = False
    return result


def is_date(date_str: str):
    # Check if given string is of date format
    result = True
    try:
        res = bool(datetime.datetime.strptime(date_str, DATE_FORMAT))
    except ValueError:
        result = False
    return result
