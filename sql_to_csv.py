import streamlit as st
import pandas as pd
from io import StringIO
import datetime
from pprint import pp

CREATE = "CREATE TABLE"
CREATE_IF = "CREATE TABLE IF NOT EXISTS"
INSERT = "INSERT INTO"
CONSTRAINT = "CONSTRAINT"
VALUES = "VALUES ("
NULL = "NULL"


def main():
    sql_string = st.text_area("Enter SQL statements:")
    if sql_string:
        parse_sql(sql_string)


def file_upload():
    file = st.file_uploader("Choose a SQL script", "sql")
    if file is None:
        st.write("Waiting on file upload...")
    else:
        # Convert UploadedFile to a string based IO
        stringio = StringIO(file.getvalue().decode("utf-8"))
        # Read file as string
        string_data = stringio.read()
        parse_sql(string_data)


def parse_sql(sql: str):
    with st.status("Parsing SQL...", expanded=False) as status:
        # Get statements of SQL script
        statements = parse_statements(sql)
        st.write("SQL statements parsed.")
        # Get tables from statements
        tables = parse_tables(statements)
        st.write("SQL tables parsed.")
        # Get inserts from statements
        inserts = parse_inserts(statements, tables)
        st.write("SQL inserts parsed.")
        # Done
        status.update(label="Parsing SQL completed!", state="complete", expanded=False)
    # View tables parsed from SQL
    st.subheader("Parsed Tables")
    st.json(tables, expanded=False)
    # Create CSV for selected table
    create_csv_tables(tables)


def create_csv_tables(tables: dict):
    table = st.radio("Select the table to download:", tables.keys())
    delimiter = st.radio("Select the delimiter of the CSV:", [";", ","])
    if delimiter:
        df = pd.DataFrame(tables[table])
        data = df.to_csv(index=False, sep=delimiter)
        # file_name = (
        #     str(datetime.datetime.now())
        #     .replace(" ", "_")
        #     .replace(":", "")
        #     .replace(".", "")
        # )
        if st.download_button(
            ":material/download: Download Data", data, f"{table}.csv"
        ):
            return


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


if __name__ == "__main__":
    main()
