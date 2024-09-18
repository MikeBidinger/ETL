import streamlit as st
import pandas as pd
from pandas import DataFrame
import numpy as np
from io import BytesIO, StringIO
import pickle
from sql import PostgreSQL, parse_statements, parse_tables, parse_inserts

FILE_TYPES = [
    "Delimited",
    "Excel",
    "SQL",
    # "JSON",
    # "XML",
]
DELIMITERS = {
    "Comma": ",",
    "Semicolon": ";",
    "Pipe": "|",
    "Space": " ",
    "Tab": "\t",
}

# st.set_page_config(page_title="ETL App", page_icon=":material/database:")
st.set_page_config(page_title="ETL App", page_icon="file_view.svg")

st.title("ETL App")


class ETL:
    def __init__(self):
        # Extract
        self.file = None
        self.interpretation = None
        self.encoding = "utf-8-sig"
        self.file_type_in = None
        self.data_type = None
        self.delimiter_in = None
        self.sheet = None
        self.sql = ""
        self.df: DataFrame = None
        # Transform
        # Load
        self.name = None
        self.extension = None
        self.file_type_out = None
        self.delimiter_out = None
        self.del_idx = None
        self.db_name = ""
        self.db_table = ""

    def import_settings(self):
        if st.button("Import ETL Settings"):
            with open("settings.pickle", "rb") as f:
                self.__dict__ = pickle.load(f)

    def extract(self) -> bool:
        st.subheader("Extract")
        with st.expander("View/Hide data extraction settings"):
            if self._upload_file():
                self._read_data()
                return True
        return False

    def transform(self) -> bool:
        st.subheader("Transform")
        with st.expander("View/Hide data transformation settings"):
            if self._transform_data():
                return True
        return False

    def load(self):
        st.subheader("Load")
        with st.expander("View/Hide data load settings"):
            self._load_data()

    def save_settings(self):
        if st.button("Save ETL Settings"):
            with open("settings.pickle", "wb") as f:
                pickle.dump(self.__dict__, f)

    def _upload_file(self) -> bool:
        # Select data interpretation
        self.interpretation = st.radio(
            "Select data interpretation:",
            [None, "str"],
            horizontal=True,
            captions=["Auto interpretation", "String interpretation"],
        )
        # Select data type of extraction
        data_type = st.radio(
            "Select the file type of the data to extract:", FILE_TYPES, horizontal=True
        )
        if data_type == "Delimited":
            # Define delimited file type
            self.file_type_in = self._define_delimited()
        elif data_type == "Excel":
            self.file_type_in = ["xlsx", "xls"]
        else:
            self.file_type_in = data_type.lower()
        # File selection
        self.file = st.file_uploader("Choose a file", self.file_type_in)
        self.sql = ""
        if data_type == "SQL":
            if st.checkbox("Enter SQL statements manually"):
                self.sql = st.text_area("Enter SQL statements:")
        if self.file is None and self.sql == "":
            st.write("Waiting on file upload...")
            return False
        elif data_type == "Excel":
            # Excel sheet selection
            self.sheet = self._sheet_selection()
        self.data_type = data_type
        return True

    def _define_delimited(self) -> list[str]:
        # Delimiter selection
        delimiter = None
        if st.checkbox("Detect delimiter automatically", True):
            delimiter = None
        else:
            delimiter = st.radio(
                "Select a delimiter:",
                DELIMITERS.values(),
                horizontal=True,
                captions=DELIMITERS.keys(),
            )
        self.delimiter_in = delimiter
        return ["csv", "txt"]

    def _sheet_selection(self) -> str:
        # Excel sheet selection
        xl = pd.ExcelFile(self.file)
        return st.radio("Select a Excel sheet to extract:", xl.sheet_names)

    def _read_data(self):
        # Read file into DataFrame
        if self.data_type == "Delimited":
            self.df = pd.read_csv(
                self.file,
                sep=self.delimiter_in,
                dtype=self.interpretation,
                engine="python",
                encoding=self.encoding,
            )
        elif self.data_type == "Excel":
            self.df = pd.read_excel(self.file, self.sheet)
        elif self.data_type == "SQL":
            self.df = self._parse_sql()
        # Preview data
        if st.checkbox("Preview data"):
            preview_rows = st.slider(
                "Preview row amount selection:",
                min_value=0,
                max_value=self.df.shape[0],
                step=1,
            )
            st.dataframe(self.df.head(preview_rows))

    def _parse_sql(self) -> DataFrame:
        if self.file is not None:
            # Convert UploadedFile to a string based IO
            stringio = StringIO(self.file.getvalue().decode("utf-8"))
            # Read file as string
            self.sql = stringio.read()
        # Get statements of SQL script
        statements = parse_statements(self.sql)
        # Get tables from statements
        tables = parse_tables(statements)
        # Add insert values to tables
        parse_inserts(statements, tables)
        # Return selected table as DataFrame
        table = st.radio("Select the table to download:", tables.keys())
        return pd.DataFrame(tables[table])

    def _transform_data(self) -> bool | None:
        # Data transformation
        if st.checkbox("No transformation", True):
            return True
        # Filter columns
        # Filter rows
        # Transform column
        # Calculate column

    def _load_data(self):
        # Output extension selection
        self.file_type_out = st.radio(
            "File type selection", FILE_TYPES, None, horizontal=True
        )
        # File name selection
        self.name = st.text_input("Enter a file name:", self.name)
        # Download data
        if self.file_type_out:
            if self._download_data():
                st.write(
                    f"Data downloaded successfully as `{self.name}{self.extension}`."
                )

    def _download_data(self) -> bool:
        # File type selection
        if self.file_type_out == "Delimited":
            self.delimiter_out = st.radio(
                "Select a delimiter:",
                DELIMITERS.values(),
                index=self.del_idx,
                horizontal=True,
                captions=DELIMITERS.keys(),
            )
            if self.delimiter_out is None:
                return False
        if self.file_type_out == "SQL":
            self.db_name = st.text_input("Enter DB name:")
            self.db_table = st.text_input("Enter Table name:")
            if self.db_name == "" or self.db_table == "":
                return False
        # Download data
        if self.name:
            data = self._convert_df()
            if st.download_button(
                ":material/download: Download Data",
                data,
                f"{self.name}{self.extension}",
            ):
                return True
        return False

    # @st.cache_data
    # # IMPORTANT: Cache the conversion to prevent computation on every rerun
    def _convert_df(self):
        # Convert DataFrame to specific selections:
        if self.file_type_out == "Delimited":
            # - CSV
            if (
                self.delimiter_out == DELIMITERS["Comma"]
                or self.delimiter_out == DELIMITERS["Semicolon"]
            ):
                self.extension = ".csv"
                return self.df.to_csv(index=False, sep=self.delimiter_out)
            # - TXT
            elif (
                self.delimiter_out == DELIMITERS["Pipe"]
                or self.delimiter_out == DELIMITERS["Space"]
            ):
                self.extension = ".txt"
                return self.df.to_csv(index=False, sep=self.delimiter_out)
            elif self.delimiter_out == DELIMITERS["Tab"]:
                self.extension = ".tsv"
                return self.df.to_csv(index=False, sep=self.delimiter_out)
        # - Excel
        if self.file_type_out == "Excel":
            self.extension = ".xlsx"
            buffer = BytesIO()
            with pd.ExcelWriter(buffer) as writer:
                # Write each dataframe to a different worksheet.
                self.df.to_excel(writer, sheet_name="Sheet1", index=False)
                return buffer
        # - SQL
        if self.file_type_out == "SQL":
            self.extension = ".sql"
            sql = PostgreSQL(self.df, self.db_name, self.db_table)
            sql.load_data()
            return sql.write_script()
        # - JSON
        # - XML


if __name__ == "__main__":
    etl = ETL()
    # etl.import_settings()
    if etl.extract():
        if etl.transform():
            etl.load()
        # etl.save_settings()
    # st.subheader("Debug")
    # etl
