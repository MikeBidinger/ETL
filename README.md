# ETL App

This Python ETL app (GUI built with Streamlit) is able to extract, transform
and load data in a user-friendly manner.
See the progress of included functionalities below.

-   Extract: _read_data()_ (all including data previewing)

    -   [x] Delimited:
        -   [x] Comma
        -   [x] Semicolon
        -   [x] Pipe
        -   [x] Space
        -   [x] Tab
    -   [x] Excel
    -   [x] SQL
    -   [ ] JSON
    -   [ ] XML

-   Transform: _transform_data()_

    -   [ ] Filter columns
    -   [ ] Filter rows
    -   [ ] Transform column
    -   [ ] Calculate column

-   Load: _convert_df()_

    -   [x] CSV
    -   [x] TXT
    -   [x] TSV
    -   [x] Excel
    -   [x] SQL
    -   [ ] JSON
    -   [ ] XML

-   Additional features are:

    -   [ ] Save settings
    -   [ ] Import settings

# Assets

The assets used to build this app are:

-   [Anaconda](https://www.anaconda.com/ "Anaconda")
-   [Streamlit](https://streamlit.io/ "Streamlit")
-   [Pandas](https://pandas.pydata.org/ "pandas")
-   [NumPy](https://numpy.org/ "NumPy")

# Setup

### Create the Anaconda virtual environment:

```console
conda env create -f environment.yml
```

### Activate the Anaconda virtual environment:

```console
conda activate StreamlitEnv
```

### Run the Streamlit app:

-   Locally:

First `cd` into the correct directory:

```console
cd /Users/user/directory
```

```console
streamlit run main.py
```

-   GitHub:

```console
streamlit run https://raw.githubusercontent.com/MikeBidinger/ETL/main/main.py
```
