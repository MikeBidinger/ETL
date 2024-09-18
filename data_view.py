import streamlit as st
import pandas as pd
import numpy as np
from utils import generate_sql, create_aggregation_columns, missing

AGG = ["size", "count", missing, "nunique", "unique"]

st.set_page_config(page_title="Data Discovery", page_icon="file_view.svg")

st.title("Data Discovery")

upload_file = st.file_uploader("Choose a CSV file")

# If a file is uploaded
if upload_file is not None:

    # Read CSV file into DataFrame
    df = pd.read_csv(upload_file)

    # Data preview
    st.subheader("Data Preview")
    with st.expander("View/Hide data preview"):
        preview_rows = st.slider(
            "Preview row amount selection:", min_value=1, max_value=df.shape[0], step=1
        )
        st.write(f"Previewing `{preview_rows}` row(s)")
        st.write(df.head(preview_rows))

    # Data statistics summary
    st.subheader("Data Statistics Summary")
    with st.expander("View/Hide data statistics summary"):
        st.write(df.describe())

    # Data filtering
    st.subheader("Data Filtering")
    with st.expander("View/Hide data filtering"):
        columns = df.columns.tolist()
        sel_cols = st.multiselect("Select columns to display", columns, columns[0])
        if sel_cols:
            df_cols = df[sel_cols]
            sel_col = st.selectbox("Select column to filter", sel_cols)
            unique_vals = np.sort(df[sel_col].unique())
            sel_val = st.selectbox("Select value to filter", unique_vals)
            df_filter = df_cols[df_cols[sel_col] == sel_val]
            st.write(f"Rows remaining after filtering: `{df_filter.shape[0]}`")
            st.write(df_filter)
            if st.button("Generate SQL"):
                st.write(generate_sql(sel_cols, sel_col, sel_val))

    # Data quality
    st.subheader("Data Quality")
    with st.expander("View/Hide data quality"):
        columns = df.columns.tolist()
        sel_cols = st.multiselect("Select columns to investigate", columns, columns[0])
        agg_cols = create_aggregation_columns(sel_cols, AGG)
        if sel_cols:
            df_quality = df.agg(agg_cols)
            st.dataframe(df_quality)

    # Data editing
    st.subheader("Data Editing")
    with st.expander("View/Hide data editing"):
        st.data_editor(df)

    # Data visualization
    st.subheader("Data Visualization")
    with st.expander("View/Hide data visualization"):
        x_col = st.selectbox("Select x-axis column", columns)
        y_col = st.selectbox("Select y-axis column", columns)
        if st.checkbox("Activate filtering"):
            sel_col = st.selectbox("Select column", columns)
            unique_vals = np.sort(df[sel_col].unique())
            sel_val = st.selectbox("Select value", unique_vals)
            df_filter = df[df[sel_col] == sel_val]
        else:
            df_filter = df
        if st.button("Generate Plot"):
            st.line_chart(df_filter.set_index(x_col)[y_col])
        if st.button("Generate Chart"):
            st.bar_chart(df_filter.set_index(x_col)[y_col])
        if st.button("Generate Geo-plot"):
            st.map(
                df.astype({"Latitude": float, "Longitude": float}),
                latitude="Latitude",
                longitude="Longitude",
            )

else:
    st.write("Waiting on file upload...")
