from pandas import Series


def generate_sql(columns: str, column: str, value: str) -> str:
    script = f"SELECT {", ".join(columns)} FROM [DB_NAME].[TABLE_NAME]\n"
    script += f"WHERE {column} = {value}"
    return f"```sql\n{script}\n```"


def create_aggregation_columns(columns: list, aggregations: list) -> dict:
    result = {}
    for column in columns:
        result[column] = aggregations
    return result


def missing(series: Series):
    return series.isnull().sum()
