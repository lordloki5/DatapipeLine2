import pandas as pd
import psycopg2
from datetime import datetime
import os

# Default database connection URL
DEFAULT_DB_URL = "dbname=ohcldata host=localhost port=5432 user=dhruvbhandari password=''"

# Helper function to get index name from index_id
def get_index_name(index_id, cursor):
    query = f"SELECT index_name FROM indices WHERE index_id = {index_id}"
    cursor.execute(query)
    result = cursor.fetchone()
    return result[0] if result else None

# 1. get_score_forReferences
def get_score_for_references(stock, score_type, score_subtype, date, aggregation_method="max", export_format=None):
    year, month = date.year, date.month
    connection = psycopg2.connect(DEFAULT_DB_URL)
    cursor = connection.cursor()
    
    query = f"""
        SELECT t1.score_value, t1.sectoral_index_id
        FROM top_1_scores t1
        JOIN stock_index_mapping sim ON t1.sectoral_index_id = sim.index_id
        WHERE sim.stock_symbol = '{stock}'
        AND t1.score_type = '{score_type}'
        AND EXTRACT(YEAR FROM t1.trade_date) = {year}
        AND EXTRACT(MONTH FROM t1.trade_date) = {month}
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=['score_value', 'sectoral_index_id'])
    
    cursor.close()
    connection.close()
    
    if df.empty:
        return None
    
    if aggregation_method == "max":
        result = df['score_value'].max()
    elif aggregation_method == "min":
        result = df['score_value'].min()
    elif aggregation_method == "both":
        result = df.to_dict(orient="records")
    
    if export_format:
        export_df = pd.DataFrame([{"stock": stock, "score": result}]) if aggregation_method != "both" else df
        if export_format == "excel":
            export_df.to_excel(f"score_{stock}_{year}_{month}.xlsx", index=False)
        elif export_format == "csv":
            export_df.to_csv(f"score_{stock}_{year}_{month}.csv", index=False)
    
    return result

# 2. get_all_scores_for_references_for_stock_date
def get_all_scores_for_references_for_stock_date(stock, date, aggregation_method="max", export_format=None):
    year, month = date.year, date.month
    connection = psycopg2.connect(DEFAULT_DB_URL)
    cursor = connection.cursor()
    
    query = f"""
        SELECT t1.score_type, t1.score_value, t1.sectoral_index_id
        FROM top_1_scores t1
        JOIN stock_index_mapping sim ON t1.sectoral_index_id = sim.index_id
        WHERE sim.stock_symbol = '{stock}'
        AND EXTRACT(YEAR FROM t1.trade_date) = {year}
        AND EXTRACT(MONTH FROM t1.trade_date) = {month}
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=['score_type', 'score_value', 'sectoral_index_id'])
    
    cursor.close()
    connection.close()
    
    if df.empty:
        return None
    
    if aggregation_method == "both":
        result = df
    else:
        result = df.groupby('score_type').agg({'score_value': aggregation_method}).reset_index()
    
    if export_format:
        if export_format == "excel":
            result.to_excel(f"all_scores_{stock}_{year}_{month}.xlsx", index=False)
        elif export_format == "csv":
            result.to_csv(f"all_scores_{stock}_{year}_{month}.csv", index=False)
    
    return result

# 3. get_scores_for_subtype_date
def get_scores_for_subtype_date(score_subtype, date, stocks=None, aggregation_method="max", export_format=None):
    year, month = date.year, date.month
    stock_filter = f"AND sim.stock_symbol IN {tuple(stocks)}" if stocks else ""
    connection = psycopg2.connect(DEFAULT_DB_URL)
    cursor = connection.cursor()
    
    query = f"""
        SELECT sim.stock_symbol, t1.score_value, t1.sectoral_index_id
        FROM top_1_scores t1
        JOIN stock_index_mapping sim ON t1.sectoral_index_id = sim.index_id
        WHERE t1.score_type = '{score_subtype}'
        AND EXTRACT(YEAR FROM t1.trade_date) = {year}
        AND EXTRACT(MONTH FROM t1.trade_date) = {month}
        {stock_filter}
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=['stock_symbol', 'score_value', 'sectoral_index_id'])
    
    cursor.close()
    connection.close()
    
    if df.empty:
        return None
    
    if aggregation_method == "both":
        result = df
    else:
        result = df.groupby('stock_symbol').agg({'score_value': aggregation_method}).reset_index()
    
    if export_format:
        if export_format == "excel":
            result.to_excel(f"scores_subtype_{score_subtype}_{year}_{month}.xlsx", index=False)
        elif export_format == "csv":
            result.to_csv(f"scores_subtype_{score_subtype}_{year}_{month}.csv", index=False)
    
    return result

# 4. get_scores_for_type_date
def get_scores_for_type_date(score_type, date, stocks=None, aggregation_method="max", export_format=None):
    year, month = date.year, date.month
    stock_filter = f"AND sim.stock_symbol IN {tuple(stocks)}" if stocks else ""
    connection = psycopg2.connect(DEFAULT_DB_URL)
    cursor = connection.cursor()
    
    query = f"""
        SELECT sim.stock_symbol, t1.score_value, t1.sectoral_index_id
        FROM top_1_scores t1
        JOIN stock_index_mapping sim ON t1.sectoral_index_id = sim.index_id
        WHERE t1.score_type = '{score_type}'
        AND EXTRACT(YEAR FROM t1.trade_date) = {year}
        AND EXTRACT(MONTH FROM t1.trade_date) = {month}
        {stock_filter}
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=['stock_symbol', 'score_value', 'sectoral_index_id'])
    
    cursor.close()
    connection.close()
    
    if df.empty:
        return None
    
    if aggregation_method == "both":
        result = df
    else:
        result = df.groupby('stock_symbol').agg({'score_value': aggregation_method}).reset_index()
    
    if export_format:
        if export_format == "excel":
            result.to_excel(f"scores_type_{score_type}_{year}_{month}.xlsx", index=False)
        elif export_format == "csv":
            result.to_csv(f"scores_type_{score_type}_{year}_{month}.csv", index=False)
    
    return result

# 5. get_score_summary_by_type
def get_score_summary_by_type(date, score_type=None, score_subtype=None, entity="both", export_format=None):
    year, month = date.year, date.month
    type_filter = f"AND t1.score_type = '{score_type}'" if score_type else ""
    subtype_filter = f"AND t1.score_type = '{score_subtype}'" if score_subtype else ""
    connection = psycopg2.connect(DEFAULT_DB_URL)
    cursor = connection.cursor()
    
    query = f"""
        SELECT t1.sectoral_index_id, t1.score_value
        FROM top_1_scores t1
        WHERE EXTRACT(YEAR FROM t1.trade_date) = {year}
        AND EXTRACT(MONTH FROM t1.trade_date) = {month}
        {type_filter}
        {subtype_filter}
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=['sectoral_index_id', 'score_value'])
    
    cursor.close()
    connection.close()
    
    if df.empty:
        return None
    
    df['index_name'] = df['sectoral_index_id'].apply(lambda x: get_index_name(x, psycopg2.connect(DEFAULT_DB_URL).cursor()))
    summary = df.groupby('index_name')['score_value'].agg(['mean', 'min', 'max', 'count']).reset_index()
    
    if export_format:
        if export_format == "excel":
            summary.to_excel(f"summary_{year}_{month}.xlsx", index=False)
        elif export_format == "csv":
            summary.to_csv(f"summary_{year}_{month}.csv", index=False)
    
    return summary

# 6. get_score_summary_by_conditions
def get_score_summary_by_conditions(conditions, entity="both", date=None, export_format=None):
    year, month = (date.year, date.month) if date else (None, None)
    date_filter = f"AND EXTRACT(YEAR FROM t1.trade_date) = {year} AND EXTRACT(MONTH FROM t1.trade_date) = {month}" if date else ""
    connection = psycopg2.connect(DEFAULT_DB_URL)
    cursor = connection.cursor()
    
    where_clauses = []
    for condition in conditions.get("AND", []):
        if "score_type" in condition:
            where_clauses.append(f"t1.score_type = '{condition['score_type']}'")
        if "score_subtype" in condition:
            where_clauses.append(f"t1.score_type = '{condition['score_subtype']}'")
        if "score_value" in condition:
            for op, val in condition["score_value"].items():
                where_clauses.append(f"t1.score_value {op} {val}")
    
    where_clause = " AND ".join(where_clauses) if where_clauses else "TRUE"
    
    query = f"""
        SELECT t1.sectoral_index_id, t1.score_value
        FROM top_1_scores t1
        WHERE {where_clause}
        {date_filter}
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=['sectoral_index_id', 'score_value'])
    
    cursor.close()
    connection.close()
    
    if df.empty:
        return None
    
    df['index_name'] = df['sectoral_index_id'].apply(lambda x: get_index_name(x, psycopg2.connect(DEFAULT_DB_URL).cursor()))
    summary = df.groupby('index_name')['score_value'].agg(['mean', 'min', 'max', 'count']).reset_index()
    
    if export_format:
        filename = f"summary_conditions_{year}_{month}" if date else "summary_conditions"
        if export_format == "excel":
            summary.to_excel(f"{filename}.xlsx", index=False)
        elif export_format == "csv":
            summary.to_csv(f"{filename}.csv", index=False)
    
    return summary

# 7. get_topbottom_scores_by_type_subtype
def get_topbottom_scores_by_type_subtype(top_n, direction, score_type, score_subtype, date, aggregation_method="max", stocks=None, export_format=None):
    year, month = date.year, date.month
    stock_filter = f"AND sim.stock_symbol IN {tuple(stocks)}" if stocks else ""
    connection = psycopg2.connect(DEFAULT_DB_URL)
    cursor = connection.cursor()
    
    result = {}
    
    # Top 1
    query_top1 = f"""
        SELECT t1.score_value AS top_1_score, t1.sectoral_index_id
        FROM top_1_scores t1
        WHERE t1.score_type = '{score_type}'
        AND EXTRACT(YEAR FROM t1.trade_date) = {year}
        AND EXTRACT(MONTH FROM t1.trade_date) = {month}
    """
    cursor.execute(query_top1)
    rows_top1 = cursor.fetchall()
    df_top1 = pd.DataFrame(rows_top1, columns=['top_1_score', 'sectoral_index_id'])
    result['Top 1 Score'] = df_top1['top_1_score'].max() if aggregation_method == "max" else df_top1['top_1_score'].min()
    result['Top 1 Sectors'] = [get_index_name(id, cursor) for id in df_top1['sectoral_index_id'].tolist() if get_index_name(id, cursor)]
    
    if top_n >= 2:
        # Top 2
        query_top2 = f"""
            SELECT t2.score_value AS top_2_score, t2.sectoral_index_id
            FROM top_2_scores t2
            WHERE t2.score_type = '{score_type}'
            AND t2.rank = 2
            AND EXTRACT(YEAR FROM t2.trade_date) = {year}
            AND EXTRACT(MONTH FROM t2.trade_date) = {month}
        """
        cursor.execute(query_top2)
        rows_top2 = cursor.fetchall()
        df_top2 = pd.DataFrame(rows_top2, columns=['top_2_score', 'sectoral_index_id'])
        result['Top 2 Score'] = df_top2['top_2_score'].max() if aggregation_method == "max" else df_top2['top_2_score'].min()
        result['Top 2 Sectors'] = [get_index_name(id, cursor) for id in df_top2['sectoral_index_id'].tolist() if get_index_name(id, cursor)]
    
    if top_n >= 3:
        # Top 3
        query_top3 = f"""
            SELECT t3.score_value AS top_3_score, t3.sectoral_index_id
            FROM top_3_scores t3
            WHERE t3.score_type = '{score_type}'
            AND t3.rank = 3
            AND EXTRACT(YEAR FROM t3.trade_date) = {year}
            AND EXTRACT(MONTH FROM t3.trade_date) = {month}
        """
        cursor.execute(query_top3)
        rows_top3 = cursor.fetchall()
        df_top3 = pd.DataFrame(rows_top3, columns=['top_3_score', 'sectoral_index_id'])
        result['Top 3 Score'] = df_top3['top_3_score'].max() if aggregation_method == "max" else df_top3['top_3_score'].min()
        result['Top 3 Sectors'] = [get_index_name(id, cursor) for id in df_top3['sectoral_index_id'].tolist() if get_index_name(id, cursor)]
    
    cursor.close()
    connection.close()
    
    result_df = pd.DataFrame([result])
    
    if export_format:
        if export_format == "excel":
            result_df.to_excel(f"topbottom_{direction}_{top_n}_{year}_{month}.xlsx", index=False)
        elif export_format == "csv":
            result_df.to_csv(f"topbottom_{direction}_{top_n}_{year}_{month}.csv", index=False)
    
    return result_df

# Example Usage
if __name__ == "__main__":
    date = datetime(2024, 6, 1)
    
    # Test get_score_for_references
    print("get_score_for_references:")
    print(get_score_for_references("RELIANCE", "s", "n1", date, export_format="csv"))
    
    # Test get_all_scores_for_references_for_stock_date
    print("\nget_all_scores_for_references_for_stock_date:")
    print(get_all_scores_for_references_for_stock_date("RELIANCE", date))
    
    # Test get_scores_for_subtype_date
    print("\nget_scores_for_subtype_date:")
    print(get_scores_for_subtype_date("n1", date, stocks=["RELIANCE", "TCS"]))
    
    # Test get_scores_for_type_date
    print("\nget_scores_for_type_date:")
    print(get_scores_for_type_date("s", date))
    
    # Test get_score_summary_by_type
    print("\nget_score_summary_by_type:")
    print(get_score_summary_by_type(date, score_type="s"))
    
    # Test get_score_summary_by_conditions
    print("\nget_score_summary_by_conditions:")
    conditions = {"AND": [{"score_type": "s"}, {"score_value": {">": 0.5}}]}
    print(get_score_summary_by_conditions(conditions, date=date))
    
    # Test get_topbottom_scores_by_type_subtype
    print("\nget_topbottom_scores_by_type_subtype:")
    print(get_topbottom_scores_by_type_subtype(2, "top", "s", "n1", date, export_format="excel"))