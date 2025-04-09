from flask import Flask, request, jsonify, send_file
import psycopg2
import pandas as pd
from datetime import datetime
import calendar
import os
import io
import sys
import json
from werkzeug.exceptions import BadRequest, InternalServerError

app = Flask(__name__)

# Database connection configuration
DEFAULT_DB_URL = "dbname=ohcldata host=localhost port=5432 user=dhruvbhandari password=''"

def get_db_connection():
    try:
        connection = psycopg2.connect(DEFAULT_DB_URL)
        return connection
    except Exception as e:
        raise InternalServerError(f"Database connection failed: {str(e)}")

# Helper function to get month start and end dates
def get_month_bounds(date):
    year, month = date.year, date.month
    first_day = datetime(year, month, 1)
    last_day = datetime(year, month, calendar.monthrange(year, month)[1])
    return first_day, last_day

# Helper function to validate month and year
def validate_month_year(month, year):
    try:
        month = int(month)
        year = int(year)
        if month < 1 or month > 12:
            raise ValueError("Month must be between 1 and 12")
        if year < 1900 or year > 9999:
            raise ValueError("Year must be between 1900 and 9999")
        return month, year
    except ValueError as e:
        raise BadRequest(f"Invalid month or year: {str(e)}")

# Helper function to generate file output
def generate_file_output(df, file_format, filename_prefix):
    output = io.BytesIO()
    if file_format == 'excel':
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='Sheet1')
        output.seek(0)
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=f"{filename_prefix}.xlsx"
        )
    elif file_format == 'csv':
        df.to_csv(output, index=False)
        output.seek(0)
        return send_file(
            output,
            mimetype='text/csv',
            as_attachment=True,
            download_name=f"{filename_prefix}.csv"
        )
    else:
        raise BadRequest("Invalid file format. Use 'excel' or 'csv'.")

# 1. get_score_forReferences (single date)
@app.route('/api/get_score', methods=['GET'])
def get_score():
    stock = request.args.get('stock')
    score_type = request.args.get('score_type')
    score_subtype = request.args.get('score_subtype')
    date_str = request.args.get('date', '2018-04-01')
    aggregation_method = request.args.get('aggregation_method', 'max')
    export_format = request.args.get('export_format')
    
    date = datetime.strptime(date_str, '%Y-%m-%d')
    month_start, month_end = get_month_bounds(date)
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    table = 'n_ratios' if score_type in ['s', 'i', 'p'] and score_subtype.startswith('n') else 'b_ratios'
    score_column = score_subtype
    
    query = f"""
        SELECT r.trade_date, r.sectoral_index_id, r.{score_column}
        FROM {table} r
        JOIN stock_index_mapping sim ON r.sectoral_index_id = sim.index_id
        WHERE sim.stock_symbol = %s
        AND r.trade_date >= %s AND r.trade_date <= %s
        AND r.{score_column} IS NOT NULL
        ORDER BY r.trade_date
    """
    
    cur.execute(query, (stock, month_start, month_end))
    results = cur.fetchall()
    
    cur.close()
    conn.close()
    
    if not results:
        return jsonify({"error": "No scores found"}), 404
    
    scores_by_date = {}
    for trade_date, index_id, score in results:
        month_key = trade_date.strftime('%Y-%m')
        if month_key not in scores_by_date:
            scores_by_date[month_key] = {}
        scores_by_date[month_key][index_id] = score
    
    aggregated_scores = []
    for month, index_scores in scores_by_date.items():
        scores = list(index_scores.values())
        if aggregation_method == 'max':
            aggregated_score = max(scores)
        elif aggregation_method == 'min':
            aggregated_score = min(scores)
        else:  # both
            aggregated_score = scores[0] if len(scores) == 1 else {'max': max(scores), 'min': min(scores)}
        aggregated_scores.append({
            "date": month,
            "score": aggregated_score,
            "indices": list(index_scores.keys()) if aggregation_method == 'both' and len(scores) > 1 else None
        })
    
    result = aggregated_scores[0] if aggregated_scores else {"error": "No scores found"}
    
    if export_format in ['excel', 'csv']:
        df = pd.DataFrame([result] if isinstance(result, dict) else result)
        return generate_file_output(df, export_format, f"score_{stock}_{date_str}")
    
    return jsonify(result)

# 1. get_score_forReferences (date range)
@app.route('/api/get_score_range', methods=['GET'])
def get_score_range():
    stock = request.args.get('stock')
    score_type = request.args.get('score_type')
    score_subtype = request.args.get('score_subtype')
    start_date_str = request.args.get('start_date', '2018-04-01')
    end_date_str = request.args.get('end_date', '2018-04-30')
    aggregation_method = request.args.get('aggregation_method', 'max')
    export_format = request.args.get('export_format')
    
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    table = 'n_ratios' if score_type in ['s', 'i', 'p'] and score_subtype.startswith('n') else 'b_ratios'
    score_column = score_subtype
    
    query = f"""
        SELECT r.trade_date, r.sectoral_index_id, r.{score_column}
        FROM {table} r
        JOIN stock_index_mapping sim ON r.sectoral_index_id = sim.index_id
        WHERE sim.stock_symbol = %s
        AND r.trade_date >= %s AND r.trade_date <= %s
        AND r.{score_column} IS NOT NULL
        ORDER BY r.trade_date
    """
    
    cur.execute(query, (stock, start_date, end_date))
    results = cur.fetchall()
    
    cur.close()
    conn.close()
    
    if not results:
        return jsonify({"error": "No scores found"}), 404
    
    scores_by_date = {}
    for trade_date, index_id, score in results:
        month_key = trade_date.strftime('%Y-%m')
        if month_key not in scores_by_date:
            scores_by_date[month_key] = {}
        scores_by_date[month_key][index_id] = score
    
    aggregated_scores = []
    for month, index_scores in sorted(scores_by_date.items()):
        scores = list(index_scores.values())
        if aggregation_method == 'max':
            aggregated_score = max(scores)
        elif aggregation_method == 'min':
            aggregated_score = min(scores)
        else:  # both
            aggregated_score = scores[0] if len(scores) == 1 else {'max': max(scores), 'min': min(scores)}
        aggregated_scores.append({
            "date": month,
            "score": aggregated_score,
            "indices": list(index_scores.keys()) if aggregation_method == 'both' and len(scores) > 1 else None
        })
    
    if export_format in ['excel', 'csv']:
        df = pd.DataFrame(aggregated_scores)
        return generate_file_output(df, export_format, f"score_range_{stock}_{start_date_str}_to_{end_date_str}")
    
    return jsonify({"scores": aggregated_scores})

# 2. get_all_scores_forReferences (single date)
@app.route('/api/get_all_scores', methods=['GET'])
def get_all_scores():
    stock = request.args.get('stock')
    date_str = request.args.get('date')
    aggregation_method = request.args.get('aggregation_method', 'max')
    
    date = datetime.strptime(date_str, '%Y-%m-%d')
    month_start, month_end = get_month_bounds(date)
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    query = """
        SELECT 
            r.trade_date, 
            r.sectoral_index_id, 
            r.n1, r.n2, r.n3, 
            r.b1, r.b2, r.b3
        FROM (
            SELECT 
                trade_date, 
                sectoral_index_id, 
                n1, n2, n3, 
                NULL AS b1, NULL AS b2, NULL AS b3
            FROM n_ratios
            UNION ALL
            SELECT 
                trade_date, 
                sectoral_index_id, 
                NULL AS n1, NULL AS n2, NULL AS n3, 
                b1, b2, b3
            FROM b_ratios
        ) r
        JOIN stock_index_mapping sim ON r.sectoral_index_id = sim.index_id
        WHERE sim.stock_symbol = %s
        AND r.trade_date >= %s AND r.trade_date <= %s
    """
    
    cur.execute(query, (stock, month_start, month_end))
    results = cur.fetchall()
    
    cur.close()
    conn.close()
    
    if not results:
        return jsonify({"error": "No scores found"}), 404
    
    scores_by_subtype = {
        'n1': {}, 'n2': {}, 'n3': {},
        'b1': {}, 'b2': {}, 'b3': {}
    }
    
    for trade_date, index_id, n1, n2, n3, b1, b2, b3 in results:
        month_key = trade_date.strftime('%Y-%m')
        for subtype, score in [('n1', n1), ('n2', n2), ('n3', n3), 
                              ('b1', b1), ('b2', b2), ('b3', b3)]:
            if score is not None:
                if month_key not in scores_by_subtype[subtype]:
                    scores_by_subtype[subtype][month_key] = {}
                scores_by_subtype[subtype][month_key][index_id] = score
    
    result = {}
    for subtype in scores_by_subtype:
        if scores_by_subtype[subtype]:
            month = list(scores_by_subtype[subtype].keys())[0]  # Single month
            scores = list(scores_by_subtype[subtype][month].values())
            if aggregation_method == 'max':
                result[subtype] = max(scores)
            elif aggregation_method == 'min':
                result[subtype] = min(scores)
            else:  # both
                result[subtype] = {'max': max(scores), 'min': min(scores)}
    
    return jsonify({"date": date.strftime('%Y-%m'), "scores": result})

# 2. get_all_scores_forReferences (date range)
@app.route('/api/get_all_scores_range', methods=['GET'])
def get_all_scores_range():
    stock = request.args.get('stock')
    start_date_str = request.args.get('start_date', '2018-04-01')
    end_date_str = request.args.get('end_date')
    aggregation_method = request.args.get('aggregation_method', 'max')
    
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    query = """
        SELECT 
            r.trade_date, 
            r.sectoral_index_id, 
            r.n1, r.n2, r.n3, 
            r.b1, r.b2, r.b3
        FROM (
            SELECT 
                trade_date, 
                sectoral_index_id, 
                n1, n2, n3, 
                NULL AS b1, NULL AS b2, NULL AS b3
            FROM n_ratios
            UNION ALL
            SELECT 
                trade_date, 
                sectoral_index_id, 
                NULL AS n1, NULL AS n2, NULL AS n3, 
                b1, b2, b3
            FROM b_ratios
        ) r
        JOIN stock_index_mapping sim ON r.sectoral_index_id = sim.index_id
        WHERE sim.stock_symbol = %s
        AND r.trade_date >= %s AND r.trade_date <= %s
    """
    
    cur.execute(query, (stock, start_date, end_date))
    results = cur.fetchall()
    
    cur.close()
    conn.close()
    
    if not results:
        return jsonify({"error": "No scores found"}), 404
    
    scores_by_month_subtype = {}
    for trade_date, index_id, n1, n2, n3, b1, b2, b3 in results:
        month_key = trade_date.strftime('%Y-%m')
        if month_key not in scores_by_month_subtype:
            scores_by_month_subtype[month_key] = {
                'n1': {}, 'n2': {}, 'n3': {},
                'b1': {}, 'b2': {}, 'b3': {}
            }
        for subtype, score in [('n1', n1), ('n2', n2), ('n3', n3), 
                              ('b1', b1), ('b2', b2), ('b3', b3)]:
            if score is not None:
                scores_by_month_subtype[month_key][subtype][index_id] = score
    
    aggregated_scores = []
    for month in sorted(scores_by_month_subtype.keys()):
        month_scores = {"date": month, "scores": {}}
        for subtype in scores_by_month_subtype[month]:
            if scores_by_month_subtype[month][subtype]:
                scores = list(scores_by_month_subtype[month][subtype].values())
                if aggregation_method == 'max':
                    month_scores["scores"][subtype] = max(scores)
                elif aggregation_method == 'min':
                    month_scores["scores"][subtype] = min(scores)
                else:  # both
                    month_scores["scores"][subtype] = {
                        'max': max(scores),
                        'min': min(scores)
                    }
        aggregated_scores.append(month_scores)
    
    return jsonify({"scores": aggregated_scores})

# 3. get_scores_for_subtype_date
@app.route('/api/get_scores_by_subtype', methods=['GET'])
def get_scores_by_subtype():
    score_subtype = request.args.get('score_subtype')
    date_str = request.args.get('date')
    stocks = request.args.getlist('stocks')
    aggregation_method = request.args.get('aggregation_method', 'max')
    
    date = datetime.strptime(date_str, '%Y-%m-%d')
    month_start, month_end = get_month_bounds(date)
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    table = 'n_ratios' if score_subtype.startswith('n') else 'b_ratios'
    query = f"""
        SELECT sim.stock_symbol, r.{score_subtype}
        FROM {table} r
        JOIN stock_index_mapping sim ON r.sectoral_index_id = sim.index_id
        WHERE r.trade_date >= %s AND r.trade_date <= %s
        AND r.{score_subtype} IS NOT NULL
    """
    params = [month_start, month_end]
    if stocks:
        query += " AND sim.stock_symbol IN %s"
        params.append(tuple(stocks))
    
    cur.execute(query, params)
    results = cur.fetchall()
    
    cur.close()
    conn.close()
    
    df = pd.DataFrame(results, columns=['stock', 'score'])
    
    if aggregation_method == 'max':
        aggregated_df = df.groupby('stock')['score'].max().reset_index()
        aggregated_df.columns = ['stock', 'score']
        return jsonify(aggregated_df.to_dict())
    elif aggregation_method == 'min':
        aggregated_df = df.groupby('stock')['score'].min().reset_index()
        aggregated_df.columns = ['stock', 'score']
        return jsonify(aggregated_df.to_dict())
    elif aggregation_method == 'both':
        aggregated_df = df.groupby('stock')['score'].agg(['max', 'min']).reset_index()
        aggregated_df.columns = ['stock', 'max_score', 'min_score']
        return jsonify(aggregated_df.to_dict())
    else:
        return jsonify(df.to_dict())

@app.route('/api/get_scores_by_subtype_range', methods=['GET'])
def get_scores_by_subtype_range():
    score_subtype = request.args.get('score_subtype')
    start_date_str = request.args.get('start_date', '2018-04-01')
    end_date_str = request.args.get('end_date', '2018-04-30')
    stocks = request.args.getlist('stocks')
    aggregation_method = request.args.get('aggregation_method', 'max')
    
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    table = 'n_ratios' if score_subtype.startswith('n') else 'b_ratios'
    query = f"""
        SELECT sim.stock_symbol, 
               EXTRACT(YEAR FROM r.trade_date) AS year,
               EXTRACT(MONTH FROM r.trade_date) AS month,
               r.{score_subtype}
        FROM {table} r
        JOIN stock_index_mapping sim ON r.sectoral_index_id = sim.index_id
        WHERE r.trade_date >= %s AND r.trade_date <= %s
        AND r.{score_subtype} IS NOT NULL
    """
    params = [start_date, end_date]
    if stocks:
        query += " AND sim.stock_symbol IN %s"
        params.append(tuple(stocks))
    
    cur.execute(query, params)
    results = cur.fetchall()
    
    cur.close()
    conn.close()
    
    df = pd.DataFrame(results, columns=['stock', 'year', 'month', 'score'])
    df['date'] = df['year'].astype(int).astype(str) + '-' + df['month'].astype(int).astype(str).str.zfill(2)
    
    if aggregation_method == 'max':
        aggregated_df = df.groupby(['stock', 'date'])['score'].max().reset_index()
        return jsonify(aggregated_df.to_dict())
    elif aggregation_method == 'min':
        aggregated_df = df.groupby(['stock', 'date'])['score'].min().reset_index()
        return jsonify(aggregated_df.to_dict())
    elif aggregation_method == 'both':
        aggregated_df = df.groupby(['stock', 'date'])['score'].agg(['max', 'min']).reset_index()
        aggregated_df.columns = ['stock', 'date', 'max_score', 'min_score']
        return jsonify(aggregated_df.to_dict())
    else:
        return jsonify(df[['stock', 'date', 'score']].to_dict())

# 4. get_scores_for_type_date
@app.route('/api/get_scores_for_type_date', methods=['GET'])
def get_scores_for_type_date():
    score_type = request.args.get('score_type')
    date_str = request.args.get('date', '2018-04-01')
    stocks = request.args.getlist('stocks')
    aggregation_method = request.args.get('aggregation_method', 'max').lower()
    export_format = request.args.get('export_format')
    
    if not score_type:
        return jsonify({"error": "score_type is required"}), 400
    
    date = datetime.strptime(date_str, '%Y-%m-%d')
    month_start, month_end = get_month_bounds(date)
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    table = 'n_ratios' if score_type in ['s', 'i', 'p'] else 'b_ratios'
    subtypes = ['n1', 'n2', 'n3'] if table == 'n_ratios' else ['b1', 'b2', 'b3']
    select_clause = ', '.join([f'r.{subtype}' for subtype in subtypes])
    
    query = f"""
        SELECT sim.stock_symbol, {select_clause}
        FROM {table} r
        JOIN stock_index_mapping sim ON r.sectoral_index_id = sim.index_id
        WHERE r.trade_date >= %s AND r.trade_date <= %s
    """
    params = [month_start, month_end]
    if stocks:
        query += " AND sim.stock_symbol IN %s"
        params.append(tuple(stocks))
    
    cur.execute(query, params)
    results = cur.fetchall()
    
    cur.close()
    conn.close()
    
    df = pd.DataFrame(results, columns=['stock'] + subtypes)
    
    if aggregation_method == 'max':
        df['score'] = df[subtypes].max(axis=1)
        result_df = df.groupby('stock')['score'].max().reset_index()
    elif aggregation_method == 'min':
        df['score'] = df[subtypes].min(axis=1)
        result_df = df.groupby('stock')['score'].min().reset_index()
    elif aggregation_method == 'both':
        df['max_score'] = df[subtypes].max(axis=1)
        df['min_score'] = df[subtypes].min(axis=1)
        result_df = df.groupby('stock').agg({'max_score': 'max', 'min_score': 'min'}).reset_index()
    else:
        result_df = df
    
    if export_format:
        return generate_file_output(result_df, export_format, f"scores_{score_type}_{date_str}")
    
    return jsonify(result_df.to_dict())

@app.route('/api/get_scores_for_type_range', methods=['GET'])
def get_scores_for_type_range():
    score_type = request.args.get('score_type')
    start_date_str = request.args.get('start_date', '2018-04-01')
    end_date_str = request.args.get('end_date', '2018-04-30')
    stocks = request.args.getlist('stocks')
    aggregation_method = request.args.get('aggregation_method', 'max').lower()
    export_format = request.args.get('export_format')
    
    if not score_type:
        return jsonify({"error": "score_type is required"}), 400
    
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    table = 'n_ratios' if score_type in ['s', 'i', 'p'] else 'b_ratios'
    subtypes = ['n1', 'n2', 'n3'] if table == 'n_ratios' else ['b1', 'b2', 'b3']
    select_clause = ', '.join([f'r.{subtype}' for subtype in subtypes])
    
    query = f"""
        SELECT sim.stock_symbol,
               EXTRACT(YEAR FROM r.trade_date) AS year,
               EXTRACT(MONTH FROM r.trade_date) AS month,
               {select_clause}
        FROM {table} r
        JOIN stock_index_mapping sim ON r.sectoral_index_id = sim.index_id
        WHERE r.trade_date >= %s AND r.trade_date <= %s
    """
    params = [start_date, end_date]
    if stocks:
        query += " AND sim.stock_symbol IN %s"
        params.append(tuple(stocks))
    
    cur.execute(query, params)
    results = cur.fetchall()
    
    cur.close()
    conn.close()
    
    df = pd.DataFrame(results, columns=['stock', 'year', 'month'] + subtypes)
    df['date'] = df['year'].astype(int).astype(str) + '-' + df['month'].astype(int).astype(str).str.zfill(2)
    
    if aggregation_method == 'max':
        df['score'] = df[subtypes].max(axis=1)
        result_df = df.groupby(['stock', 'date'])['score'].max().reset_index()
    elif aggregation_method == 'min':
        df['score'] = df[subtypes].min(axis=1)
        result_df = df.groupby(['stock', 'date'])['score'].min().reset_index()
    elif aggregation_method == 'both':
        df['max_score'] = df[subtypes].max(axis=1)
        df['min_score'] = df[subtypes].min(axis=1)
        result_df = df.groupby(['stock', 'date']).agg({'max_score': 'max', 'min_score': 'min'}).reset_index()
    else:
        result_df = df.groupby(['stock', 'date'])['score'].max().reset_index()
    
    if export_format:
        return generate_file_output(result_df, export_format, f"scores_{score_type}_{start_date_str}_to_{end_date_str}")
    
    return jsonify(result_df.to_dict())

# 5. Summary
@app.route('/api/get_score_summary', methods=['GET'])
def get_score_summary():
    date_str = request.args.get('date')
    score_type = request.args.get('score_type')
    score_subtype = request.args.get('score_subtype')
    entity = request.args.get('entity')
    export_format = request.args.get('export_format')
    
    if not date_str:
        return jsonify({"error": "date is required"}), 400
    if not (score_type or score_subtype):
        return jsonify({"error": "At least one of score_type or score_subtype must be provided"}), 400
    
    date = datetime.strptime(date_str, '%Y-%m-%d')
    month_start, month_end = get_month_bounds(date)
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    if score_subtype:
        table = 'n_ratios' if score_subtype.startswith('n') else 'b_ratios'
        if score_type and score_type in ['s', 'i', 'p'] and not score_subtype.startswith('n'):
            return jsonify({"error": "score_type 's', 'i', 'p' requires subtype starting with 'n'"}), 400
        if score_type and score_type not in ['s', 'i', 'p'] and not score_subtype.startswith('b'):
            return jsonify({"error": "score_type other than 's', 'i', 'p' requires subtype starting with 'b'"}), 400
    elif score_type:
        table = 'n_ratios' if score_type in ['s', 'i', 'p'] else 'b_ratios'
        subtypes = ['n1', 'n2', 'n3'] if table == 'n_ratios' else ['b1', 'b2', 'b3']
        score_subtype = subtypes[0]
    else:
        return jsonify({"error": "Invalid combination of score_type and score_subtype"}), 400
    
    select_fields = f"""
        AVG(r.{score_subtype}) as avg_score,
        MIN(r.{score_subtype}) as min_score,
        MAX(r.{score_subtype}) as max_score,
        COUNT(r.{score_subtype}) as score_count
    """
    where_clause = f"""
        WHERE r.trade_date >= %s AND r.trade_date <= %s
        AND r.{score_subtype} IS NOT NULL
    """
    params = [month_start, month_end]
    
    if entity == 'stock':
        query = f"""
            SELECT sim.stock_symbol as entity_id, {select_fields}
            FROM {table} r
            JOIN stock_index_mapping sim ON r.sectoral_index_id = sim.index_id
            {where_clause}
            GROUP BY sim.stock_symbol
        """
    elif entity == 'sector_index':
        query = f"""
            SELECT r.sectoral_index_id as entity_id, {select_fields}
            FROM {table} r
            {where_clause}
            GROUP BY r.sectoral_index_id
        """
    else:
        query_stock = f"""
            SELECT sim.stock_symbol as entity_id, 'stock' as entity_type, {select_fields}
            FROM {table} r
            JOIN stock_index_mapping sim ON r.sectoral_index_id = sim.index_id
            {where_clause}
            GROUP BY sim.stock_symbol
        """
        query_sector = f"""
            SELECT r.sectoral_index_id as entity_id, 'sector_index' as entity_type, {select_fields}
            FROM {table} r
            {where_clause}
            GROUP BY r.sectoral_index_id
        """
        query = f"{query_stock} UNION ALL {query_sector}"
    
    cur.execute(query, params)
    results = cur.fetchall()
    
    cur Popeye.close()
    
    columns = ['entity_id', 'entity_type', 'avg_score', 'min_score', 'max_score', 'score_count'] if entity is None else ['entity_id', 'avg_score', 'min_score', 'max_score', 'score_count']
    df = pd.DataFrame(results, columns=columns)
    
    if export_format:
        return generate_file_output(df, export_format, f"score_summary_{score_type or ''}_{score_subtype}_{date_str}")
    
    return jsonify(df.to_dict())

@app.route('/api/get_score_summary_range', methods=['GET'])
def get_score_summary_range():
    start_date_str = request.args.get('start_date', '2018-04-01')
    end_date_str = request.args.get('end_date', '2018-04-30')
    score_type = request.args.get('score_type')
    score_subtype = request.args.get('score_subtype')
    entity = request.args.get('entity')
    export_format = request.args.get('export_format')
    
    if not (start_date_str and end_date_str):
        return jsonify({"error": "start_date and end_date are required"}), 400
    if not (score_type or score_subtype):
        return jsonify({"error": "At least one of score_type or score_subtype must be provided"}), 400
    
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    if score_subtype:
        table = 'n_ratios' if score_subtype.startswith('n') else 'b_ratios'
        if score_type and score_type in ['s', 'i', 'p'] and not score_subtype.startswith('n'):
            return jsonify({"error": "score_type 's', 'i', 'p' requires subtype starting with 'n'"}), 400
        if score_type and score_type not in ['s', 'i', 'p'] and not score_subtype.startswith('b'):
            return jsonify({"error": "score_type other than 's', 'i', 'p' requires subtype starting with 'b'"}), 400
    elif score_type:
        table = 'n_ratios' if score_type in ['s', 'i', 'p'] else 'b_ratios'
        subtypes = ['n1', 'n2', 'n3'] if table == 'n_ratios' else ['b1', 'b2', 'b3']
        score_subtype = subtypes[0]
    else:
        return jsonify({"error": "Invalid combination of score_type and score_subtype"}), 400
    
    select_fields = f"""
        EXTRACT(YEAR FROM r.trade_date) AS year,
        EXTRACT(MONTH FROM r.trade_date) AS month,
        AVG(r.{score_subtype}) as avg_score,
        MIN(r.{score_subtype}) as min_score,
        MAX(r.{score_subtype}) as max_score,
        COUNT(r.{score_subtype}) as score_count
    """
    where_clause = f"""
        WHERE r.trade_date >= %s AND r.trade_date <= %s
        AND r.{score_subtype} IS NOT NULL
    """
    params = [start_date, end_date]
    
    if entity == 'stock':
        query = f"""
            SELECT sim.stock_symbol as entity_id, {select_fields}
            FROM {table} r
            JOIN stock_index_mapping sim ON r.sectoral_index_id = sim.index_id
            {where_clause}
            GROUP BY sim.stock_symbol, EXTRACT(YEAR FROM r.trade_date), EXTRACT(MONTH FROM r.trade_date)
        """
    elif entity == 'sector_index':
        query = f"""
            SELECT r.sectoral_index_id as entity_id, {select_fields}
            FROM {table} r
            {where_clause}
            GROUP BY r.sectoral_index_id, EXTRACT(YEAR FROM r.trade_date), EXTRACT(MONTH FROM r.trade_date)
        """
    else:
        query_stock = f"""
            SELECT sim.stock_symbol as entity_id, 'stock' as entity_type, {select_fields}
            FROM {table} r
            JOIN stock_index_mapping sim ON r.sectoral_index_id = sim.index_id
            {where_clause}
            GROUP BY sim.stock_symbol, EXTRACT(YEAR FROM r.trade_date), EXTRACT(MONTH FROM r.trade_date)
        """
        query_sector = f"""
            SELECT r.sectoral_index_id as entity_id, 'sector_index' as entity_type, {select_fields}
            FROM {table} r
            {where_clause}
            GROUP BY r.sectoral_index_id, EXTRACT(YEAR FROM r.trade_date), EXTRACT(MONTH FROM r.trade_date)
        """
        query = f"{query_stock} UNION ALL {query_sector}"
    
    cur.execute(query, params)
    results = cur.fetchall()
    
    cur.close()
    conn.close()
    
    columns = ['entity_id', 'entity_type', 'year', 'month', 'avg_score', 'min_score', 'max_score', 'score_count'] if entity is None else ['entity_id', 'year', 'month', 'avg_score', 'min_score', 'max_score', 'score_count']
    df = pd.DataFrame(results, columns=columns)
    df['date'] = df['year'].astype(int).astype(str) + '-' + df['month'].astype(int).astype(str).str.zfill(2)
    df = df.drop(['year', 'month'], axis=1)
    
    if export_format:
        return generate_file_output(df, export_format, f"score_summary_{score_type or ''}_{score_subtype}_{start_date_str}_to_{end_date_str}")
    
    return jsonify(df.to_dict())

# 6. get_score_summary_by_conditions
@app.route('/api/get_score_summary_by_conditions', methods=['GET'])
def get_score_summary_by_conditions():
    conditions_str = request.args.get('conditions')
    entity = request.args.get('entity', 'both').lower()
    date_str = request.args.get('date')
    export_format = request.args.get('export_format')
    
    if not conditions_str:
        return jsonify({"error": "conditions parameter is required"}), 400
    
    try:
        conditions = json.loads(conditions_str)
    except json.JSONDecodeError:
        return jsonify({"error": "Invalid JSON format for conditions"}), 400
    
    if date_str:
        date = datetime.strptime(date_str, '%Y-%m-%d')
        month_start, month_end = get_month_bounds(date)
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    def build_conditions_clause(conditions, params):
        if isinstance(conditions, dict):
            if 'AND' in conditions:
                clauses = [build_conditions_clause(cond, params) for cond in conditions['AND']]
                return ' AND '.join(f'({clause})' for clause in clauses if clause)
            elif 'OR' in conditions:
                clauses = [build_conditions_clause(cond, params) for cond in conditions['OR']]
                return ' OR '.join(f'({clause})' for clause in clauses if clause)
            else:
                for key, value in conditions.items():
                    if key in ['score_type', 'score_subtype']:
                        params.append(value)
                        return f"r.{key} = %s"
                    elif key == 'score_value':
                        if isinstance(value, dict):
                            for op, val in value.items():
                                params.append(val)
                                if op == '>': return "r.score_value > %s"
                                elif op == '<': return "r.score_value < %s"
                                elif op == '=': return "r.score_value = %s"
                                elif op == '>=': return "r.score_value >= %s"
                                elif op == '<=': return "r.score_value <= %s"
        return ""

    score_subtype = None
    score_type = None
    for cond in conditions.get('AND', []) + conditions.get('OR', []):
        if isinstance(cond, dict):
            if 'score_subtype' in cond:
                score_subtype = cond['score_subtype']
            if 'score_type' in cond:
                score_type = cond['score_type']
    
    table = 'n_ratios' if (score_subtype and score_subtype.startswith('n')) or (score_type in ['s', 'i', 'p']) else 'b_ratios'
    score_subtype = score_subtype or ('n1' if table == 'n_ratios' else 'b1')
    
    select_fields = f"""
        AVG(r.{score_subtype}) as avg_score,
        MIN(r.{score_subtype}) as min_score,
        MAX(r.{score_subtype}) as max_score,
        COUNT(r.{score_subtype}) as score_count
    """
    params = []
    where_clause = build_conditions_clause(conditions, params)
    if date_str:
        where_clause += " AND r.trade_date >= %s AND r.trade_date <= %s"
        params.extend([month_start, month_end])
    if where_clause:
        where_clause = f"WHERE {where_clause} AND r.{score_subtype} IS NOT NULL"
    else:
        where_clause = f"WHERE r.{score_subtype} IS NOT NULL"
    
    if entity == 'stock':
        query = f"""
            SELECT sim.stock_symbol as entity_id, {select_fields}
            FROM {table} r
            JOIN stock_index_mapping sim ON r.sectoral_index_id = sim.index_id
            {where_clause}
            GROUP BY sim.stock_symbol
        """
    elif entity == 'sector_index':
        query = f"""
            SELECT r.sectoral_index_id as entity_id, {select_fields}
            FROM {table} r
            {where_clause}
            GROUP BY r.sectoral_index_id
        """
    else:
        query_stock = f"""
            SELECT sim.stock_symbol as entity_id, 'stock' as entity_type, {select_fields}
            FROM {table} r
            JOIN stock_index_mapping sim ON r.sectoral_index_id = sim.index_id
            {where_clause}
            GROUP BY sim.stock_symbol
        """
        query_sector = f"""
            SELECT r.sectoral_index_id as entity_id, 'sector_index' as entity_type, {select_fields}
            FROM {table} r
            {where_clause}
            GROUP BY r.sectoral_index_id
        """
        query = f"{query_stock} UNION ALL {query_sector}"
    
    cur.execute(query, params)
    results = cur.fetchall()
    
    cur.close()
    conn.close()
    
    columns = ['entity_id', 'entity_type', 'avg_score', 'min_score', 'max_score', 'score_count'] if entity == 'both' else ['entity_id', 'avg_score', 'min_score', 'max_score', 'score_count']
    df = pd.DataFrame(results, columns=columns)
    
    if export_format:
        return generate_file_output(df, export_format, f"score_summary_conditions_{date_str or 'all'}_{score_subtype}")
    
    return jsonify(df.to_dict())

# Top/Bottom Scores API
@app.route('/api/topbottom_scores', methods=['GET'])
def get_topbottom_scores():
    try:
        month = request.args.get('month')
        year = request.args.get('year')
        direction = request.args.get('direction').lower()
        direction_n = request.args.get('direction_n')
        subtype = request.args.get('subtype', '').lower()
        file_format = request.args.get('file_format', 'json').lower()

        month, year = validate_month_year(month, year)
        if direction not in ['top', 'bottom']:
            raise BadRequest("Direction must be 'top' or 'bottom'")
        try:
            direction_n = int(direction_n)
            if direction_n not in [1, 2, 3]:
                raise ValueError
        except ValueError:
            raise BadRequest("direction_n must be 1, 2, or 3")
        valid_subtypes = ['n1', 'n2', 'n3', 'b1', 'b2', 'b3', '']
        if subtype not in valid_subtypes:
            raise BadRequest("Subtype must be one of 'n1', 'n2', 'n3', 'b1', 'b2', 'b3', or empty")
        if file_format not in ['json', 'excel', 'csv']:
            raise BadRequest("file_format must be 'json', 'excel', or 'csv'")

        conn = get_db_connection()
        cursor = conn.cursor()

        table_prefix = 'top' if direction == 'top' else 'bottom'

        result = {'month': month, 'year': year}
        if subtype:
            result['subtype'] = subtype
        df_data = []

        cursor.execute("SELECT index_id, index_name FROM indices")
        index_map = {row[0]: row[1] for row in cursor.fetchall()}

        def fetch_scores(table_num, rank=None):
            table_name = f"{table_prefix}_{table_num}_scores"
            query = f"""
                SELECT sectoral_index_id, score_value
                FROM {table_name}
                WHERE EXTRACT(MONTH FROM trade_date) = %s
                AND EXTRACT(YEAR FROM trade_date) = %s
            """
            params = [month, year]
            if subtype:
                query += " AND score_type = %s"
                params.append(subtype)
            if rank:
                query += " AND rank = %s"
                params.append(rank)
            
            cursor.execute(query, params)
            return cursor.fetchall()

        data_found = False

        if direction_n >= 1:
            rows = fetch_scores(1)
            if rows:
                data_found = True
                scores = [row[1] for row in rows]
                sectors = [index_map.get(row[0], f"Index_{row[0]}") for row in rows]
                result[f"{table_prefix}_1_scores"] = scores
                result[f"{table_prefix}_1_sectors"] = sectors
                for index_id, score in rows:
                    df_data.append({
                        'month': month,
                        'year': year,
                        'type': f"{table_prefix}_1",
                        'score': score,
                        'sector': index_map.get(index_id, f"Index_{index_id}")
                    })
            else:
                result[f"{table_prefix}_1_scores"] = []
                result[f"{table_prefix}_1_sectors"] = []

        if direction_n >= 2:
            rows = fetch_scores(2, rank=2)
            if rows:
                data_found = True
                scores = [row[1] for row in rows]
                sectors = [index_map.get(row[0], f"Index_{row[0]}") for row in rows]
                result[f"{table_prefix}_2_scores"] = scores
                result[f"{table_prefix}_2_sectors"] = sectors
                for index_id, score in rows:
                    df_data.append({
                        'month': month,
                        'year': year,
                        'type': f"{table_prefix}_2",
                        'score': score,
                        'sector': index_map.get(index_id, f"Index_{index_id}")
                    })
            else:
                result[f"{table_prefix}_2_scores"] = []
                result[f"{table_prefix}_2_sectors"] = []

        if direction_n == 3:
            rows = fetch_scores(3, rank=3)
            if rows:
                data_found = True
                scores = [row[1] for row in rows]
                sectors = [index_map.get(row[0], f"Index_{row[0]}") for row in rows]
                result[f"{table_prefix}_3_scores"] = scores
                result[f"{table_prefix}_3_sectors"] = sectors
                for index_id, score in rows:
                    df_data.append({
                        'month': month,
                        'year': year,
                        'type': f"{table_prefix}_3",
                        'score': score,
                        'sector': index_map.get(index_id, f"Index_{index_id}")
                    })
            else:
                result[f"{table_prefix}_3_scores"] = []
                result[f"{table_prefix}_3_sectors"] = []

        if not data_found:
            message = f"No data found for {table_prefix} scores in {month}/{year}"
            if subtype:
                message += f" with subtype '{subtype}'"
            result['message'] = message

        df = pd.DataFrame(df_data)

        cursor.close()
        conn.close()

        if file_format == 'json':
            return jsonify(result)
        else:
            return generate_file_output(df, file_format, f"{table_prefix}_scores_{year}_{month}")

    except BadRequest as e:
        return jsonify({"error": str(e)}), 400
    except InternalServerError as e:
        return jsonify({"error": str(e)}), 500
    except Exception as e:
        return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500 

@app.route('/api/topbottom_scores_by_range', methods=['GET'])
def get_topbottom_scores_by_range():
    try:
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        direction = request.args.get('direction').lower()
        direction_n = request.args.get('direction_n')
        subtype = request.args.get('subtype', '').lower()
        file_format = request.args.get('file_format', 'json').lower()

        try:
            start_date = datetime.strptime(start_date, '%Y-%m-%d')
            end_date = datetime.strptime(end_date, '%Y-%m-%d')
            if start_date > end_date:
                raise ValueError("start_date must be before or equal to end_date")
        except (ValueError, TypeError):
            raise BadRequest("start_date and end_date must be in YYYY-MM-DD format, and start_date must be <= end_date")
        
        if direction not in ['top', 'bottom']:
            raise BadRequest("Direction must be 'top' or 'bottom'")
        try:
            direction_n = int(direction_n)
            if direction_n not in [1, 2, 3]:
                raise ValueError
        except ValueError:
            raise BadRequest("direction_n must be 1, 2, or 3")
        valid_subtypes = ['n1', 'n2', 'n3', 'b1', 'b2', 'b3', '']
        if subtype not in valid_subtypes:
            raise BadRequest("Subtype must be one of 'n1', 'n2', 'n3', 'b1', 'b2', 'b3', or empty")
        if file_format not in ['json', 'excel', 'csv']:
            raise BadRequest("file_format must be 'json', 'excel', or 'csv'")

        conn = get_db_connection()
        cursor = conn.cursor()

        table_prefix = 'top' if direction == 'top' else 'bottom'

        cursor.execute("SELECT index_id, index_name FROM indices")
        index_map = {row[0]: row[1] for row in cursor.fetchall()}

        def fetch_scores(table_num, rank=None):
            table_name = f"{table_prefix}_{table_num}_scores"
            query = f"""
                SELECT EXTRACT(YEAR FROM trade_date) AS year,
                       EXTRACT(MONTH FROM trade_date) AS month,
                       sectoral_index_id, score_value
                FROM {table_name}
                WHERE trade_date >= %s
                AND trade_date <= %s
            """
            params = [start_date, end_date]
            if subtype:
                query += " AND score_type = %s"
                params.append(subtype)
            if rank:
                query += " AND rank = %s"
                params.append(rank)
            
            cursor.execute(query, params)
            return cursor.fetchall()

        data_by_month = {}
        if direction_n >= 1:
            rows = fetch_scores(1)
            for year, month, index_id, score in rows:
                key = (int(year), int(month))
                if key not in data_by_month:
                    data_by_month[key] = {'top_1_scores': [], 'top_1_sectors': []}
                data_by_month[key]['top_1_scores'].append(score)
                data_by_month[key]['top_1_sectors'].append(index_map.get(index_id, f"Index_{index_id}"))

        if direction_n >= 2:
            rows = fetch_scores(2, rank=2)
            for year, month, index_id, score in rows:
                key = (int(year), int(month))
                if key not in data_by_month:
                    data_by_month[key] = {'top_1_scores': [], 'top_1_sectors': []}
                data_by_month[key]['top_2_scores'] = data_by_month[key].get('top_2_scores', []) + [score]
                data_by_month[key]['top_2_sectors'] = data_by_month[key].get('top_2_sectors', []) + [index_map.get(index_id, f"Index_{index_id}")]

        if direction_n == 3:
            rows = fetch_scores(3, rank=3)
            for year, month, index_id, score in rows:
                key = (int(year), int(month))
                if key not in data_by_month:
                    data_by_month[key] = {'top_1_scores': [], 'top_1_sectors': []}
                data_by_month[key]['top_3_scores'] = data_by_month[key].get('top_3_scores', []) + [score]
                data_by_month[key]['top_3_sectors'] = data_by_month[key].get('top_3_sectors', []) + [index_map.get(index_id, f"Index_{index_id}")]

        result = {'start_date': start_date.strftime('%Y-%m-%d'), 'end_date': end_date.strftime('%Y-%m-%d')}
        if subtype:
            result['subtype'] = subtype
        
        df_data = []
        for (year, month), data in sorted(data_by_month.items()):
            entry = {'year': year, 'month': month}
            if 'top_1_scores' in data:
                entry['Top 1 Score'] = max(data['top_1_scores']) if data['top_1_scores'] else None
                entry['Top 1 Sectors'] = ', '.join(data['top_1_sectors'])
            if direction_n >= 2 and 'top_2_scores' in data:
                entry['Top 2 Score'] = max(data['top_2_scores']) if data['top_2_scores'] else None
                entry['Top 2 Sectors'] = ', '.join(data['top_2_sectors'])
            if direction_n == 3 and 'top_3_scores' in data:
                entry['Top 3 Score'] = max(data['top_3_scores']) if data['top_3_scores'] else None
                entry['Top 3 Sectors'] = ', '.join(data['top_3_sectors'])
            df_data.append(entry)

        if not df_data:
            message = f"No data found for {table_prefix} scores between {start_date.strftime('%Y-%m-%d')} and {end_date.strftime('%Y-%m-%d')}"
            if subtype:
                message += f" with subtype '{subtype}'"
            result['message'] = message

        df = pd.DataFrame(df_data)

        cursor.close()
        conn.close()

        if file_format == 'json':
            for entry in df_data:
                key = f"{entry['year']}-{entry['month']:02d}"
                result[key] = {k: v for k, v in entry.items() if k not in ['year', 'month']}
            return jsonify(result)
        else:
            if direction_n == 1:
                columns = ['year', 'month', 'Top 1 Score', 'Top 1 Sectors']
            elif direction_n == 2:
                columns = ['year', 'month', 'Top 1 Score', 'Top 2 Score', 'Top 1 Sectors', 'Top 2 Sectors']
            else:
                columns = ['year', 'month', 'Top 1 Score', 'Top 2 Score', 'Top 3 Score', 'Top 1 Sectors', 'Top 2 Sectors', 'Top 3 Sectors']
            df = df[columns] if not df.empty else pd.DataFrame(columns=columns)
            return generate_file_output(df, file_format, f"{table_prefix}_scores_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}")

    except BadRequest as e:
        return jsonify({"error": str(e)}), 400
    except InternalServerError as e:
        return jsonify({"error": str(e)}), 500
    except Exception as e:
        return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500

@app.route('/api/ratio_data', methods=['GET'])
def get_ratio_data():
    try:
        ratio_choice = request.args.get('ratio_choice', 'n1').lower()
        file_format = request.args.get('file_format', 'json').lower()

        valid_ratios = ['n1', 'n2', 'n3', 'b1', 'b2', 'b3']
        if ratio_choice not in valid_ratios:
            raise BadRequest(f"Invalid ratio choice. Must be one of {valid_ratios}")
        if file_format not in ['json', 'excel', 'csv']:
            raise BadRequest("file_format must be 'json', 'excel', or 'csv'")

        table_name = 'n_ratios' if ratio_choice.startswith('n') else 'b_ratios'

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT index_id, index_name 
            FROM indices 
            WHERE index_id != 1 AND index_id != 2
            ORDER BY index_id
        """)
        indices = cursor.fetchall()
        index_mapping = {idx[0]: idx[1] for idx in indices}

        query = f"""
            SELECT 
                trade_date,
                sectoral_index_id,
                {ratio_choice}
            FROM {table_name}
            WHERE {ratio_choice} IS NOT NULL
            ORDER BY trade_date, sectoral_index_id
        """
        cursor.execute(query)
        data = cursor.fetchall()

        monthly_data = {}
        for trade_date, index_id, ratio_value in data:
            date_str = trade_date.strftime('%Y-%m')
            if date_str not in monthly_data:
                monthly_data[date_str] = {}
            monthly_data[date_str][index_id] = ratio_value

        dates = sorted(monthly_data.keys())
        df_data = {'trade_date': dates}
        for index_id, index_name in index_mapping.items():
            values = [monthly_data[date].get(index_id, None) for date in dates]
            df_data[index_name] = values

        df = pd.DataFrame(df_data)

        result = {'ratio_choice': ratio_choice}
        if df.empty:
            result['message'] = f"No data found for ratio '{ratio_choice}'"
        else:
            result['data'] = {date: {index_mapping[idx]: val for idx, val in monthly_data[date].items()} for date in dates}

        cursor.close()
        conn.close()

        if file_format == 'json':
            return jsonify(result)
        else:
            return generate_file_output(df, file_format, f"monthly_{ratio_choice}_data")

    except BadRequest as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500

@app.route('/api/ratio_data_by_range', methods=['GET'])
def get_ratio_data_by_range():
    try:
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        ratio_choice = request.args.get('ratio_choice', 'n1').lower()
        file_format = request.args.get('file_format', 'json').lower()

        try:
            start_date = datetime.strptime(start_date, '%Y-%m-%d')
            end_date = datetime.strptime(end_date, '%Y-%m-%d')
            if start_date > end_date:
                raise ValueError("start_date must be before or equal to end_date")
        except (ValueError, TypeError):
            raise BadRequest("start_date and end_date must be in YYYY-MM-DD format, and start_date must be <= end_date")
        
        valid_ratios = ['n1', 'n2', 'n3', 'b1', 'b2', 'b3']
        if ratio_choice not in valid_ratios:
            raise BadRequest(f"Invalid ratio choice. Must be one of {valid_ratios}")
        if file_format not in ['json', 'excel', 'csv']:
            raise BadRequest("file_format must be 'json', 'excel', or 'csv'")

        table_name = 'n_ratios' if ratio_choice.startswith('n') else 'b_ratios'

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT index_id, index_name 
            FROM indices 
            WHERE index_id != 1 AND index_id != 2
            ORDER BY index_id
        """)
        indices = cursor.fetchall()
        index_mapping = {idx[0]: idx[1] for idx in indices}

        query = f"""
            SELECT 
                trade_date,
                sectoral_index_id,
                {ratio_choice}
            FROM {table_name}
            WHERE {ratio_choice} IS NOT NULL
            AND trade_date >= %s
            AND trade_date <= %s
            ORDER BY trade_date, sectoral_index_id
        """
        cursor.execute(query, (start_date, end_date))
        data = cursor.fetchall()

        monthly_data = {}
        for trade_date, index_id, ratio_value in data:
            date_str = trade_date.strftime('%Y-%m')
            if date_str not in monthly_data:
                monthly_data[date_str] = {}
            monthly_data[date_str][index_id] = ratio_value

        dates = sorted(monthly_data.keys())
        df_data = {'trade_date': dates}
        for index_id, index_name in index_mapping.items():
            values = [monthly_data[date].get(index_id, None) for date in dates]
            df_data[index_name] = values

        df = pd.DataFrame(df_data)

        result = {
            'start_date': start_date.strftime('%Y-%m-%d'),
            'end_date': end_date.strftime('%Y-%m-%d'),
            'ratio_choice': ratio_choice
        }
        if df.empty:
            result['message'] = f"No data found for ratio '{ratio_choice}' between {start_date.strftime('%Y-%m-%d')} and {end_date.strftime('%Y-%m-%d')}"
        else:
            result['data'] = {date: {index_mapping[idx]: val for idx, val in monthly_data[date].items()} for date in dates}

        cursor.close()
        conn.close()

        if file_format == 'json':
            return jsonify(result)
        else:
            return generate_file_output(df, file_format, f"monthly_{ratio_choice}_data_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}")

    except BadRequest as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500

if __name__ == '__main__':
    port = int(os.environ.get('FLASK_PORT', 8080))
    app.run(debug=True, host='0.0.0.0', port=port)