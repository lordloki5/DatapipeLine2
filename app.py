from flask import Flask, request, jsonify
import psycopg2
import pandas as pd
from datetime import datetime
import calendar

app = Flask(__name__)

DEFAULT_DB_URL = "dbname=ohcldata host=localhost port=5432 user=dhruvbhandari password=''"

def get_db_connection():
    return psycopg2.connect(DEFAULT_DB_URL)

# Helper function to get month start and end dates
def get_month_bounds(date):
    year, month = date.year, date.month
    first_day = datetime(year, month, 1)
    last_day = datetime(year, month, calendar.monthrange(year, month)[1])
    return first_day, last_day

# 1. get_score_forReferences (single date) - unchanged
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
        return df.to_csv() if export_format == 'csv' else df.to_excel()
    
    return jsonify(result)

# 1. get_score_forReferences (date range) - modified
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
    
    # Group by date and index
    scores_by_date = {}
    for trade_date, index_id, score in results:
        month_key = trade_date.strftime('%Y-%m')
        if month_key not in scores_by_date:
            scores_by_date[month_key] = {}
        scores_by_date[month_key][index_id] = score
    
    # Apply aggregation across indices for each month
    aggregated_scores = []
    for month, index_scores in sorted(scores_by_date.items()):
        scores = list(index_scores.values())
        if aggregation_method == 'max':
            aggregated_score = max(scores)
        elif aggregation_method == 'min':
            aggregated_score = min(scores)
        else:  # both
            # Return single value if only one index, otherwise return max and min
            aggregated_score = scores[0] if len(scores) == 1 else {'max': max(scores), 'min': min(scores)}
        aggregated_scores.append({
            "date": month,
            "score": aggregated_score,
            "indices": list(index_scores.keys()) if aggregation_method == 'both' and len(scores) > 1 else None
        })
    
    if export_format in ['excel', 'csv']:
        df = pd.DataFrame(aggregated_scores)
        return df.to_csv() if export_format == 'csv' else df.to_excel()
    
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
    
    # Group by subtype and index
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
    
    # Aggregate across indices
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
    
    # Group by month, subtype, and index
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
    
    # Aggregate across indices for each month and subtype
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
    
    # Group by stock_symbol only, aggregating across all sectors
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
    # Include trade_date and extract year/month
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
    
    # Create DataFrame with year and month
    df = pd.DataFrame(results, columns=['stock', 'year', 'month', 'score'])
    # Create a date column in YYYY-MM format
    df['date'] = df['year'].astype(int).astype(str) + '-' + df['month'].astype(int).astype(str).str.zfill(2)
    
    # Group by stock and month
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
        # Return all scores with dates
        return jsonify(df[['stock', 'date', 'score']].to_dict())

# 4. get_scores_for_type_date
@app.route('/api/get_scores_for_type_date', methods=['GET'])
def get_scores_for_type_date():
    # Input parameters
    score_type = request.args.get('score_type')
    date_str = request.args.get('date', '2018-04-01')
    stocks = request.args.getlist('stocks')
    aggregation_method = request.args.get('aggregation_method', 'max').lower()  # Make case-insensitive
    export_format = request.args.get('export_format')
    
    if not score_type:
        return jsonify({"error": "score_type is required"}), 400
    
    # Convert date and get month bounds
    date = datetime.strptime(date_str, '%Y-%m-%d')
    month_start, month_end = get_month_bounds(date)
    
    # Database connection
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Determine table and subtypes based on score_type
    table = 'n_ratios' if score_type in ['s', 'i', 'p'] else 'b_ratios'
    subtypes = ['n1', 'n2', 'n3'] if table == 'n_ratios' else ['b1', 'b2', 'b3']
    select_clause = ', '.join([f'r.{subtype}' for subtype in subtypes])
    
    # Build SQL query
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
    
    # Execute query
    cur.execute(query, params)
    results = cur.fetchall()
    
    cur.close()
    conn.close()
    
    # Create DataFrame
    df = pd.DataFrame(results, columns=['stock'] + subtypes)
    
    # Apply aggregation across subtypes for each stock
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
        result_df = df  # Return all subtype scores without aggregation
    
    # Export functionality
    if export_format:
        output = BytesIO()
        if export_format.lower() == 'excel':
            result_df.to_excel(output, index=False)
            output.seek(0)
            return send_file(output, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                           download_name=f'scores_{score_type}_{date_str}.xlsx', as_attachment=True)
        elif export_format.lower() == 'csv':
            result_df.to_csv(output, index=False)
            output.seek(0)
            return send_file(output, mimetype='text/csv',
                           download_name=f'scores_{score_type}_{date_str}.csv', as_attachment=True)
        else:
            return jsonify({"error": "Invalid export_format. Use 'excel' or 'csv'"}), 400
    
    # Return JSON response if no export
    return jsonify(result_df.to_dict())

@app.route('/api/get_scores_for_type_range', methods=['GET'])
def get_scores_for_type_range():
    # Input parameters
    score_type = request.args.get('score_type')
    start_date_str = request.args.get('start_date', '2018-04-01')
    end_date_str = request.args.get('end_date', '2018-04-30')
    stocks = request.args.getlist('stocks')
    aggregation_method = request.args.get('aggregation_method', 'max').lower()  # Make case-insensitive
    export_format = request.args.get('export_format')
    
    if not score_type:
        return jsonify({"error": "score_type is required"}), 400
    
    # Convert date range
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
    
    # Database connection
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Determine table and subtypes based on score_type
    table = 'n_ratios' if score_type in ['s', 'i', 'p'] else 'b_ratios'
    subtypes = ['n1', 'n2', 'n3'] if table == 'n_ratios' else ['b1', 'b2', 'b3']
    select_clause = ', '.join([f'r.{subtype}' for subtype in subtypes])
    
    # Build SQL query with month extraction
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
    
    # Execute query
    cur.execute(query, params)
    results = cur.fetchall()
    
    cur.close()
    conn.close()
    
    # Create DataFrame with year and month
    df = pd.DataFrame(results, columns=['stock', 'year', 'month'] + subtypes)
    # Create a date column in YYYY-MM format
    df['date'] = df['year'].astype(int).astype(str) + '-' + df['month'].astype(int).astype(str).str.zfill(2)
    
    # Apply aggregation across subtypes for each stock and month
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
        # Default to max aggregation if method not recognized
        df['score'] = df[subtypes].max(axis=1)
        result_df = df.groupby(['stock', 'date'])['score'].max().reset_index()
        # Alternative: return jsonify({"error": f"Invalid aggregation_method: {aggregation_method}"}), 400
    
    # Export functionality
    if export_format:
        output = BytesIO()
        if export_format.lower() == 'excel':
            result_df.to_excel(output, index=False)
            output.seek(0)
            return send_file(output, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                           download_name=f'scores_{score_type}_{start_date_str}_to_{end_date_str}.xlsx', as_attachment=True)
        elif export_format.lower() == 'csv':
            result_df.to_csv(output, index=False)
            output.seek(0)
            return send_file(output, mimetype='text/csv',
                           download_name=f'scores_{score_type}_{start_date_str}_to_{end_date_str}.csv', as_attachment=True)
        else:
            return jsonify({"error": "Invalid export_format. Use 'excel' or 'csv'"}), 400
    
    # Return JSON response if no export
    return jsonify(result_df.to_dict())


# 5 Summary
@app.route('/api/get_score_summary', methods=['GET'])
def get_score_summary():
    # Input parameters
    date_str = request.args.get('date')
    score_type = request.args.get('score_type')
    score_subtype = request.args.get('score_subtype')
    entity = request.args.get('entity')  # 'stock' or 'sector_index', default to both if omitted
    export_format = request.args.get('export_format')
    # additional_filters = request.args.get('additional_filters')  # Could be parsed as JSON if provided
    
    # Validation
    if not date_str:
        return jsonify({"error": "date is required"}), 400
    if not (score_type or score_subtype):
        return jsonify({"error": "At least one of score_type or score_subtype must be provided"}), 400
    
    # Convert date
    date = datetime.strptime(date_str, '%Y-%m-%d')
    month_start, month_end = get_month_bounds(date)
    
    # Database connection
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Determine table and validate subtype
    if score_subtype:
        table = 'n_ratios' if score_subtype.startswith('n') else 'b_ratios'
        if score_type and score_type in ['s', 'i', 'p'] and not score_subtype.startswith('n'):
            return jsonify({"error": "score_type 's', 'i', 'p' requires subtype starting with 'n'"}), 400
        if score_type and score_type not in ['s', 'i', 'p'] and not score_subtype.startswith('b'):
            return jsonify({"error": "score_type other than 's', 'i', 'p' requires subtype starting with 'b'"}), 400
    elif score_type:
        table = 'n_ratios' if score_type in ['s', 'i', 'p'] else 'b_ratios'
        subtypes = ['n1', 'n2', 'n3'] if table == 'n_ratios' else ['b1', 'b2', 'b3']
        score_subtype = subtypes[0]  # Default to first subtype if only type is provided
    else:
        return jsonify({"error": "Invalid combination of score_type and score_subtype"}), 400
    
    # Build base query components
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
    
    # Adjust query based on entity
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
    else:  # Default: provide both perspectives
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
    
    # TODO: Add additional_filters logic here if provided
    # e.g., if additional_filters.get('sector'): query += " AND sim.sector = %s"; params.append(...)
    
    # Execute query
    cur.execute(query, params)
    results = cur.fetchall()
    
    cur.close()
    conn.close()
    
    # Create DataFrame
    columns = ['entity_id', 'entity_type', 'avg_score', 'min_score', 'max_score', 'score_count'] if entity is None else ['entity_id', 'avg_score', 'min_score', 'max_score', 'score_count']
    df = pd.DataFrame(results, columns=columns)
    
    # Export functionality
    if export_format:
        output = BytesIO()
        if export_format.lower() == 'excel':
            df.to_excel(output, index=False)
            output.seek(0)
            return send_file(output, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                           download_name=f'score_summary_{score_type or ""}_{score_subtype}_{date_str}.xlsx', as_attachment=True)
        elif export_format.lower() == 'csv':
            df.to_csv(output, index=False)
            output.seek(0)
            return send_file(output, mimetype='text/csv',
                           download_name=f'score_summary_{score_type or ""}_{score_subtype}_{date_str}.csv', as_attachment=True)
        else:
            return jsonify({"error": "Invalid export_format. Use 'excel' or 'csv'"}), 400
    
    # Return JSON response
    return jsonify(df.to_dict())


@app.route('/api/get_score_summary_range', methods=['GET'])
def get_score_summary_range():
    # Input parameters
    start_date_str = request.args.get('start_date', '2018-04-01')
    end_date_str = request.args.get('end_date', '2018-04-30')
    score_type = request.args.get('score_type')
    score_subtype = request.args.get('score_subtype')
    entity = request.args.get('entity')  # 'stock' or 'sector_index', default to both if omitted
    export_format = request.args.get('export_format')
    # additional_filters = request.args.get('additional_filters')  # Could be parsed as JSON if provided
    
    # Validation
    if not (start_date_str and end_date_str):
        return jsonify({"error": "start_date and end_date are required"}), 400
    if not (score_type or score_subtype):
        return jsonify({"error": "At least one of score_type or score_subtype must be provided"}), 400
    
    # Convert date range
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
    
    # Database connection
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Determine table and validate subtype
    if score_subtype:
        table = 'n_ratios' if score_subtype.startswith('n') else 'b_ratios'
        if score_type and score_type in ['s', 'i', 'p'] and not score_subtype.startswith('n'):
            return jsonify({"error": "score_type 's', 'i', 'p' requires subtype starting with 'n'"}), 400
        if score_type and score_type not in ['s', 'i', 'p'] and not score_subtype.startswith('b'):
            return jsonify({"error": "score_type other than 's', 'i', 'p' requires subtype starting with 'b'"}), 400
    elif score_type:
        table = 'n_ratios' if score_type in ['s', 'i', 'p'] else 'b_ratios'
        subtypes = ['n1', 'n2', 'n3'] if table == 'n_ratios' else ['b1', 'b2', 'b3']
        score_subtype = subtypes[0]  # Default to first subtype if only type is provided
    else:
        return jsonify({"error": "Invalid combination of score_type and score_subtype"}), 400
    
    # Build base query components with month extraction
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
    
    # Adjust query based on entity
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
    else:  # Default: provide both perspectives
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
    
    # TODO: Add additional_filters logic here if provided
    # e.g., if additional_filters.get('sector'): query += " AND sim.sector = %s"; params.append(...)
    
    # Execute query
    cur.execute(query, params)
    results = cur.fetchall()
    
    cur.close()
    conn.close()
    
    # Create DataFrame with date column
    columns = ['entity_id', 'entity_type', 'year', 'month', 'avg_score', 'min_score', 'max_score', 'score_count'] if entity is None else ['entity_id', 'year', 'month', 'avg_score', 'min_score', 'max_score', 'score_count']
    df = pd.DataFrame(results, columns=columns)
    df['date'] = df['year'].astype(int).astype(str) + '-' + df['month'].astype(int).astype(str).str.zfill(2)
    df = df.drop(['year', 'month'], axis=1)  # Remove temporary columns
    
    # Export functionality
    if export_format:
        output = BytesIO()
        if export_format.lower() == 'excel':
            df.to_excel(output, index=False)
            output.seek(0)
            return send_file(output, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                           download_name=f'score_summary_{score_type or ""}_{score_subtype}_{start_date_str}_to_{end_date_str}.xlsx', as_attachment=True)
        elif export_format.lower() == 'csv':
            df.to_csv(output, index=False)
            output.seek(0)
            return send_file(output, mimetype='text/csv',
                           download_name=f'score_summary_{score_type or ""}_{score_subtype}_{start_date_str}_to_{end_date_str}.csv', as_attachment=True)
        else:
            return jsonify({"error": "Invalid export_format. Use 'excel' or 'csv'"}), 400
    
    # Return JSON response
    return jsonify(df.to_dict())
# 6. get_score_summary_by_conditions
@app.route('/api/get_score_summary_by_conditions', methods=['GET'])
def get_score_summary_by_conditions():
    # Input parameters
    conditions_str = request.args.get('conditions')  # Expecting JSON string
    entity = request.args.get('entity', 'both').lower()  # Default to 'both'
    date_str = request.args.get('date')
    export_format = request.args.get('export_format')
    # additional_filters = request.args.get('additional_filters')  # Could be parsed as JSON
    
    # Validation
    if not conditions_str:
        return jsonify({"error": "conditions parameter is required"}), 400
    
    try:
        conditions = json.loads(conditions_str)
    except json.JSONDecodeError:
        return jsonify({"error": "Invalid JSON format for conditions"}), 400
    
    # Optional date handling
    if date_str:
        date = datetime.strptime(date_str, '%Y-%m-%d')
        month_start, month_end = get_month_bounds(date)
    
    # Database connection
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Build WHERE clause from conditions
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

    # Determine table based on conditions
    score_subtype = None
    score_type = None
    for cond in conditions.get('AND', []) + conditions.get('OR', []):
        if isinstance(cond, dict):
            if 'score_subtype' in cond:
                score_subtype = cond['score_subtype']
            if 'score_type' in cond:
                score_type = cond['score_type']
    
    table = 'n_ratios' if (score_subtype and score_subtype.startswith('n')) or (score_type in ['s', 'i', 'p']) else 'b_ratios'
    score_subtype = score_subtype or ('n1' if table == 'n_ratios' else 'b1')  # Default subtype
    
    # Build query
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
    
    # Adjust query based on entity
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
    else:  # Default: both
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
    
    # TODO: Add additional_filters logic here if provided
    
    # Execute query
    cur.execute(query, params)
    results = cur.fetchall()
    
    cur.close()
    conn.close()
    
    # Create DataFrame
    columns = ['entity_id', 'entity_type', 'avg_score', 'min_score', 'max_score', 'score_count'] if entity == 'both' else ['entity_id', 'avg_score', 'min_score', 'max_score', 'score_count']
    df = pd.DataFrame(results, columns=columns)
    
    # Export functionality
    if export_format:
        output = BytesIO()
        filename = f'score_summary_conditions_{date_str or "all"}_{score_subtype}'
        if export_format.lower() == 'excel':
            df.to_excel(output, index=False)
            output.seek(0)
            return send_file(output, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                           download_name=f'{filename}.xlsx', as_attachment=True)
        elif export_format.lower() == 'csv':
            df.to_csv(output, index=False)
            output.seek(0)
            return send_file(output, mimetype='text/csv',
                           download_name=f'{filename}.csv', as_attachment=True)
        else:
            return jsonify({"error": "Invalid export_format. Use 'excel' or 'csv'"}), 400
    
    # Return JSON response
    return jsonify(df.to_dict())
if __name__ == '__main__':
    app.run(debug=True, port=8080)