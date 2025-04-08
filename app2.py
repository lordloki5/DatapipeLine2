from flask import Flask, request, jsonify, send_file
import psycopg2
import pandas as pd
from datetime import datetime
import os
import io
import sys
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

# Summary API
@app.route('/api/summary', methods=['GET'])
def get_summary():
    try:
        month = request.args.get('month')
        year = request.args.get('year')
        file_format = request.args.get('format', 'json').lower()

        # Validate inputs
        month, year = validate_month_year(month, year)
        if file_format not in ['json', 'excel', 'csv']:
            raise BadRequest("Invalid format. Use 'json', 'excel', or 'csv'.")

        # Connect to database
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get all sectoral indices
        cursor.execute("SELECT index_id, index_name FROM indices WHERE index_id IN (SELECT DISTINCT sectoral_index_id FROM b_ratios)")
        indices = cursor.fetchall()
        index_map = {row[0]: row[1] for row in indices}

        # Query b_ratios for the specified month and year
        query = """
            SELECT sectoral_index_id, close_ratio
            FROM b_ratios
            WHERE EXTRACT(MONTH FROM trade_date) = %s
            AND EXTRACT(YEAR FROM trade_date) = %s
        """
        cursor.execute(query, (month, year))
        ratios = cursor.fetchall()

        # Create a dictionary for the summary row
        summary_row = {index_map.get(index_id, f"Index_{index_id}"): close_ratio for index_id, close_ratio in ratios}
        summary_row['month'] = month
        summary_row['year'] = year

        # Convert to DataFrame
        df = pd.DataFrame([summary_row])

        # Reorder columns to have month and year first
        cols = ['month', 'year'] + [col for col in df.columns if col not in ['month', 'year']]
        df = df[cols]

        # Close database connection
        cursor.close()
        conn.close()

        # Return based on format
        if file_format == 'json':
            return jsonify(summary_row)
        else:
            return generate_file_output(df, file_format, f"summary_{year}_{month}")

    except BadRequest as e:
        return jsonify({"error": str(e)}), 400
    except InternalServerError as e:
        return jsonify({"error": str(e)}), 500
    except Exception as e:
        return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500

# Top/Bottom Scores API
@app.route('/api/topbottom_scores', methods=['GET'])
def get_topbottom_scores():
    try:
        month = request.args.get('month')
        year = request.args.get('year')
        direction = request.args.get('direction').lower()
        direction_n = request.args.get('direction_n')
        subtype = request.args.get('subtype', '').lower()  # Parameter: n1, n2, n3, b1, b2, b3
        file_format = request.args.get('file_format', 'json').lower()  # json, excel, csv

        # Validate inputs
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

        # Connect to database
        conn = get_db_connection()
        cursor = conn.cursor()

        # Determine table prefix
        table_prefix = 'top' if direction == 'top' else 'bottom'

        # Initialize result
        result = {'month': month, 'year': year}
        if subtype:
            result['subtype'] = subtype
        df_data = []

        # Get index names
        cursor.execute("SELECT index_id, index_name FROM indices")
        index_map = {row[0]: row[1] for row in cursor.fetchall()}

        # Helper function to fetch scores and sectors with subtype filtering
        def fetch_scores(table_num, rank=None):
            table_name = f"{table_prefix}_{table_num}_scores"
            query = f"""
                SELECT sectoral_index_id, score_value
                FROM {table_name}
                WHERE EXTRACT(MONTH FROM trade_date) = %s
                AND EXTRACT(YEAR FROM trade_date) = %s
            """
            params = [month, year]
            if subtype:  # Filter by score_type if subtype is provided
                query += " AND score_type = %s"
                params.append(subtype)
            if rank:
                query += " AND rank = %s"
                params.append(rank)
            
            cursor.execute(query, params)
            rows = cursor.fetchall()
            return rows

        # Flag to track if any data was found
        data_found = False

        # Fetch data based on direction_n
        if direction_n >= 1:
            # Fetch all from table 1
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
            # Fetch all from table 2 with rank=2
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
            # Fetch all from table 3 with rank=3
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

        # If no data was found, add a message
        if not data_found:
            message = f"No data found for {table_prefix} scores in {month}/{year}"
            if subtype:
                message += f" with subtype '{subtype}'"
            result['message'] = message

        # Create DataFrame
        df = pd.DataFrame(df_data)

        # Close database connection
        cursor.close()
        conn.close()

        # Return based on file_format
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

from flask import Flask, jsonify, request, send_file
from werkzeug.exceptions import BadRequest, InternalServerError
import pandas as pd
import io
from datetime import datetime

@app.route('/api/topbottom_scores_by_range', methods=['GET'])
def get_topbottom_scores_by_range():
    try:
        start_date = request.args.get('start_date')  # Format: YYYY-MM-DD
        end_date = request.args.get('end_date')      # Format: YYYY-MM-DD
        direction = request.args.get('direction').lower()
        direction_n = request.args.get('direction_n')
        subtype = request.args.get('subtype', '').lower()  # Parameter: n1, n2, n3, b1, b2, b3
        file_format = request.args.get('file_format', 'json').lower()  # json, excel, csv

        # Validate inputs
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

        # Connect to database
        conn = get_db_connection()
        cursor = conn.cursor()

        # Determine table prefix
        table_prefix = 'top' if direction == 'top' else 'bottom'

        # Get index names
        cursor.execute("SELECT index_id, index_name FROM indices")
        index_map = {row[0]: row[1] for row in cursor.fetchall()}

        # Helper function to fetch scores and sectors with subtype filtering
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
            rows = cursor.fetchall()
            return rows

        # Fetch data based on direction_n and organize by year/month
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

        # Prepare result for JSON and DataFrame for Excel/CSV
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

        # If no data found
        if not df_data:
            message = f"No data found for {table_prefix} scores between {start_date.strftime('%Y-%m-%d')} and {end_date.strftime('%Y-%m-%d')}"
            if subtype:
                message += f" with subtype '{subtype}'"
            result['message'] = message

        # Create DataFrame
        df = pd.DataFrame(df_data)

        # Close database connection
        cursor.close()
        conn.close()

        # Return based on file_format
        if file_format == 'json':
            # Format result for JSON
            for entry in df_data:
                key = f"{entry['year']}-{entry['month']:02d}"
                result[key] = {k: v for k, v in entry.items() if k not in ['year', 'month']}
            return jsonify(result)
        else:
            # Ensure column order based on direction_n
            if direction_n == 1:
                columns = ['year', 'month', 'Top 1 Score', 'Top 1 Sectors']
            elif direction_n == 2:
                columns = ['year', 'month', 'Top 1 Score', 'Top 2 Score', 'Top 1 Sectors', 'Top 2 Sectors']
            else:  # direction_n == 3
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

        # Validate inputs
        valid_ratios = ['n1', 'n2', 'n3', 'b1', 'b2', 'b3']
        if ratio_choice not in valid_ratios:
            raise BadRequest(f"Invalid ratio choice. Must be one of {valid_ratios}")
        if file_format not in ['json', 'excel', 'csv']:
            raise BadRequest("file_format must be 'json', 'excel', or 'csv'")

        # Determine table name
        table_name = 'n_ratios' if ratio_choice.startswith('n') else 'b_ratios'

        # Connect to database
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get all indices
        cursor.execute("""
            SELECT index_id, index_name 
            FROM indices 
            ORDER BY index_id
        """)
        indices = cursor.fetchall()
        index_mapping = {idx[0]: idx[1] for idx in indices}

        # Query data
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

        # Process data into a dictionary
        monthly_data = {}
        for trade_date, index_id, ratio_value in data:
            date_str = trade_date.strftime('%Y-%m')
            if date_str not in monthly_data:
                monthly_data[date_str] = {}
            monthly_data[date_str][index_id] = ratio_value

        # Create DataFrame
        dates = sorted(monthly_data.keys())
        df_data = {'trade_date': dates}
        for index_id, index_name in index_mapping.items():
            values = [monthly_data[date].get(index_id, None) for date in dates]
            df_data[index_name] = values

        df = pd.DataFrame(df_data)

        # Prepare JSON result
        result = {'ratio_choice': ratio_choice}
        if df.empty:
            result['message'] = f"No data found for ratio '{ratio_choice}'"
        else:
            result['data'] = {date: {index_mapping[idx]: val for idx, val in monthly_data[date].items()} for date in dates}

        # Close database connection
        cursor.close()
        conn.close()

        # Return based on file_format
        if file_format == 'json':
            return jsonify(result)
        else:
            return generate_file_output(df, file_format, f"monthly_{ratio_choice}_data")

    except BadRequest as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500

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
    
if __name__ == '__main__':
    # Configurable port number (default to 5000)
    port = int(os.environ.get('FLASK_PORT', 8080))
    app.run(debug=True, host='0.0.0.0', port=port)