from flask import Flask, request, jsonify, make_response
from flask_sqlalchemy import SQLAlchemy
import pandas as pd
from datetime import datetime
import json
from sqlalchemy import extract
import io

app = Flask(__name__)

# PostgreSQL connection string
DEFAULT_DB_URL = "postgresql://dhruvbhandari:@localhost:5432/ohcldata"
app.config['SQLALCHEMY_DATABASE_URI'] = DEFAULT_DB_URL
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

# Define Models
class StockIndexMapping(db.Model):
    __tablename__ = 'stock_index_mapping'
    mapping_id = db.Column(db.Integer, primary_key=True)
    stock_symbol = db.Column(db.String(20), nullable=False)
    index_id = db.Column(db.Integer, db.ForeignKey('indices.index_id'), nullable=False)

class Indices(db.Model):
    __tablename__ = 'indices'
    index_id = db.Column(db.Integer, primary_key=True)
    index_name = db.Column(db.String(50), nullable=False)
    exchange = db.Column(db.String(10), nullable=False)

class NRatios(db.Model):
    __tablename__ = 'n_ratios'
    trade_date = db.Column(db.DateTime, nullable=False)
    sectoral_index_id = db.Column(db.Integer, db.ForeignKey('indices.index_id'), nullable=False, primary_key=True)
    benchmark_index_id = db.Column(db.Integer, db.ForeignKey('indices.index_id'), nullable=False)
    open_ratio = db.Column(db.Numeric, nullable=False)
    high_ratio = db.Column(db.Numeric, nullable=False)
    low_ratio = db.Column(db.Numeric, nullable=False)
    close_ratio = db.Column(db.Numeric, nullable=False)
    tag = db.Column(db.String(20))
    n1 = db.Column(db.Integer)
    n2 = db.Column(db.Integer)
    n3 = db.Column(db.Integer)

class BRatios(db.Model):
    __tablename__ = 'b_ratios'
    trade_date = db.Column(db.DateTime, nullable=False)
    sectoral_index_id = db.Column(db.Integer, db.ForeignKey('indices.index_id'), nullable=False, primary_key=True)
    benchmark_index_id = db.Column(db.Integer, db.ForeignKey('indices.index_id'), nullable=False)
    open_ratio = db.Column(db.Numeric, nullable=False)
    high_ratio = db.Column(db.Numeric, nullable=False)
    low_ratio = db.Column(db.Numeric, nullable=False)
    close_ratio = db.Column(db.Numeric, nullable=False)
    tag = db.Column(db.String(20))
    b1 = db.Column(db.Integer)
    b2 = db.Column(db.Integer)
    b3 = db.Column(db.Integer)

# Helper function to validate month and year
def validate_month_year(month, year):
    if month is None or year is None:
        return False, "Month and year are required"
    if not isinstance(month, int) or not isinstance(year, int):
        return False, "Month and year must be integers"
    if not (1 <= month <= 12):
        return False, "Month must be between 1 and 12"
    if year < 1900 or year > 9999:
        return False, "Year must be between 1900 and 9999"
    return True, ""

# API 1: get_score_forReferences
@app.route('/api/get_score', methods=['GET'])
def get_score():
    stock = request.args.get('stock')
    score_type = request.args.get('score_type')  # Added as per PDF, not used
    score_subtype = request.args.get('score_subtype')
    month = request.args.get('month', type=int)
    year = request.args.get('year', type=int)
    aggregation_method = request.args.get('aggregation_method', 'max')
    export_format = request.args.get('export_format')

    valid, error = validate_month_year(month, year)
    if not valid:
        return jsonify({"error": error}), 400
    if not stock or not score_subtype or not score_type:
        return jsonify({"error": "Stock, score_type, and score_subtype are required"}), 400

    mappings = StockIndexMapping.query.filter_by(stock_symbol=stock).all()
    scores = []
    
    for mapping in mappings:
        if score_subtype in ['n1', 'n2', 'n3']:
            ratio = NRatios.query.filter(
                NRatios.sectoral_index_id == mapping.index_id,
                extract('month', NRatios.trade_date) == month,
                extract('year', NRatios.trade_date) == year
            ).first()
            score = getattr(ratio, score_subtype) if ratio else None
        elif score_subtype in ['b1', 'b2', 'b3']:
            ratio = BRatios.query.filter(
                BRatios.sectoral_index_id == mapping.index_id,
                extract('month', BRatios.trade_date) == month,
                extract('year', BRatios.trade_date) == year
            ).first()
            score = getattr(ratio, score_subtype) if ratio else None
        if score is not None:
            scores.append(score)
    
    if not scores:
        return jsonify({"error": "No scores found"}), 404
    
    if aggregation_method == 'max':
        result = max(scores)
    elif aggregation_method == 'min':
        result = min(scores)
    else:  # 'both'
        result = scores
    
    if export_format in ['excel', 'csv']:
        df = pd.DataFrame([result] if isinstance(result, (int, float)) else result, columns=['score'])
        if export_format == 'csv':
            output = io.StringIO()
            df.to_csv(output, index=False)
            return make_response(output.getvalue(), 200, {'Content-Type': 'text/csv', 'Content-Disposition': 'attachment; filename=scores.csv'})
        else:  # excel
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, index=False)
            output.seek(0)
            return make_response(output.getvalue(), 200, {'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', 'Content-Disposition': 'attachment; filename=scores.xlsx'})
    return jsonify({"score": result})

# API 2: get_all_scores_forReferences
@app.route('/api/get_all_scores', methods=['GET'])
def get_all_scores():
    stock = request.args.get('stock')
    month = request.args.get('month', type=int)
    year = request.args.get('year', type=int)
    aggregation_method = request.args.get('aggregation_method', 'max')
    export_format = request.args.get('export_format')

    valid, error = validate_month_year(month, year)
    if not valid:
        return jsonify({"error": error}), 400
    if not stock:
        return jsonify({"error": "Stock is required"}), 400

    mappings = StockIndexMapping.query.filter_by(stock_symbol=stock).all()
    all_scores = {}
    
    for mapping in mappings:
        n_ratio = NRatios.query.filter(
            NRatios.sectoral_index_id == mapping.index_id,
            extract('month', NRatios.trade_date) == month,
            extract('year', NRatios.trade_date) == year
        ).first()
        b_ratio = BRatios.query.filter(
            BRatios.sectoral_index_id == mapping.index_id,
            extract('month', BRatios.trade_date) == month,
            extract('year', BRatios.trade_date) == year
        ).first()
        if n_ratio:
            all_scores.update({f"s_n{key}": getattr(n_ratio, key) for key in ['n1', 'n2', 'n3'] if getattr(n_ratio, key) is not None})
        if b_ratio:
            all_scores.update({f"s_b{key}": getattr(b_ratio, key) for key in ['b1', 'b2', 'b3'] if getattr(b_ratio, key) is not None})
    
    if not all_scores:
        return jsonify({"error": "No scores found"}), 404
    
    if aggregation_method != 'both':
        values = list(all_scores.values())
        result = max(values) if aggregation_method == 'max' else min(values)
    else:
        result = all_scores
    
    if export_format in ['excel', 'csv']:
        df = pd.DataFrame([result] if isinstance(result, (int, float)) else [result])
        if export_format == 'csv':
            output = io.StringIO()
            df.to_csv(output, index=False)
            return make_response(output.getvalue(), 200, {'Content-Type': 'text/csv', 'Content-Disposition': 'attachment; filename=all_scores.csv'})
        else:  # excel
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, index=False)
            output.seek(0)
            return make_response(output.getvalue(), 200, {'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', 'Content-Disposition': 'attachment; filename=all_scores.xlsx'})
    return jsonify(result)

# API 3: get_scores_for_subtype_date
@app.route('/api/get_scores_for_subtype', methods=['GET'])
def get_scores_for_subtype():
    score_type = request.args.get('score_type')  # Added as per PDF, not used
    score_subtype = request.args.get('score_subtype')
    month = request.args.get('month', type=int)
    year = request.args.get('year', type=int)
    stocks = request.args.get('stocks', '').split(',') if request.args.get('stocks') else None
    aggregation_method = request.args.get('aggregation_method', 'max')
    export_format = request.args.get('export_format')

    valid, error = validate_month_year(month, year)
    if not valid:
        return jsonify({"error": error}), 400
    if not score_subtype or not score_type:
        return jsonify({"error": "Score_type and score_subtype are required"}), 400

    query = NRatios if score_subtype in ['n1', 'n2', 'n3'] else BRatios
    ratios = query.query.filter(
        extract('month', query.trade_date) == month,
        extract('year', query.trade_date) == year
    ).all()
    
    result = {}
    for ratio in ratios:
        mappings = StockIndexMapping.query.filter_by(index_id=ratio.sectoral_index_id).all()
        for mapping in mappings:
            if not stocks or mapping.stock_symbol in stocks:
                score = getattr(ratio, score_subtype)
                if score is not None:
                    if mapping.stock_symbol in result:
                        if aggregation_method == 'max':
                            result[mapping.stock_symbol] = max(result[mapping.stock_symbol], score)
                        elif aggregation_method == 'min':
                            result[mapping.stock_symbol] = min(result[mapping.stock_symbol], score)
                        else:  # 'both'
                            if not isinstance(result[mapping.stock_symbol], list):
                                result[mapping.stock_symbol] = [result[mapping.stock_symbol]]
                            result[mapping.stock_symbol].append(score)
                    else:
                        result[mapping.stock_symbol] = score
    
    if not result:
        return jsonify({"error": "No scores found"}), 404
    
    if export_format in ['excel', 'csv']:
        df = pd.DataFrame.from_dict(result, orient='index', columns=['score'])
        if export_format == 'csv':
            output = io.StringIO()
            df.to_csv(output, index=True)
            return make_response(output.getvalue(), 200, {'Content-Type': 'text/csv', 'Content-Disposition': 'attachment; filename=subtype_scores.csv'})
        else:  # excel
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, index=True)
            output.seek(0)
            return make_response(output.getvalue(), 200, {'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', 'Content-Disposition': 'attachment; filename=subtype_scores.xlsx'})
    return jsonify(result)

# API 4: get_scores_for_type_date
@app.route('/api/get_scores_for_type', methods=['GET'])
def get_scores_for_type():
    score_type = request.args.get('score_type')  # Added as per PDF, not used
    month = request.args.get('month', type=int)
    year = request.args.get('year', type=int)
    stocks = request.args.get('stocks', '').split(',') if request.args.get('stocks') else None
    aggregation_method = request.args.get('aggregation_method', 'max')
    export_format = request.args.get('export_format')

    valid, error = validate_month_year(month, year)
    if not valid:
        return jsonify({"error": error}), 400
    if not score_type:
        return jsonify({"error": "Score_type is required"}), 400

    result = {}
    
    for mapping in StockIndexMapping.query.all():
        if not stocks or mapping.stock_symbol in stocks:
            n_ratio = NRatios.query.filter(
                NRatios.sectoral_index_id == mapping.index_id,
                extract('month', NRatios.trade_date) == month,
                extract('year', NRatios.trade_date) == year
            ).first()
            b_ratio = BRatios.query.filter(
                BRatios.sectoral_index_id == mapping.index_id,
                extract('month', BRatios.trade_date) == month,
                extract('year', BRatios.trade_date) == year
            ).first()
            scores = []
            if n_ratio:
                scores.extend([n_ratio.n1, n_ratio.n2, n_ratio.n3])
            if b_ratio:
                scores.extend([b_ratio.b1, b_ratio.b2, b_ratio.b3])
            scores = [s for s in scores if s is not None]
            if scores:
                if aggregation_method == 'max':
                    result[mapping.stock_symbol] = max(scores)
                elif aggregation_method == 'min':
                    result[mapping.stock_symbol] = min(scores)
                else:  # 'both'
                    result[mapping.stock_symbol] = scores
    
    if not result:
        return jsonify({"error": "No scores found"}), 404
    
    if export_format in ['excel', 'csv']:
        df = pd.DataFrame.from_dict(result, orient='index', columns=['score'])
        if export_format == 'csv':
            output = io.StringIO()
            df.to_csv(output, index=True)
            return make_response(output.getvalue(), 200, {'Content-Type': 'text/csv', 'Content-Disposition': 'attachment; filename=type_scores.csv'})
        else:  # excel
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, index=True)
            output.seek(0)
            return make_response(output.getvalue(), 200, {'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', 'Content-Disposition': 'attachment; filename=type_scores.xlsx'})
    return jsonify(result)

# API 5: get_score_summary_by_type
@app.route('/api/get_score_summary', methods=['GET'])
def get_score_summary():
    score_type = request.args.get('score_type')  # Added as per PDF, not used
    month = request.args.get('month', type=int)
    year = request.args.get('year', type=int)
    score_subtype = request.args.get('score_subtype')
    export_format = request.args.get('export_format')

    valid, error = validate_month_year(month, year)
    if not valid:
        return jsonify({"error": error}), 400
    if not score_subtype or not score_type:
        return jsonify({"error": "Score_type and score_subtype are required"}), 400

    query = NRatios if score_subtype in ['n1', 'n2', 'n3'] else BRatios
    ratios = query.query.filter(
        extract('month', query.trade_date) == month,
        extract('year', query.trade_date) == year
    ).all()
    
    scores = [getattr(ratio, score_subtype) for ratio in ratios if getattr(ratio, score_subtype) is not None]
    if not scores:
        return jsonify({"error": "No scores found"}), 404
    
    summary = {
        'average': sum(scores) / len(scores),
        'min': min(scores),
        'max': max(scores),
        'count': len(scores)
    }
    
    if export_format in ['excel', 'csv']:
        df = pd.DataFrame([summary])
        if export_format == 'csv':
            output = io.StringIO()
            df.to_csv(output, index=False)
            return make_response(output.getvalue(), 200, {'Content-Type': 'text/csv', 'Content-Disposition': 'attachment; filename=score_summary.csv'})
        else:  # excel
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, index=False)
            output.seek(0)
            return make_response(output.getvalue(), 200, {'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', 'Content-Disposition': 'attachment; filename=score_summary.xlsx'})
    return jsonify(summary)

# API 6: get_score_summary_by_conditions
@app.route('/api/get_score_summary_by_conditions', methods=['POST'])
def get_score_summary_by_conditions():
    data = request.get_json()
    conditions = data.get('conditions')
    month = data.get('month', type=int)
    year = data.get('year', type=int)
    export_format = data.get('export_format')
    
    if month is not None and year is not None:
        valid, error = validate_month_year(month, year)
        if not valid:
            return jsonify({"error": error}), 400

    scores = []
    
    for ratio in NRatios.query.all() + BRatios.query.all():
        if month is not None and year is not None:
            if extract('month', ratio.trade_date) != month or extract('year', ratio.trade_date) != year:
                continue
        for subtype in ['n1', 'n2', 'n3', 'b1', 'b2', 'b3']:
            score = getattr(ratio, subtype, None)
            if score is not None and evaluate_conditions(conditions, score, subtype):
                scores.append(score)
    
    if not scores:
        return jsonify({"error": "No scores found"}), 404
    
    summary = {
        'average': sum(scores) / len(scores),
        'min': min(scores),
        'max': max(scores),
        'count': len(scores)
    }
    
    if export_format in ['excel', 'csv']:
        df = pd.DataFrame([summary])
        if export_format == 'csv':
            output = io.StringIO()
            df.to_csv(output, index=False)
            return make_response(output.getvalue(), 200, {'Content-Type': 'text/csv', 'Content-Disposition': 'attachment; filename=conditional_summary.csv'})
        else:  # excel
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, index=False)
            output.seek(0)
            return make_response(output.getvalue(), 200, {'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', 'Content-Disposition': 'attachment; filename=conditional_summary.xlsx'})
    return jsonify(summary)

def evaluate_conditions(conditions, score, subtype):
    if 'AND' in conditions:
        return all(evaluate_conditions(cond, score, subtype) for cond in conditions['AND'])
    if 'OR' in conditions:
        return any(evaluate_conditions(cond, score, subtype) for cond in conditions['OR'])
    if 'score_subtype' in conditions and conditions['score_subtype'] != subtype:
        return False
    if 'score_value' in conditions:
        for op, val in conditions['score_value'].items():
            if op == '>' and not score > val:
                return False
            elif op == '<' and not score < val:
                return False
            elif op == '=' and not score == val:
                return False
            elif op == '>=' and not score >= val:
                return False
            elif op == '<=' and not score <= val:
                return False
    return True

# API 7: get_topbottom_scores_by_type_subtype
@app.route('/api/get_topbottom_scores', methods=['GET'])
def get_topbottom_scores():
    top_n = request.args.get('top_n', type=int)
    direction = request.args.get('direction')
    score_type = request.args.get('score_type')  # Added as per PDF, not used
    score_subtype = request.args.get('score_subtype')
    month = request.args.get('month', type=int)
    year = request.args.get('year', type=int)
    aggregation_method = request.args.get('aggregation_method', 'max')
    export_format = request.args.get('export_format')

    valid, error = validate_month_year(month, year)
    if not valid:
        return jsonify({"error": error}), 400
    if not top_n or direction not in ['top', 'bottom'] or not score_subtype or not score_type:
        return jsonify({"error": "top_n, direction, score_type, and score_subtype are required"}), 400

    query = NRatios if score_subtype in ['n1', 'n2', 'n3'] else BRatios
    ratios = query.query.filter(
        extract('month', query.trade_date) == month,
        extract('year', query.trade_date) == year
    ).all()
    
    result = {}
    for ratio in ratios:
        mappings = StockIndexMapping.query.filter_by(index_id=ratio.sectoral_index_id).all()
        for mapping in mappings:
            score = getattr(ratio, score_subtype)
            if score is not None:
                if mapping.stock_symbol in result:
                    if aggregation_method == 'max':
                        result[mapping.stock_symbol] = max(result[mapping.stock_symbol], score)
                    elif aggregation_method == 'min':
                        result[mapping.stock_symbol] = min(result[mapping.stock_symbol], score)
                    else:  # 'both'
                        if not isinstance(result[mapping.stock_symbol], list):
                            result[mapping.stock_symbol] = [result[mapping.stock_symbol]]
                        result[mapping.stock_symbol].append(score)
                else:
                    result[mapping.stock_symbol] = score
    
    if not result:
        return jsonify({"error": "No scores found"}), 404
    
    sorted_scores = sorted(result.items(), key=lambda x: x[1] if not isinstance(x[1], list) else max(x[1]), 
                          reverse=(direction == 'top'))
    top_bottom = sorted_scores[:top_n]
    
    if export_format in ['excel', 'csv']:
        df = pd.DataFrame(top_bottom, columns=['stock', 'score'])
        if export_format == 'csv':
            output = io.StringIO()
            df.to_csv(output, index=False)
            return make_response(output.getvalue(), 200, {'Content-Type': 'text/csv', 'Content-Disposition': 'attachment; filename=topbottom_scores.csv'})
        else:  # excel
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, index=False)
            output.seek(0)
            return make_response(output.getvalue(), 200, {'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', 'Content-Disposition': 'attachment; filename=topbottom_scores.xlsx'})
    return jsonify(dict(top_bottom))

# Run the app
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8080)