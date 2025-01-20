from flask import Flask, render_template, request, jsonify, send_from_directory
import pandas as pd
import os
from werkzeug.utils import secure_filename
from pymongo import MongoClient
import json

app = Flask(__name__, static_folder='assets')

# Set upload folder and allowed file extensions
UPLOAD_FOLDER = 'uploads'
ALLOWED_EXTENSIONS = {'xls', 'xlsx'}

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# MongoDB connection
client = MongoClient("mongodb+srv://mass:ayamass@nomc.r8hka.mongodb.net/")
db = client['nomc']
collection = db['processed_data']

# Delete all documents in the collection
result = collection.delete_many({})
print(f"Deleted {result.deleted_count} documents.")

# Helper function to check file extensions
def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# Route to render the index.html file
@app.route("/")
def index():
    return render_template("index.html")

# Route to handle file uploads and get columns
@app.route("/get_columns", methods=["POST"])
def get_columns():
    try:
        file = request.files['file']
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(filepath)

            # Read the Excel file and extract columns
            df = pd.read_excel(filepath)
            columns = df.columns.tolist()

            return jsonify({"columns": columns})
        else:
            return jsonify({"error": "Invalid file format"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Route to process the uploaded data
@app.route("/process_data", methods=["POST"])
def process_data():
    try:
        # Retrieve uploaded files and selected column names
        file1 = request.files['file1']
        file2 = request.files['file2']
        wfm_column = request.form['wfmColumn'].strip()
        hes_column = request.form['hesColumn'].strip()
        non_comm_column = request.form['nonCommColumn'].strip()

        # Save uploaded files
        file1_path = os.path.join(app.config['UPLOAD_FOLDER'], secure_filename(file1.filename))
        file2_path = os.path.join(app.config['UPLOAD_FOLDER'], secure_filename(file2.filename))
        file1.save(file1_path)
        file2.save(file2_path)

        # Read Excel files and normalize column names
        df_wfm = pd.read_excel(file1_path)
        df_hes = pd.read_excel(file2_path)
        df_wfm.columns = df_wfm.columns.str.strip()
        df_hes.columns = df_hes.columns.str.strip()

        # Validate selected columns
        if wfm_column not in df_wfm.columns:
            return jsonify({"error": f"'{wfm_column}' not found in WFM file"}), 400
        if hes_column not in df_hes.columns:
            return jsonify({"error": f"'{hes_column}' not found in HES file"}), 400
        if non_comm_column not in df_hes.columns:
            return jsonify({"error": f"'{non_comm_column}' not found in HES file"}), 400

        # **Non-Comm Logic**: Filter rows where `non_comm_column` has values > 3
        non_comm_data = df_hes[df_hes[non_comm_column].astype(str).apply(lambda x: int(x) > 3 if x.isdigit() else False)]

        # **Never Comm Logic**: Rows in WFM where `wfm_column` is NOT in HES's `hes_column`
        never_comm_data = df_wfm[~df_wfm[wfm_column].isin(df_hes[hes_column])]

        # **Unmapped Logic**: Rows in HES where `hes_column` is NOT in WFM's `wfm_column`
        unmapped_data = df_hes[~df_hes[hes_column].isin(df_wfm[wfm_column])]

        # Save processed data to new files
        timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        non_comm_file = os.path.join(app.config['UPLOAD_FOLDER'], f"Non_Comm_{timestamp}.xlsx")
        never_comm_file = os.path.join(app.config['UPLOAD_FOLDER'], f"Never_Comm_{timestamp}.xlsx")
        unmapped_file = os.path.join(app.config['UPLOAD_FOLDER'], f"Unmapped_{timestamp}.xlsx")

        # Save results (include all columns)
        non_comm_data.to_excel(non_comm_file, index=False)
        never_comm_data.to_excel(never_comm_file, index=False)
        unmapped_data.to_excel(unmapped_file, index=False)

        # Prepare summaries for detailed analysis
        def analyze_column(dataframe, columns):
            analysis = {}
            for column in columns:
                if column in dataframe.columns:
                    value_counts = dataframe[column].value_counts().to_dict()
                    analysis[column] = {
                        "unique_count": len(value_counts),
                        "frequencies": value_counts
                    }
            return analysis

        wfm_analysis = analyze_column(df_wfm, ["Region Name", "OLD Meter Phase Type","Installation Type"])
        hes_analysis = analyze_column(df_hes, ["CTWC", "MeterType","CommunicationMedium"])
        non_comm_analysis = analyze_column(non_comm_data, ["CTWC", "MeterType","CommunicationMedium"])
        never_comm_analysis = analyze_column(never_comm_data, ["Region Name", "OLD Meter Phase Type","Meter Communication Type"])
        unmapped_analysis = analyze_column(unmapped_data, ["CTWC", "MeterType","CommunicationMedium"])

        # Prepare overall summary
        summary = {
            "WFM Total Entries": len(df_wfm),
            "HES Total Entries": len(df_hes),
            "Non-Comm Count": len(non_comm_data),
            "Never-Comm Count": len(never_comm_data),
            "Unmapped Count": len(unmapped_data),
            "Matched Entries": len(df_wfm[df_wfm[wfm_column].isin(df_hes[hes_column])]),
            "Detailed Analysis": {
                "Non-Comm": non_comm_analysis,
                "Never-Comm": never_comm_analysis,
                "Unmapped": unmapped_analysis,
                "WFM": wfm_analysis,
                "HES": hes_analysis
            }
        }

        # Generate pivot summaries
        def generate_pivot_summary(df_wfm, df_hes, non_comm_data, never_comm_data, unmapped_data):
            summary = {}

            # WFM Summary: Count by Region, Meter Type (WC/DT), and Phase Type (1 Phase/3 Phase)
            def wfm_summary():
                return pd.crosstab(
                    [df_wfm['Region Name'], df_wfm['Installation Type']],
                    df_wfm['OLD Meter Phase Type'],
                    margins=True, margins_name="Total"
                ).fillna(0).to_dict()

            # HES Summary: Count by Meter Type (WC/DT) and Communication Medium (RF/GPRS)
            def hes_summary():
                return pd.crosstab(
                    df_hes['MeterType'],
                    df_hes['CommunicationMedium'],
                    margins=True, margins_name="Total"
                ).fillna(0).to_dict()

            # Non-Comm Summary: Count by Meter Type (WC/DT) and Communication Medium (RF/GPRS)
            def non_comm_summary():
                return pd.crosstab(
                    non_comm_data['MeterType'],
                    non_comm_data['CommunicationMedium'],
                    margins=True, margins_name="Total"
                ).fillna(0).to_dict()

            # Never-Comm Summary: Count by Region and Communication Type (GPRS/RF)
            def never_comm_summary():
                return pd.crosstab(
                    never_comm_data['Region Name'],
                    never_comm_data['Meter Communication Type'],
                    margins=True, margins_name="Total"
                ).fillna(0).to_dict()

            # Unmapped Summary: Count by Meter Type (WC/DT) and Communication Medium (RF/GPRS)
            def unmapped_summary():
                return pd.crosstab(
                    unmapped_data['MeterType'],
                    unmapped_data['CommunicationMedium'],
                    margins=True, margins_name="Total"
                ).fillna(0).to_dict()

            # Convert tuple keys to strings
            def convert_keys_to_strings(d):
                new_d = {}
                for k, v in d.items():
                    if isinstance(k, tuple):
                        k = ' & '.join(map(str, k))
                    if isinstance(v, dict):
                        v = convert_keys_to_strings(v)
                    new_d[k] = v
                return new_d

            # Generate Summaries for all datasets
            summary['wfm'] = convert_keys_to_strings(wfm_summary())
            summary['hes'] = convert_keys_to_strings(hes_summary())
            summary['non_comm'] = convert_keys_to_strings(non_comm_summary())
            summary['never_comm'] = convert_keys_to_strings(never_comm_summary())
            summary['unmapped'] = convert_keys_to_strings(unmapped_summary())

            return summary

        pivot_summary = generate_pivot_summary(df_wfm, df_hes, non_comm_data, never_comm_data, unmapped_data)

        # Store the processed data in MongoDB
        data_to_store = {
            "timestamp": timestamp,
            "summary": summary,
            "pivotSummary": pivot_summary,
            "nonCommFile": non_comm_file,
            "neverCommFile": never_comm_file,
            "unmappedFile": unmapped_file
        }
        collection.insert_one(data_to_store)

        return jsonify({
            "nonCommFile": f"/download/{os.path.basename(non_comm_file)}",
            "neverCommFile": f"/download/{os.path.basename(never_comm_file)}",
            "unmappedFile": f"/download/{os.path.basename(unmapped_file)}",
            "summary": summary,
            "pivotSummary": pivot_summary
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Route to download the processed files
@app.route('/download/<filename>')
def download_file(filename):
    return send_from_directory(app.config['UPLOAD_FOLDER'], filename)

if __name__ == "__main__":
    # Create upload folder if it doesn't exist
    if not os.path.exists(UPLOAD_FOLDER):
        os.makedirs(UPLOAD_FOLDER)

    # Run the app
    app.run(host="0.0.0.0", port=5000, debug=True)
