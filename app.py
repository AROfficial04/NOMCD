from flask import Flask, render_template, request, jsonify, send_file, send_from_directory
import pandas as pd
import os
from werkzeug.utils import secure_filename
from pymongo import MongoClient
import io
import gridfs
from datetime import datetime

# Initialize Flask app
app = Flask(__name__, static_folder='assets')

# Set upload folder and allowed file extensions
UPLOAD_FOLDER = 'uploads'
ALLOWED_EXTENSIONS = {'xls', 'xlsx'}
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# MongoDB connection
client = MongoClient("mongodb+srv://mass:ayamass@cluster0.r8hka.mongodb.net/")
db = client['nomc']
collection = db['processed_data']
fs = gridfs.GridFS(db)

# Ensure the upload folder exists
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

# Helper function to check allowed file extensions
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

# Route to handle file uploads and store in GridFS
@app.route("/upload", methods=["POST"])
def upload_file():
    try:
        file = request.files['file']
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)

            # Save file to GridFS
            file_id = fs.put(file, filename=filename)
            return jsonify({"message": "File uploaded successfully", "file_id": str(file_id)}), 200
        else:
            return jsonify({"error": "Invalid file format"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Route to download files from GridFS
@app.route('/download/<file_id>', methods=['GET'])
def download_gridfs_file(file_id):
    try:
        grid_out = fs.get(file_id)
        return send_file(
            io.BytesIO(grid_out.read()),
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            as_attachment=True,
            download_name=grid_out.filename
        )
    except Exception as e:
        return jsonify({"error": f"File not found: {str(e)}"}), 404

# Route to process the uploaded data
@app.route("/process_data", methods=["POST"])
def process_data():
    try:
        # Ensure the uploads directory exists
        if not os.path.exists(app.config['UPLOAD_FOLDER']):
            os.makedirs(app.config['UPLOAD_FOLDER'])

        # Retrieve uploaded files and selected column names
        file1 = request.files['file1']
        file2 = request.files['file2']
        wfm_column = request.form['wfmColumn'].strip()
        hes_column = request.form['hesColumn'].strip()
        non_comm_column = request.form['nonCommColumn'].strip()

        # Save files locally
        file1_path = os.path.join(app.config['UPLOAD_FOLDER'], secure_filename(file1.filename))
        file2_path = os.path.join(app.config['UPLOAD_FOLDER'], secure_filename(file2.filename))
        file1.save(file1_path)
        file2.save(file2_path)

        # Read Excel files
        df_wfm = pd.read_excel(file1_path)
        df_hes = pd.read_excel(file2_path)
        df_wfm.columns = df_wfm.columns.str.strip()
        df_hes.columns = df_hes.columns.str.strip()

        # Validate selected columns
        if wfm_column not in df_wfm.columns or hes_column not in df_hes.columns or non_comm_column not in df_hes.columns:
            return jsonify({"error": "One or more selected columns are invalid"}), 400

        # Non-Comm Logic
        non_comm_data = df_hes[df_hes[non_comm_column].astype(str).apply(lambda x: int(x) > 3 if x.isdigit() else False)]

        # Never-Comm Logic
        never_comm_data = df_wfm[~df_wfm[wfm_column].isin(df_hes[hes_column])]

        # Unmapped Logic
        unmapped_data = df_hes[~df_hes[hes_column].isin(df_wfm[wfm_column])]

        # Save processed data to new files
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        non_comm_file = os.path.join(app.config['UPLOAD_FOLDER'], f"Non_Comm_{timestamp}.xlsx")
        never_comm_file = os.path.join(app.config['UPLOAD_FOLDER'], f"Never_Comm_{timestamp}.xlsx")
        unmapped_file = os.path.join(app.config['UPLOAD_FOLDER'], f"Unmapped_{timestamp}.xlsx")
        non_comm_data.to_excel(non_comm_file, index=False)
        never_comm_data.to_excel(never_comm_file, index=False)
        unmapped_data.to_excel(unmapped_file, index=False)

        # Analyze data
        def analyze_column(dataframe, columns):
            return {col: {"unique_count": dataframe[col].nunique(), "frequencies": dataframe[col].value_counts().to_dict()}
                    for col in columns if col in dataframe.columns}

        summary = {
            "WFM Total Entries": len(df_wfm),
            "HES Total Entries": len(df_hes),
            "Non-Comm Count": len(non_comm_data),
            "Never-Comm Count": len(never_comm_data),
            "Unmapped Count": len(unmapped_data),
            "Detailed Analysis": {
                "Non-Comm": analyze_column(non_comm_data, ["CTWC", "MeterType", "CommunicationMedium"]),
                "Never-Comm": analyze_column(never_comm_data, ["Region Name", "OLD Meter Phase Type"]),
                "Unmapped": analyze_column(unmapped_data, ["CTWC", "MeterType", "CommunicationMedium"]),
                "WFM": analyze_column(df_wfm, ["Region Name", "OLD Meter Phase Type"]),
                "HES": analyze_column(df_hes, ["CTWC", "MeterType", "CommunicationMedium"])
            }
        }

        # Store summary in MongoDB
        collection.insert_one({
            "timestamp": timestamp,
            "summary": summary,
            "nonCommFile": non_comm_file,
            "neverCommFile": never_comm_file,
            "unmappedFile": unmapped_file
        })

        return jsonify({
            "nonCommFile": f"/download/{os.path.basename(non_comm_file)}",
            "neverCommFile": f"/download/{os.path.basename(never_comm_file)}",
            "unmappedFile": f"/download/{os.path.basename(unmapped_file)}",
            "summary": summary
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Route to download processed files
@app.route('/download/<filename>')
def download_file(filename):
    try:
        return send_from_directory(app.config['UPLOAD_FOLDER'], filename)
    except Exception as e:
        return jsonify({"error": str(e)}), 404

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
