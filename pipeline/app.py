"""
Standalone Flask web app for the CSV pipeline.

Upload a CSV of ticket threads, run Pass 1 → 3 → 4 in the background,
poll for progress, and download result CSVs.

Usage:
    python -m pipeline.app          # starts on port 5001
    python pipeline/app.py
"""

import os
import tempfile

from flask import Flask, jsonify, request, render_template, send_from_directory, Response
from werkzeug.utils import secure_filename

from pipeline.csv_runner import submit_job, get_job_status, list_jobs
from pipeline.blob_store import (
    download_file as blob_download_file,
    download_blob_bytes,
    list_blobs,
)

app = Flask(__name__, template_folder=os.path.join(os.path.dirname(__file__), "templates"))

# Store uploads and outputs under a temp root unique to this process
_WORK_DIR = os.path.join(tempfile.gettempdir(), "csv_pipeline_jobs")
os.makedirs(_WORK_DIR, exist_ok=True)

ALLOWED_EXTENSIONS = {"csv"}
MAX_CONTENT_LENGTH = 50 * 1024 * 1024  # 50 MB
app.config["MAX_CONTENT_LENGTH"] = MAX_CONTENT_LENGTH


def _allowed(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/upload", methods=["POST"])
def upload():
    if "file" not in request.files:
        return jsonify({"error": "No file part"}), 400

    f = request.files["file"]
    if not f or not f.filename:
        return jsonify({"error": "No file selected"}), 400

    if not _allowed(f.filename):
        return jsonify({"error": "Only .csv files are allowed"}), 400

    # Create a unique working directory for this job
    import uuid
    job_dir = os.path.join(_WORK_DIR, uuid.uuid4().hex[:12])
    os.makedirs(job_dir, exist_ok=True)

    input_path = os.path.join(job_dir, secure_filename(f.filename))
    f.save(input_path)

    output_dir = os.path.join(job_dir, "output")

    # Optional LLM model override
    inference_server = request.form.get("inference_server")
    inference_server = int(inference_server) if inference_server else None

    job_id = submit_job(input_path, output_dir, inference_server=inference_server)

    return jsonify({"job_id": job_id, "output_dir": output_dir})


@app.route("/status/<job_id>")
def status(job_id):
    st = get_job_status(job_id)
    if st is None:
        return jsonify({"error": "Unknown job"}), 404
    return jsonify(st)


@app.route("/jobs")
def jobs():
    return jsonify(list_jobs())


@app.route("/download/<job_id>/<filename>")
def download(job_id, filename):
    st = get_job_status(job_id)
    if st is None:
        return jsonify({"error": "Unknown job"}), 404

    # Only allow downloading specific result files
    allowed_files = {"pass1_results.csv", "pass3_results.csv", "pass4_results.csv", "pass5_results.csv"}
    if filename not in allowed_files:
        return jsonify({"error": "Invalid filename"}), 400

    output_dir = st["output_dir"]
    filepath = os.path.join(output_dir, filename)

    # If the local file is gone (container restarted), fetch from blob
    if not os.path.isfile(filepath):
        os.makedirs(output_dir, exist_ok=True)
        if not blob_download_file(job_id, filename, filepath):
            return jsonify({"error": "File not found"}), 404

    return send_from_directory(output_dir, filename, as_attachment=True)


# ── Blob storage browser ────────────────────────────────────────────

@app.route("/files")
def files_browser():
    return render_template("files.html")


@app.route("/api/files")
def api_files():
    prefix = request.args.get("prefix", "")
    blobs = list_blobs(prefix)
    return jsonify(blobs)


@app.route("/api/files/download/<path:blob_name>")
def api_file_download(blob_name):
    data = download_blob_bytes(blob_name)
    if data is None:
        return jsonify({"error": "File not found"}), 404

    filename = blob_name.rsplit("/", 1)[-1] if "/" in blob_name else blob_name

    if filename.endswith(".log") or filename.endswith(".json"):
        return Response(data, mimetype="text/plain",
                        headers={"Content-Disposition": f"inline; filename={filename}"})
    return Response(data, mimetype="application/octet-stream",
                    headers={"Content-Disposition": f"attachment; filename={filename}"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=False)
