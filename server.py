from flask import Flask, request, Response
import boto3
import threading
import csv
import os

ASU_ID = "1222045786"
INPUT_BUCKET = f"{ASU_ID}-in-bucket"
SIMPLEDB_DOMAIN = f"{ASU_ID}-simpleDB"
REGION = "us-east-1"

app = Flask(__name__)

# AWS clients
s3 = boto3.client('s3', region_name=REGION)
sdb = boto3.client('sdb', region_name=REGION)

# Lock for SimpleDB operations
sdb_lock = threading.Lock()

# Ensure bucket and domain exist
def setup_resources():
    # Create S3 bucket if not exists
    buckets = s3.list_buckets()
    if not any(bucket['Name'] == INPUT_BUCKET for bucket in buckets['Buckets']):
        s3.create_bucket(Bucket=INPUT_BUCKET, CreateBucketConfiguration={'LocationConstraint': REGION})
        print(f"S3 bucket created: {INPUT_BUCKET}")

    # Create SimpleDB domain
    sdb.create_domain(DomainName=SIMPLEDB_DOMAIN)
    print(f"SimpleDB domain ensured: {SIMPLEDB_DOMAIN}")

# Populate SimpleDB domain from CSV
def populate_simpledb_from_csv():
    with open("classification_face_images_1000.csv", "r") as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            filename, label = row
            with sdb_lock:
                sdb.put_attributes(
                    DomainName=SIMPLEDB_DOMAIN,
                    ItemName=filename,
                    Attributes=[{
                        'Name': 'label',
                        'Value': label,
                        'Replace': True
                    }]
                )

@app.route("/", methods=["POST"])
def handle_request():
    if 'inputFile' not in request.files:
        return Response("Missing 'inputFile' in request.", status=400)

    file = request.files['inputFile']
    filename = file.filename

    if not filename:
        return Response("Empty filename.", status=400)

    # Upload to S3
    try:
        s3.upload_fileobj(file, INPUT_BUCKET, filename)
    except Exception as e:
        return Response(f"Failed to upload to S3: {str(e)}", status=500)

    # Query SimpleDB for recognition result
    try:
        with sdb_lock:
            result = sdb.get_attributes(
                DomainName=SIMPLEDB_DOMAIN,
                ItemName=filename
            )
        attributes = result.get("Attributes", [])
        if not attributes:
            return Response(f"{filename}:Unknown", mimetype="text/plain")
        label = next((attr["Value"] for attr in attributes if attr["Name"] == "label"), "Unknown")
        return Response(f"{filename}:{label}", mimetype="text/plain")
    except Exception as e:
        return Response(f"Error querying SimpleDB: {str(e)}", status=500)

# Multithreading server
def run_app():
    from werkzeug.serving import make_server
    import threading

    class ServerThread(threading.Thread):
        def __init__(self, app):
            threading.Thread.__init__(self)
            self.srv = make_server('0.0.0.0', 8000, app)
            self.ctx = app.app_context()
            self.ctx.push()

        def run(self):
            print("Server running on port 8000...")
            self.srv.serve_forever()

    ServerThread(app).start()

if __name__ == "__main__":
    setup_resources()
    populate_simpledb_from_csv()
    run_app()
