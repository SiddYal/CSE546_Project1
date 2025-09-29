from flask import Flask, request, Response, jsonify
import boto3
import csv
from werkzeug.utils import secure_filename
import os

app = Flask(__name__)

s3 = boto3.client('s3', region_name="us-east-1")
sdb = boto3.client('sdb', region_name="us-east-1")

"""
bucket_name = '1222045786-in-bucket'
db_name = '1222045786-simpleDB'
s3.create_bucket(Bucket=bucket_name)

print("bucket created")

# Create SimpleDB domain



my_session = boto3.session.Session()
ec2 = my_session.client('ec2')
s3 = my_session.client('s3')
sdb = my_session.client('sdb')

sdb.create_domain(DomainName=db_name)

print(f"SimpleDB domain ensured: {db_name}")
"""

bucket_name = '1222045786-in-bucket'
s3.create_bucket(Bucket=bucket_name)

db_name = '1222045786-simpleDB'
sdb.delete_domain(DomainName=db_name)
sdb.create_domain(DomainName=db_name)

def populate_simpledb_from_csv():
    with open('classification_results.csv', mode='r') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)
        #count = 0
        for row in reader:
            #print(row)
            image, result = row
            #print(f"{count}: {image}, {result}")
            #count = count + 1
            sdb.put_attributes(DomainName=db_name, ItemName=image,Attributes= 
                    [{
                        'Name': 'result',
                        'Value': result,
                        'Replace': True
                    }]
                )
            #domain_metadata = sdb.domain_metadata(DomainName=db_name)
            #item_count = domain_metadata.get('ItemCount')
            #print(item_count)
           
        #print(count)


@app.route('/', methods=['POST'])
def handle_post_request():
    if 'inputFile' not in request.files:
        return jsonify({'error': 'No file part with key "inputFile"'}), 400
    print("check1")
    file = request.files['inputFile']
    print("check2")
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400
    print("check3")
    # Secure filename and save
    filename = secure_filename(file.filename)
    print(type(filename))

    s3.upload_file(filename, bucket_name, filename)
    #print(f"File {filename} uploaded.")
    print("check4")
    truncfilename, ext = os.path.splitext(filename)
    #domain_metadata = sdb.domain_metadata(DomainName=db_name)
    #item_count = domain_metadata.get('ItemCount')
    #print(item_count)
    print("check5")
    result = sdb.get_attributes(
                DomainName=db_name,
                ItemName=truncfilename
            )
    print("check6")
    attributes = result.get("Attributes", [])
    print("check7")
    if not attributes:
        print("check8")
        return Response(f"{truncfilename}:Unknown", mimetype="text/plain")
    print("check9")
    result = next((attr["Value"] for attr in attributes if attr["Name"] == "result"), "Unknown")
    print("check10")
    print(f"{truncfilename}:{result}")
    return Response(f"{truncfilename}:{result}", mimetype="text/plain")

    #return jsonify({'message': f'File received and saved as {filename}'}), 200

if __name__ == '__main__':
    #print(sdb.DomainMetadata(db_name))
    populate_simpledb_from_csv()
    app.run(host='0.0.0.0', port=8000)