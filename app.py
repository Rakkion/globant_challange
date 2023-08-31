from flask import Flask, request, jsonify
import pandas as pd
import datetime
import os
import pyodbc
import tempfile
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient
import io
import avro
from avro import schema
from avro.datafile import DataFileWriter, DataFileReader
from avro.io import DatumWriter, DatumReader
import json

app = Flask(__name__)

# Use Azure Key Vault to retrieve the connection string secret
vault_url = "https://keyvalut-rak.vault.azure.net/"

azure_client_id = os.environ["AZURE_CLIENT_ID"]
azure_client_secret = os.environ["AZURE_CLIENT_SECRET"]
azure_tenant_id = os.environ["AZURE_TENANT_ID"]

credential = ClientSecretCredential(client_id=azure_client_id, client_secret=azure_client_secret, tenant_id=azure_tenant_id)
secret_client = SecretClient(vault_url=vault_url, credential=credential)

#Key Valut secret names
server_secret = "sql-server-secret"
database_secret = "sql-database-secret"
username_secret = "sql-username-secret"
password_secret = "sql-password-secret"
blob_secret = "blob-storage-conn-secret"

#Key Valut Secrets Values
server_str_secret = secret_client.get_secret(server_secret)
database_str_secret = secret_client.get_secret(database_secret)
username_str_secret = secret_client.get_secret(username_secret)
password_str_secret = secret_client.get_secret(password_secret)
blob_str_secret = secret_client.get_secret(blob_secret)

# Azure SQL Database connection configuration
server = server_str_secret.value
database = database_str_secret.value
username = username_str_secret.value
password = password_str_secret.value
driver = '{ODBC Driver 18 for SQL Server}'
conn_str = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}'

# Define data structure rules for hired_employees.csv (based on colums positions)
hired_employees_structure_rules = [
    int,
    str,
    str,
    int,
    int
]

# Define data structure rules for departments.csv (based on colums positions)
departments_structure_rules = [
    int,
    str
]

# Define data structure rules for jobs.csv (based on colums positions)
jobs_structure_rules = [
    int,
    str
]

# Read CSV file, validate/reject rows, and insert into Azure SQL Database
def process_csv_file(file_path, data_structure_rules, table_name):
    blob_data = file_path.download_blob()
    blob_stream = io.BytesIO(blob_data.readall())
    df = pd.read_csv(blob_stream, header=None)
    rejected_rows = []
    valid_rows = []
    batch_size = 1000

    cnxn1 = pyodbc.connect("DRIVER={ODBC Driver 18 for SQL Server};SERVER=" + server + ";DATABASE=" + database + ";UID=" + username + ';PWD=' + password, autocommit=False)
    cursor = cnxn1.cursor()

    # Assuming that table_name is a variable containing the name of the table
    truncate_query = f"TRUNCATE TABLE dbo.{table_name}"
    cursor.execute(truncate_query)
    cnxn1.commit()

    cnxn1.close()

    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_csv_file = temp_file.name

        for index, row in df.iterrows():
            try:
                if any(pd.isnull(row)):  # Check if any value in the row is empty or NaN
                    rejected_rows.append(row.tolist() + ["Empty or NaN values", datetime.datetime.now()])
                else:
                    converted_row = [data_type(value) for data_type, value in zip(data_structure_rules, row)]
                    valid_rows.append(converted_row)

                if len(valid_rows) == batch_size:
                    valid_df = pd.DataFrame(valid_rows, columns=df.columns.tolist())
                    valid_df.to_csv(temp_csv_file, index=False, header=False)

                    # Clean up the temporary CSV file
                    valid_rows = []

                    sCmdExecute = f"bcp dbo.{table_name} in {temp_csv_file} -c -t, -S {server} -d {database} -U {username} -P {password} -b {str(batch_size)} -F 1"
                    os.system(sCmdExecute)
                        
            except Exception as e:
                rejected_rows.append(row.tolist() + [str(e), datetime.datetime.now()])

        if valid_rows:
            valid_df = pd.DataFrame(valid_rows, columns=df.columns.tolist())
            valid_df.to_csv(temp_csv_file, index=False, header=False)
            print(len(valid_rows))
            valid_rows = []

            sCmdExecute = f"bcp {table_name} in {temp_csv_file} -c -t, -S {server} -d {database} -U {username} -P {password} -b {str(batch_size)} -F 1"
            os.system(sCmdExecute)

        # Clean up the temporary CSV file
        os.remove(temp_csv_file)
    
    # Create DataFrames for valid and rejected rows
    valid_df = pd.DataFrame(valid_rows, columns=df.columns.tolist())
    rejected_df = pd.DataFrame(rejected_rows, columns=df.columns.tolist() + ['error_message', 'timestamp'])

    return valid_df, rejected_df

# Process the CSV files
csv_files = [
    {
        'file_name': 'hired_employees.csv',
        'structure_rules': hired_employees_structure_rules,
        'table_name': 'hired_employees'
    },
    {
        'file_name': 'departments.csv',
        'structure_rules': departments_structure_rules,
        'table_name': 'departments'
    },
    {
        'file_name': 'jobs.csv',
        'structure_rules': jobs_structure_rules,
        'table_name': 'jobs'
    }
]

#Azure Blob Storage connection configuration
blob_str_secret = blob_str_secret.value
connection_string = blob_str_secret
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

csv_container = "csv"
rejected_container = "rejected-logs"

container_csv = blob_service_client.get_container_client(csv_container)
container_rejected = blob_service_client.get_container_client(rejected_container)

@app.route('/upload_data')
def upload_data():
    try:

        for csv_file_info in csv_files:
            csv_file = csv_file_info['file_name']
            structure_rules = csv_file_info['structure_rules']
            table_name = csv_file_info['table_name']
            
            csv_file_path = container_csv.get_blob_client(csv_file)
            valid_records, rejected_records = process_csv_file(csv_file_path, structure_rules, table_name)
            
            print(f"Migration completed for {csv_file}")

            if not rejected_records.empty:
                timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
                blob_name = f"rejected_records_{table_name}_{timestamp}.csv"
                rejected_text = rejected_records.to_csv(index=False, sep=',', header=False)
                blob_client = container_rejected.get_blob_client(blob_name)
                blob_client.upload_blob(rejected_text, overwrite=True)


        return jsonify({'message': 'Data uploaded and migrated successfully.'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/backup')
def backup_tables():

    backup_container = "backups"
    container_backup = blob_service_client.get_container_client(backup_container)

    # Define the actual schemas for each table
    actual_schemas = {
        "hired_employees": [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "string"},
            {"name": "datetime", "type": "string"},
            {"name": "department_id", "type": "int"},
            {"name": "job_id", "type": "int"}
        ],
        "departments": [
            {"name": "id", "type": "int"},
            {"name": "department","type": "string"},
        ],
        "jobs": [
            {"name": "id", "type": "int"},
            {"name": "job", "type": "string"},
        ]
    }

    try:
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

        for csv_file_info in csv_files:
            table_name = csv_file_info['table_name']
            avro_file_name = f"{table_name}_{timestamp}.avro"

            # Get the actual schema for the current table
            actual_schema = actual_schemas.get(table_name)

            if actual_schema is None:
                return jsonify({'error': f"No actual schema defined for {table_name}"}), 500
                
            avro_schema = {
                "type": "record",
                "name": table_name,
                "fields": actual_schema
            }

            # Construct the Avro schema JSON-formatted string
            avro_schema_str = json.dumps({
                "type": "record",
                "name": table_name,
                "fields": actual_schema
            })

            # Parse the AVRO schema using avro.schema.parse
            parsed_schema = avro.schema.parse(avro_schema_str)
 
            # Query data from the table
            cnxn = pyodbc.connect(conn_str)
            cursor = cnxn.cursor()

            # Query all data from the table
            query = f"SELECT * FROM dbo.{table_name}"
            cursor.execute(query)
            table_data = cursor.fetchall()

            # Upload AVRO data to blob storage
            blob_name = f"{table_name}_{timestamp}.avro"
            blob_client = container_backup.get_blob_client(blob_name)
            avro_stream = io.BytesIO()

            # Serialize AVRO data to avro_stream
            avro_writer = DataFileWriter(avro_stream, DatumWriter(), parsed_schema)
            for row in table_data:
                avro_record = {}  # Create an empty dictionary for the Avro record
                for i, field_info in enumerate(actual_schema):
                    field_name = field_info['name']
                    avro_record[field_name] = row[i]
                avro_writer.append(avro_record)
            
            # Upload avro_stream to blob storage
            blob_client.upload_blob(avro_stream.getvalue(), overwrite=True)
            avro_writer.close()

            print(f"Backup completed for {table_name}")

        return jsonify({'message': 'Backups created and stored successfully.'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/restore/<table_name>/<blob_name>')
def restore_tables(table_name, blob_name):
    
    try:
        backup_container = "backups"
        container_backup = blob_service_client.get_container_client(backup_container)
        
        blob_name = "hired_employees_20230830222004.avro"
        blob_client = container_backup.get_blob_client(blob_name)

        # Download blob content as a byte stream
        blob_stream = blob_client.download_blob().readall()

        # Open the downloaded blob content as a byte stream and read Avro data
        avro_stream = io.BytesIO(blob_stream)
        reader = DataFileReader(avro_stream, DatumReader())
        
        # Initialize lists to store extracted values
        user_ids = []
        names = []
        datetimes = []
        department_ids = []
        job_ids = []

        for data in reader:
            # Extract values from the record
            user_ids.append(data['id'])
            names.append(data['name'])
            datetimes.append(data['datetime'])
            department_ids.append(data['department_id'])
            job_ids.append(data['job_id'])

        reader.close()

        # Create a DataFrame from the extracted data
        data = {
            'user_id': user_ids,
            'name': names,
            'datetime': datetimes,
            'department_id': department_ids,
            'job_id': job_ids
        }
        df = pd.DataFrame(data)

        cnxn1 = pyodbc.connect("DRIVER={ODBC Driver 18 for SQL Server};SERVER=" + server + ";DATABASE=" + database + ";UID=" + username + ';PWD=' + password, autocommit=False)
        cursor = cnxn1.cursor()

        # Assuming that table_name is a variable containing the name of the table
        truncate_query = f"TRUNCATE TABLE dbo.{table_name}"
        cursor.execute(truncate_query)
        cnxn1.commit()
        cnxn1.close()

        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_csv_file = temp_file.name
            df.to_csv(temp_csv_file, index=False, header=False)
            sCmdExecute = f"bcp dbo.{table_name} in {temp_csv_file} -c -t, -S {server} -d {database} -U {username} -P {password} -F 1"
            os.system(sCmdExecute)
        
        # Clean up the temporary CSV file
        os.remove(temp_csv_file)

        return jsonify({'message': 'Backups created and stored successfully.'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/')
def hello_world():
    print(pyodbc.drivers())
    return 'Challange'

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)





