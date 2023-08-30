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
                    print(len(valid_rows))

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

        current_directory = os.getcwd()
        csv_directory = 'csv/'
        output_directory = 'outputs/'

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

@app.route('/')
def hello_world():
    print(pyodbc.drivers())
    return 'Challange'

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)





