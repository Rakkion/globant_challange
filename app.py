from flask import Flask, request, jsonify
import pandas as pd
import datetime
import os
import pyodbc
import tempfile

app = Flask(__name__)
# Azure SQL Database connection configuration
server = 'rakkion.database.windows.net'
database = 'rakkion-db'
username = 'user'
password = 'FFTeadp#'
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
    df = pd.read_csv(file_path, header=None)
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
                converted_row = [data_type(value) for data_type, value in zip(data_structure_rules, row)]
                valid_rows.append(converted_row)

                if len(valid_rows) == batch_size:
                    valid_df = pd.DataFrame(valid_rows, columns=df.columns.tolist())
                    valid_df.to_csv(temp_csv_file, index=False, header=False)
                    print(len(valid_rows))

                    # Clean up the temporary CSV file
                    valid_rows = []

                    sCmdExecute = f"bcp dbo.{table_name} in {temp_csv_file} -c -t, -S {server} -d {database} -U {username} -P {password} -b {str(batch_size)} -F 2"
                    os.system(sCmdExecute)
                        
            except Exception as e:
                rejected_rows.append(row.tolist() + [str(e), datetime.datetime.now()])

        if valid_rows:
            valid_df = pd.DataFrame(valid_rows, columns=df.columns.tolist())
            valid_df.to_csv(temp_csv_file, index=False, header=False)
            print(len(valid_rows))
            valid_rows = []

            sCmdExecute = f"bcp {table_name} in {temp_csv_file} -c -t, -S {server} -d {database} -U {username} -P {password} -b {str(batch_size)} -F 2"
            os.system(sCmdExecute)

        # Clean up the temporary CSV file
        os.remove(temp_csv_file)
    
    # Create DataFrames for valid and rejected rows
    valid_df = pd.DataFrame(valid_rows, columns=df.columns.tolist())
    rejected_df = pd.DataFrame(rejected_rows, columns=df.columns.tolist() + ['error_message', 'timestamp'])

    return valid_df, rejected_df

# Update the csv_directory variable
csv_directory = 'csv/'
output_directory = 'outputs/'

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

@app.route('/upload_data')
def upload_data():
    try:

        current_directory = os.getcwd()

        for csv_file_info in csv_files:
            csv_file = csv_file_info['file_name']
            structure_rules = csv_file_info['structure_rules']
            table_name = csv_file_info['table_name']
            
            csv_file_path = os.path.join(current_directory, csv_directory, csv_file)
            valid_records, rejected_records = process_csv_file(csv_file_path, structure_rules, table_name)
            
            print(f"Migration completed for {csv_file}")
            print(f"Valid Records for {table_name}:")


        return jsonify({'message': 'Data uploaded and migrated successfully.'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/')
def hello_world():
    print(pyodbc.drivers())
    return 'Hello World! (Classic)'

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)





