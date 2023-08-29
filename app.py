from flask import Flask, request, jsonify
import pandas as pd
import datetime
import os
import pyodbc

app = Flask(__name__)
# Azure SQL Database connection configuration
server = 'rakkion.database.windows.net'
database = ' main'
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
    print(table_name)
    rejected_rows = []
    valid_rows = []
    
    with pyodbc.connect(conn_str) as conn:
        cursor = conn.cursor()
        batch_size = 1000
        batch_records = []

        for index, row in df.iterrows():
            try:
                converted_row = tuple(data_type(value) for data_type, value in zip(data_structure_rules, row))
                batch_records.append(converted_row)

                if len(batch_records) >= batch_size:
                    num_columns = len(data_structure_rules)
                    placeholders = ', '.join(['?' for _ in range(num_columns)])
                    query = f"INSERT INTO dbo.{table_name} VALUES ({placeholders})"
                    cursor.executemany(query, batch_records)
                    conn.commit()
                    batch_records = []
                    valid_rows.append(row)
            except Exception as e:
                rejected_rows.append(row.tolist() + [str(e), datetime.datetime.now()])
        
        if batch_records:
            num_columns = len(data_structure_rules)
            placeholders = ', '.join(['?' for _ in range(num_columns)])
            query = f"INSERT INTO dbo.{table_name} VALUES ({placeholders})"
            cursor.executemany(query, batch_records)
            conn.commit()

    # Create DataFrames for valid and rejected rows
    valid_df = pd.DataFrame(valid_rows, columns=df.columns.tolist())
    rejected_df = pd.DataFrame(rejected_rows, columns=df.columns.tolist() + ['error_message', 'timestamp'])
    print(valid_df)
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





