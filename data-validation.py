import pandas as pd
import datetime
import os

# Define data structure rules for hired_employees.csv
hired_employees_structure_rules = [
    int,
    str,
    str,
    int,
    int
]

# Define data structure rules for departments.csv
departments_structure_rules = [
    int,
    str
]

# Define data structure rules for jobs.csv
jobs_structure_rules = [
    int,
    str
]

# Read CSV file and validate/reject rows
def process_csv_file(file_path, data_structure_rules):
    df = pd.read_csv(file_path, header=None)

    rejected_rows = []
    valid_rows = []

    for index, row in df.iterrows():
        try:
            converted_row = [data_type(value) for data_type, value in zip(data_structure_rules, row)]
            valid_rows.append(converted_row)
        except Exception as e:
            rejected_rows.append(row.tolist() + [str(e), datetime.datetime.now()])

    # Create DataFrames for valid and rejected rows
    valid_df = pd.DataFrame(valid_rows, columns=df.columns.tolist())
    rejected_df = pd.DataFrame(rejected_rows, columns=df.columns.tolist() + ['error_message', 'timestamp'])

    return valid_df, rejected_df

# Process the CSV files
csv_files = {
    'hired_employees.csv': hired_employees_structure_rules,
    'departments.csv': departments_structure_rules,
    'jobs.csv': jobs_structure_rules
}

output_directory = 'globant_challange/outputs/'
csv_directory = 'globant_challange/csv/'

current_directory = os.getcwd()

for csv_file, structure_rules in csv_files.items():
    csv_file_path = os.path.join(current_directory, csv_directory, csv_file)
    valid_records, rejected_records = process_csv_file(csv_file_path, structure_rules)

    print(f"Saving Valid Records for {csv_file}...")
    valid_records.to_csv(f'{output_directory}{csv_file[:-4]}_valid.csv', index=False, header=False)

    print(f"Saving Rejected Records for {csv_file}...")
    rejected_records.to_csv(f'{output_directory}{csv_file[:-4]}_rejected.csv', index=False, header=False)

    print(f"Results saved for {csv_file}\n")






