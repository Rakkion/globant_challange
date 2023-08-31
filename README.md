# globant_challange
 Globan DE Position Challange

The code consist on a simple app to move data from a CSV to a Database, based on certain rules.

Application based on the following online services:
- Azure SQL Database: for data storage
- Azure Blob Storage: for storage of the original CSV files, rejected logs and security copies of the SQL data (in AVRO format)
- Azure Key Vault: to improve security on the code avoiding raw credentials

The code also uses Docker Containers.

How it works:
- The app is divided in 3 main operations: upload, backup and restore data.
- Each operation start a workflow to validate the data and meet the rules, based on shared instructions.
- Each operation can be accesed by an URL path that will be explain below (based on localhost for this exercise).
- The rest of the code is mainly schemas structures definitions or connections strings.

--
Upload data

This operation does the following tasks:
1. Move historic data from files in CSV format to the new database.
2. Create a Rest API service to receive new data. This service must have:
2.1. Each new transaction must fit the data dictionary rules.
2.2. Be able to insert batch transactions (1 up to 1000 rows) with one request. 2.3. Receive the data for each table in the same service.
2.4. Keep in mind the data rules for each table.

To access this operation, once started the local server, enter to the path '/upload_data' (ex. localhost/upload_data). If the code run without any problem, message "Upload data successfully" will be displayed.

Note: Due security access of Azure SQL Server, is mostly sure that this path will throw an error of connectivity or firewall. Is neccesary to add your local IP address to the whitelist (only for this exercise, not the approach for a production code).

The code do the following tasks (in general view):
Note: Because we have 3 tables: hired_employees, jobs and departments, the "upload data" operation do the same task for each table on a same service.
- Takes the schema definitions of the CSV (the data type of each column based on the respective table). Also takes the: CSV name and CSV table name (ex. hired_employees)
- Based on the CSV file name, it downloaded it from Azure Blob Storage and creates a Pandas Dataframe.
- Connection to database are created. The table of destination is truncated (instructions don't ask to check existing row for an update, so is going to be a complete batch from zero).
- Temporaty CSV file is created to save the data on-memory (to then upload to  the batch process), based on the rules.
- Valid and Rejected lists are defined.
- For every row and record of columns, is checked if the data type match the schema definition (int, str, etc.). If matches, the row is recorded on the Valid array, if is rejected goes to the Rejected array. Both lists are transformed to a dataframe and then to a csv file to upload.
- Once we have 1000 valid records, it trigger the first batch proccesing. If we have more records to analize this will continue in another batch (batch per 1000 records).
- For performance improvements, and avoid row by row upload, the method is a BULK INSERT done by BCP. More information on the solution https://github.com/JMNetwalker/InsertDataFaster/blob/main/python/WithBCP.py.
- Once completed, Valid Rows are reseted and starts from 0. The code continues from it was stopped and follow the same logic until analizing all records.
- For rejected values, it is saved on Blob Storage as a CSV file with the format: table_name_timestamp.csv.

--

Backup
This operation does the following tasks:
3. Create a feature to backup for each table and save it in the file system in AVRO format.

To access this operation, enter to the path "/backup". Same consideration about SQL Server Firewall exceptions from "Upload data".

The code do the following tasks (in general view):
- Takes the schema of each table (to format the AVRO file).
- Read all the data of the corresponding table (SELECT * FROM table_name).
- Write the results on an AVRO file on azure blob storage.
