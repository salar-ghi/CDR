import ftplib
import os
import gzip
import pandas as pd
import pyodbc

# FTP connection details
FTP_HOST = "ftp.rahyab.ir"
FTP_USER = "ofogh"
FTP_PASS = "OF@#rahyab2025"
FTP_DIR = "OFOGH"


# DB_SERVER = "172.16.17.22"
DB_SERVER = "localhost"
DB_NAME = "SmsCdr"
DB_USER = "sa"
DB_PASS = "1234512345"
DB_DRIVER = "{ODBC Driver 17 for SQL Server}"  # Adjust if your ODBC driver name differs (e.g., for Linux/Mac, use FreeTDS or similar)
TABLE_NAME = "SmsRecords"

# Local download directory
DOWNLOAD_DIR = "downloads"
if not os.path.exists(DOWNLOAD_DIR):
    os.makedirs(DOWNLOAD_DIR)
print("Step 1: Created downloads folder.")

COLUMN_NAMES = ["SmsId", "DeliveredTime", "Source", "Destination", "SmsStatus", "AdditionalInfo"]

MONTH_NAMES = {
    1: "January", 2: "February", 3: "March", 4: "April",
    5: "May", 6: "June", 7: "July", 8: "August",
    9: "September", 10: "October", 11: "November", 12: "December"
}

def parse_month_from_filename(filename):
    # Assuming format like cdr_YYYY-M-D.ext (e.g., cdr_2025-9-30.gz)
    try:
        # Split by '_' to get the date part, then remove extension, then split by '-'
        date_part = filename.split('_')[1].split('.')[0]
        parts = date_part.split('-')
        month_num = int(parts[1])
        if 1 <= month_num <= 12:
            return MONTH_NAMES[month_num]
    except (IndexError, ValueError):
        pass
    return None

# Accumulators for totals
total_records_all = 0
total_inserted = 0

# Connect to FTP with error handling
try:
    print("Step 2: Attempting to connect to FTP...")
    with ftplib.FTP(FTP_HOST) as ftp:
        print("Connected to host.")

        print("Step 3: Logging in...")
        ftp.login(FTP_USER, FTP_PASS)
        print("Logged in successfully.")

        ftp.set_pasv(True)  # Enable passive mode (try commenting this out if it causes issues)
        print("Passive mode enabled.")
        
        print("Step 4: Changing to directory:", FTP_DIR)
        ftp.cwd(FTP_DIR)
        print("Directory changed successfully.")
        
        print("Step 5: Listing files...")
        # List files
        files = ftp.nlst()
        print("Files in directory:", files)

        # Filter RAR files with prefix cdr_2025 or cdr_2026
        target_files = [f for f in files if f.endswith('.gz') and (f.startswith('cdr_2025') or f.startswith('cdr_2026'))]
        print("Target files found:", target_files)

        if not target_files:
            print("No matching files found. Check prefixes, extensions, or directory contents.")
            print("Script execution complete.")
            exit()

        for filename in target_files:
            local_path = os.path.join(DOWNLOAD_DIR, filename)

            print(f"Step 6: Downloading {filename}...")
            with open(local_path, 'wb') as local_file:
                ftp.retrbinary(f"RETR {filename}", local_file.write)
            print(f"Downloaded: {filename}")
            
            # Extract .gz
            extracted_path = os.path.join(DOWNLOAD_DIR, filename[:-3])  # Remove .gz
            print(f"Step 7: Extracting {filename} to {extracted_path}...")
            # with gzip.open(local_path, 'rt', encoding='utf-8') as gz_file:
            with gzip.open(local_path, 'rb') as f_in:
                with open(extracted_path, 'wb') as f_out:
                    f_out.write(f_in.read())
            print(f"Extracted: {extracted_path}")

            # Read the extracted file (assume comma-delimited CSV, with possible quotes around fields)
            # If the delimiter is different (e.g., tab '\t', space '\s+', or '|'), change sep accordingly
            print(f"Step 8: Reading data from {extracted_path}...")
            # df = pd.read_csv(extracted_path, sep=',', header=None, names=[''])
            df = pd.read_csv(extracted_path, sep=',', header=None, names=['SmsId', 'DeliveredTime', 'Source', 'Destination', 'SmsStatus', 'Column6'], dtype=str, quoting=1)  # quoting=1 for "
            total_records_all += len(df)
            print(f"Total records in file: {len(df)}")

            # Filter only "Delivered" and replace with 2
            df_filtered = df[df['SmsStatus'] == 'Delivered'].copy()
            df_filtered['SmsStatus'] = 2
            inserted_this_file = len(df_filtered)
            total_inserted += inserted_this_file
            print(f"Filtered records to insert: {inserted_this_file}")

            # Parse month and get table name
            month_name = parse_month_from_filename(filename)
            if not month_name:
                print(f"Could not parse month for {filename}, skipping import.")
                continue

            table_name = f"Cdr{month_name}"
            print(f"Table name for import: {table_name}")

            # Connect to DB
            print("Step 9: Connecting to database...")
            conn_str = f"DRIVER={DB_DRIVER};SERVER={DB_SERVER};DATABASE={DB_NAME};UID={DB_USER};PWD={DB_PASS}"
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            print("Database connected")

            # Check if table exists, create if not
            print(f"Step 10: Checking if table {table_name} exists...")
            cursor.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}' AND table_schema = 'dbo'")

            if cursor.fetchone()[0] == 0:
                print(f"Creating table {table_name}...")
                create_query = F"""
                CREATE TABLE {table_name} (
                    SmsId bigint NULL,
                    Destination varchar(50) NULL,
                    Source varchar(50) NULL,
                    DeliveredTime datetime2(2) NULL,
                    SmsStatus varchar(50) NULL,
                    User varchar(50) NULL,
                    DeliveryStatusId tinyint NULL
                )
                """
                cursor.execute(create_query)
                conn.commit()
                print(f"Table {table_name} created.")
            else:
                print(f"Table {table_name} exists.")

            # Insert filtered data row by row (for large files, consider bulk insert methods)
            print(f"Step 11: Inserting data into {table_name}...")
            for index, row in df_filtered.iterrows():
                # Convert types as needed (pyodbc handles conversion)
                sms_id = int(row['SmsId'])
                delivered_time = row['DeliveredTime'] if row['DeliveredTime'] else None  # String in ISO format should convert to DATETIME
                source = row['Source']
                destination = row['Destination']
                sms_status = int(row['SmsStatus']) if row['SmsStatus'] else None
                User = row['User']

                insert_query = f"INSERT INTO {table_name} (SmsId, DeliveredTime, Source, Destination, SmsStatus, Column6) VALUES (?, ?, ?, ?, ?, ?)"
                cursor.execute(insert_query, sms_id, delivered_time, source, destination, sms_status, User)
            conn.commit()
            print(f"Inserted {inserted_this_file} records into {table_name}.")

            # Close DB connection for this file (reopen if needed, but  we'll close at end)
            conn.close()

    # After all files, handle CdrInfo
    print("Step 12: Handling CdrInfo table...")
    conn = pyodbc.connect(conn_str)
    cursor  = conn.cursor()

    cursor.execute("Select count(*) from information_schema.tables where table_name = 'CdrInfo' and table_schema = 'dbo'")
    if cursor.fetchone()[0] == 0:
        print("Creating CdrInfo table...")
        create_info_query = """
        CREATE TABLE CdrInfo (
            TotalRecords Bigint,
            TotalInserted Bigint
        )
        """
        cursor.execute(create_info_query)
        conn.commit()
        print("CdrInfo table created.")

    print("Inserting totals into CdrInfo...")
    cursor.execute("INSERT INTO CdrInfo (TotalRecords, TotalInserted) VALUES (?, ?)", total_records_all, total_inserted)
    conn.commit()
    print(f"Inserted into CdrInfo: TotalRecords={total_records_all}, TotalInserted={total_inserted}")
    
    conn.close()

except Exception as e:
    print("Error occurred:", str(e))
    print("Common issues: Connection refused (wrong host/port/firewall), login failed (bad credentials), directory not found, or network problems.")

print("Download and organization complete.")