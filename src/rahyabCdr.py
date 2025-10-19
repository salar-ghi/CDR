import ftplib
import os
import gzip
import pyodbc
import csv
from datetime import datetime
import time

# FTP connection details
FTP_HOST = "ftp.rahyab.ir"
FTP_USER = "ofogh"
FTP_PASS = "OF@#rahyab2025"
FTP_DIR = "OFOGH"

# Local directory to download files
DOWNLOAD_DIR = "downloads"
if not os.path.exists(DOWNLOAD_DIR):
    os.makedirs(DOWNLOAD_DIR)
print("Step 1: Created downloads folder if not exists.")

# Month mapping
MONTH_NAMES = {
    1: "January", 2: "February", 3: "March", 4: "April",
    5: "May", 6: "June", 7: "July", 8: "August",
    9: "September", 10: "October", 11: "November", 12: "December"
}

def parse_month_from_filename(filename):
    try:
        date_part = filename.split('_')[1].split('.')[0]
        parts = date_part.split('-')
        month_num = int(parts[1])
        if 1 <= month_num <= 12:
            return MONTH_NAMES[month_num]
    except (IndexError, ValueError):
        pass
    return None

def parse_date_from_filename(filename):
    try:
        date_part = filename.split('_')[1].split('.')[0]
        parts = date_part.split('-')
        year = int(parts[0])
        month = int(parts[1])
        day = int(parts[2])
        return f"{year}-{month:02d}-{day:02d}"
    except (IndexError, ValueError):
        return None

def validate_and_format_datetime(dt_str):
    try:
        # Parse the datetime string (e.g., "2025-10-10 06:49:33.666666")
        dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S.%f")
        # Format to datetime2(2) compatible string (2 decimal places, e.g., "2025-10-10 06:49:33.66")
        return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-4]
    except ValueError as e:
        print(f"Invalid datetime format: {dt_str}, Error: {str(e)}")
        return None

# Database connection details
DB_SERVER = "172.16.17.22"
DB_USER = "sa"
DB_PASS = "Kami1351@"  # Ensure this matches your SQL Server password
DB_NAME = "SmsCdr"
CONN_STR = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={DB_SERVER};DATABASE={DB_NAME};UID={DB_USER};PWD={DB_PASS}"

# Connect to FTP with error handling
try:
    print("Step 2: Attempting to connect to FTP...")
    with ftplib.FTP(FTP_HOST) as ftp:
        print("Connected to host.")
        
        print("Step 3: Logging in...")
        ftp.login(FTP_USER, FTP_PASS)
        print("Logged in successfully.")
        
        ftp.set_pasv(True)  # Enable passive mode
        print("Passive mode enabled.")
        
        print("Step 4: Changing to directory:", FTP_DIR)
        ftp.cwd(FTP_DIR)
        print("Directory changed successfully.")
        
        print("Step 5: Listing files...")
        files = ftp.nlst()
        print("Files in directory:", files)
        
        # Filter files with prefix cdr_2025 and end with .gz
        target_files = [f for f in files if f.startswith('cdr_2025') and f.endswith('.gz')]
        print("Target files found:", target_files)
        
        if not target_files:
            print("No matching files found. Check prefixes, extensions, or directory contents.")
        
        for filename in target_files:
            local_path = os.path.join(DOWNLOAD_DIR, filename)
            
            print(f"Step 6: Downloading {filename}...")
            with open(local_path, 'wb') as local_file:
                ftp.retrbinary(f"RETR {filename}", local_file.write)
            print(f"Downloaded: {filename}")
            
            # Process the .gz file directly
            print(f"Step 7: Processing {filename}...")
            with gzip.open(local_path, 'rt', encoding='utf-8') as g:
                reader = csv.reader(g, delimiter=',', quotechar='"')
                rows = list(reader)
            
            total_records = len(rows)
            print(f"Total records in file: {total_records}")
            
            delivery_records = 0
            data_to_insert = []
            valid_6_field_count = 0
            skipped_count = 0
            unique_statuses = set()
            error_rows = []  # To log problematic rows
            
            # Print first 5 non-empty rows for debugging
            sample_count = 0
            for row in rows:
                if row and sample_count < 5:
                    print(f"Sample row {sample_count + 1}: {row}")
                    sample_count += 1
            
            # Main loop
            for row in rows:
                if not row:
                    skipped_count += 1
                    error_rows.append((row, "Empty row"))
                    continue  # Skip empty rows
                
                if len(row) != 6:
                    skipped_count += 1
                    error_rows.append((row, f"Invalid field count: {len(row)}"))
                    continue  # Skip invalid row counts
                
                valid_6_field_count += 1
                
                sms_id = str(row[0])[:50]  # Ensure string and truncate if needed
                delivered_time = row[1]
                source = str(row[2])[:50]  # Ensure string and truncate
                destination = str(row[3])[:50]  # Ensure string and truncate
                sms_status = str(row[4])[:50]  # Truncate status
                
                # Validate and format datetime
                formatted_time = validate_and_format_datetime(delivered_time)
                if not formatted_time:
                    skipped_count += 1
                    error_rows.append((row, f"Invalid datetime: {delivered_time}"))
                    continue
                
                unique_statuses.add(sms_status.upper())  # Collect for debug
                
                if sms_status.upper() == "DELIVERED":
                    delivery_records += 1
                    data_to_insert.append((sms_id, destination, source, formatted_time, "DELIVERED", "ofoghtd", 2))
            
            print(f"Valid lines with exactly 6 fields: {valid_6_field_count}")
            print(f"Skipped lines (empty, wrong field count, or invalid data): {skipped_count}")
            print(f"Unique sms_status values (uppercased): {sorted(list(unique_statuses))}")
            print(f"Delivery records to insert: {delivery_records}")
            if error_rows:
                print(f"First 5 error rows (if any): {error_rows[:5]}")
            
            # Parse month and date
            month_name = parse_month_from_filename(filename)
            cdr_date = parse_date_from_filename(filename)
            if not month_name or not cdr_date:
                print(f"Skipping {filename}: Could not parse month/date.")
                continue
            
            table_name = f"Cdr{month_name}"
            
            # Connect to DB
            conn = pyodbc.connect(CONN_STR)
            cur = conn.cursor()
            
            cur.fast_executemany = True

            # Create table if not exists
            create_table_sql = f"""
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{table_name}' AND xtype='U')
            CREATE TABLE [dbo].[{table_name}] (
                [SmsId] [bigint] NULL,
                [Destination] [varchar](50) NULL,
                [Source] [varchar](50) NULL,
                [DeliveredTime] [datetime2](2) NULL,
                [SmsStatus] [varchar](50) NULL,
                [User] [varchar](50) NULL,
                [DeliveryStatusId] [tinyint] NULL
            )
            """
            cur.execute(create_table_sql)
            conn.commit()
            inserted = 0
            success = True
            if data_to_insert:
                insert_sql = f"INSERT INTO [dbo].[{table_name}] ([SmsId], [Destination], [Source], [DeliveredTime], [SmsStatus], [User], [DeliveryStatusId]) VALUES (?, ?, ?, ?, ?, ?, ?)"
                batch_size = 30000
                for i in range(0, len(data_to_insert), batch_size):
                    batch = data_to_insert[i:i + batch_size]
                    try:
                        cur.executemany(insert_sql, batch)
                        conn.commit()
                        inserted += len(batch)
                        print(f"Inserted batch: {inserted} / {delivery_records}")
                    except Exception as e:
                        print(f"Error inserting batch: {e}")
                        error_rows.append((batch[:5], str(e)))  # Log first 5 rows of failed batch
                        success = False
                        break #Stop on error
            else:
                inserted = 0
            
            # Create CdrInfo if not exists
            create_cdrinfo_sql = """
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='CdrInfo' AND xtype='U')
            CREATE TABLE [dbo].[CdrInfo] (
                [Id] int IDENTITY(1,1) NOT NULL,
                [TotalRecords] bigint NOT NULL,
                [TotalDeliveryRecords] bigint NOT NULL,
                [TotalInserted] bigint NOT NULL,
                [SendSmsRecords] bigint NOT NULL,
                [DifferentRecords] bigint NOT NULL,
                [CdrDeliveredDate] datetime2 NOT NULL,
                [CdrHandling] tinyint,  -- 1: Automate / 2: Manual
                [CreatedDate] datetime2,
            )
            """
            cur.execute(create_cdrinfo_sql)
            conn.commit()
            
            # Insert into CdrInfo
            now = datetime.now()
            different_records = total_records - delivery_records
            send_sms_records = delivery_records
            cur.execute("""
                INSERT INTO [dbo].[CdrInfo] (
                    [TotalRecords], [TotalDeliveryRecords], [TotalInserted], [SendSmsRecords],
                    [DifferentRecords], [CdrDeliveredDate], [CdrHandling], [CreatedDate]
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (total_records, delivery_records, inserted, send_sms_records,
                  different_records, cdr_date, 2, now))
            conn.commit()
            
            conn.close()
            print(f"Processed {filename}: Total={total_records}, Delivered={delivery_records}, Inserted={inserted}")
            if error_rows:
                print(f"Error rows (first 5): {error_rows[:5]}")
            
            if success:
                # Move file on FTP to CDR_[short_month_name] folder
                cdr_folder = f"CDR_{month_name}"
                try:
                    ftp.cwd(cdr_folder)
                    ftp.cwd('..')  # If exists, go back
                except ftplib.error_perm:
                    print(f"Creating folder {cdr_folder} on FTP...")
                    ftp.mkd(cdr_folder)
                
                new_path = f"{cdr_folder}/{filename}"
                print(f"Moving {filename} to {new_path} on FTP...")
                try:
                    ftp.rename(filename, new_path)
                    print(f"Moved {filename} to {new_path}")
                except ftplib.error_perm as e:
                    print(f"Failed to move {filename} to {new_path}: {str(e)}")
                
                # Delete local file
                try:
                    os.remove(local_path)
                    print(f"Deleted local file: {local_path}")
                except OSError as e:
                    print(f"Failed to delete local file {local_path}: {str(e)}")
            else:
                print(f"Insertion failed for {filename}, not moving or deleting.")

            print(" =====================>>>>>>>>>>>>>> Task started")
            time.sleep(30)  # Pauses execution for 30 seconds
            print("Task resumed after 30 seconds")

except Exception as e:
    print("Error occurred:", str(e))

print("Script execution complete.")