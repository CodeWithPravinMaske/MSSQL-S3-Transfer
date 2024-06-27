import os
import pyodbc
import boto3
import pandas as pd
import time
import configparser
import sys
from botocore.exceptions import NoCredentialsError
import traceback


def get_sql_server_connection(server_id, db_name, usr, pas, drv, prt):
    # Establishes a connection to MS SQL Server using pyodbc and returns the connection object.
    try:
        conn_str = f'DRIVER={drv};SERVER={server_id};DATABASE={db_name};UID={usr};PWD={pas};ApplicationIntent=ReadOnly'
        connection = pyodbc.connect(conn_str)
        print("SQL server connection successful")
        return connection
    except Exception as ex:
        # Handle exceptions
        print(f"An error occurred: {str(ex)}")
        traceback.print_exc()
        sys.exit(1)


def publish_csv_into_s3(csv_filename, s3_key_id, s3_bucket, iam_role, aws_access_key_id, aws_secret_access_key):
    try:
        # s3 client
        s3_client = get_s3_client(iam_role, aws_access_key_id, aws_secret_access_key)
        print('S3 client connection successful')
        # Function to upload CSV data in the provided buffer to AWS S3.
        s3_client.upload_file(csv_filename, s3_bucket, s3_key_id)
        print(f'Uploaded file to S3: s3://{s3_bucket}/{s3_key_id}')
    except NoCredentialsError:
        print("AWS credentials not found. Please provide valid credentials or use IAM role.")
        traceback.print_exc()
        sys.exit(1)
    except Exception as exx:
        print(f"An error occurred while uploading the file to S3: {str(exx)}")
        traceback.print_exc()
        sys.exit(1)

def remove_local_file(local_file_path):
    if os.path.exists(local_file_path):
        os.remove(local_file_path)
        print(f"Deleted local the CSV file: {local_file_path}")
    else:
        print(f"Local CSV file not found: {local_file_path}")


def get_sql_query(table, col_name, curr_date, nxt_month_str, query_param):
    # Function to generate the SQL query to fetch data in batches based on the date range.
    if query_param == 'None' or query_param == '':
        sql_query = f"""SELECT * FROM {table} WHERE {col_name} >= '{curr_date}' AND {col_name} < '{nxt_month_str}'"""
    else:
        sql_query = f"""SELECT * FROM {table} {query_param} WHERE {col_name} >= '{curr_date}' AND {col_name} < '{nxt_month_str}'"""
    print(sql_query)
    return sql_query


def get_s3_file_name(path, curr_date, nxt_month_str, curr_epoch_time, total_count):
    return f'{path}{curr_date}_{nxt_month_str}_{curr_epoch_time}_{str(total_count)}.csv'


def read_config_file(config_path):
    # Function to read the configuration file and return a dictionary with the configuration values.
    config_parser = configparser.ConfigParser()
    config_parser.read(config_path)
    return config_parser


def get_s3_client(iam_role_arn, access_key, secret_key):
    try:
        if iam_role_arn != 'None':
            # Create an STS client to assume the role
            sts_client = boto3.client('sts')

            # Assume the IAM role to get temporary credentials
            response = sts_client.assume_role(
                RoleArn=iam_role_arn,
                RoleSessionName='thyrocare-historical-backfill-session'  # Provide a session name
            )

            # Extract the temporary credentials
            credentials = response['Credentials']
            access_key_id = credentials['AccessKeyId']
            secret_access_key = credentials['SecretAccessKey']
            session_token = credentials['SessionToken']

            # Create an S3 client using the assumed role credentials
            return boto3.client(
                's3',
                aws_access_key_id=access_key_id,
                aws_secret_access_key=secret_access_key,
                aws_session_token=session_token
            )
        elif access_key != 'None' and secret_key != 'None':
            return boto3.client('s3', aws_access_key_id=access_key,
                                aws_secret_access_key=secret_key)
        else:
            return boto3.client('s3')
    except NoCredentialsError:
        print("AWS credentials not found. Please provide valid credentials or use IAM role.")
        traceback.print_exc()
        sys.exit(1)
    except Exception as e:
        # Handle exceptions
        print(f"An error occurred: {str(e)}")
        traceback.print_exc()
        sys.exit(1)


def add_counts_in_file(file_name, fin_count_df):
    # Save the grouped DataFrame to a CSV file with 'date' and 'count' as headers
    print(f'Adding data into count csv file')
    # If the file exists, append the data without writing the header
    fin_count_df.to_csv(file_name, index=False, header=True)


def main(configs):
    conn_config = configs['Database']
    server = conn_config['server']
    database = conn_config['database']
    if conn_config['username'] == 'None' and conn_config['password'] == 'None':
        if len(sys.argv) >= 3:
            username = sys.argv[2]
        else:
            print("Please pass mssql user name in config.ini or command line argument")
            sys.exit(1)
        if len(sys.argv) >= 4:
            password = sys.argv[3]
        else:
            print("Please pass mssql user name in config.ini or command line argument")
            sys.exit(1)
    else:
        username = conn_config['username']
        password = conn_config['password']
    port = int(conn_config['port'])
    driver = conn_config['driver']

    # AWS configuration details
    aws_config = configs['AWS']
    iam_role = aws_config['iam_role']
    aws_access_key_id = aws_config['aws_access_key_id']
    aws_secret_access_key = aws_config['aws_secret_access_key']
    s3_bucket = aws_config['s3_bucket']
    s3_path = aws_config['s3_path']

    # Table details and Date range parameters
    table_details = configs['TableDetails']
    table_name = table_details['table_name']
    query_param = table_details['query_param']
    date_column_name = table_details['date_column_name']
    start_date = table_details['start_date']
    end_date = table_details['end_date']

    # Count File details
    count_file_details = configs['CountFileDetails']
    is_count_file_required = count_file_details['is_count_file_required'].lower()
    count_file_name = count_file_details['count_file_name']

    # set batch type
    if table_details['batch_type'] == 'None' or table_details['batch_type'] == '':
        batch_type = "months"
    elif table_details['batch_type'] not in ["months", "days"]:
        batch_type = "months"
    else:
        batch_type = str(table_details['batch_type'])

    # set batch days or months
    if table_details['batch_days_months'] == 'None' or table_details['batch_days_months'] == '':
        batch_days_months = 1
    else:
        batch_days_months = int(table_details['batch_days_months'])

    # Initialize an empty DataFrame to accumulate counts
    total_count_df = pd.DataFrame(columns=['date', 'count'])

    try:
        # Fetch data in batches based on date range
        current_date = pd.to_datetime(start_date)
        total_file_size = 0
        while current_date <= pd.to_datetime(end_date):
            print(f'######################################################################')
            # Format date strings for the query
            start_date_str = current_date.strftime('%Y-%m-%d')

            if batch_type == "days":
                next_month_date = (current_date + pd.DateOffset(days=batch_days_months))
            else:
                next_month_date = (current_date + pd.DateOffset(months=batch_days_months))

            if next_month_date > pd.to_datetime(end_date):
                next_month_date = pd.to_datetime(end_date)
                next_month_str = next_month_date.strftime('%Y-%m-%d')
                print(f"Fetching data for start date : {start_date_str} and end date: {next_month_str}")
            else:
                next_month_str = next_month_date.strftime('%Y-%m-%d')
                print(f"Fetching data for start date : {start_date_str} and end date: {next_month_str}")

            current_epoch_time = int(time.time())
            csv_filename = f"{start_date_str}_{next_month_str}_{current_epoch_time}.csv"
            query = get_sql_query(table_name, date_column_name, start_date_str, next_month_str, query_param)

            # Establish the SQL Server connection
            conn = get_sql_server_connection(server, database, username, password, driver, port)
            start_time = time.time()  # Record the start time
            df = pd.read_sql(query, conn)
            end_time = time.time()  # Record the end time
            query_time_seconds = end_time - start_time  # Calculate query time in seconds
            query_time_minutes = query_time_seconds / 60  # Convert to minutes
            print(f"Query executed in {query_time_minutes:.2f} minutes")
            # Once we get the data we will close the connection
            conn.close()
            print(f"Closed the SQL server connection successfully")
            total_records = len(df)
            print(f'Total records: {str(total_records)}')
            if total_records != 0:
                df.to_csv(csv_filename, index=False, header=(total_file_size == 0))

                if is_count_file_required == 'true':
                    # Group by date_column_name and calculate the count for each date
                    grouped_df = df.groupby(df[date_column_name].dt.date).size().reset_index(name='count')
                    grouped_df.rename(columns={date_column_name: 'date'}, inplace=True)

                    # Append the batch's count DataFrame to the total_count_df
                    total_count_df = total_count_df.append(grouped_df, ignore_index=True)

                    del grouped_df

                del df

                s3_key = get_s3_file_name(s3_path, start_date_str, next_month_str, current_epoch_time, total_records)
                # Upload the CSV data to S3
                publish_csv_into_s3(csv_filename, s3_key, s3_bucket, iam_role, aws_access_key_id, aws_secret_access_key)
                remove_local_file(csv_filename)
            else:
                print(f"Getting 0 records in sql query : {query}")

            # Update the current_date to the next sub-range
            if batch_type == "days":
                current_date = current_date + pd.DateOffset(days=batch_days_months)
            else:
                current_date = current_date + pd.DateOffset(months=batch_days_months)

        if is_count_file_required == 'true':
            # Convert 'date' column to datetime format
            total_count_df['date'] = pd.to_datetime(total_count_df['date'])
            # Group by 'date' and sum the 'count' values
            grouped_df = total_count_df.groupby('date')['count'].sum().reset_index()
            del total_count_df
            print(f"Total rows in count_df: {len(grouped_df)}")
            if len(grouped_df) != 0:
                # Upload the count CSV data to S3
                grouped_df.to_csv(count_file_name, index=False)
                del grouped_df
                s3_count_key = f'{s3_path}{count_file_name}'
                publish_csv_into_s3(count_file_name, s3_count_key, s3_bucket, iam_role, aws_access_key_id, aws_secret_access_key)
                remove_local_file(count_file_name)
            else:
                print(f"We got 0 records in the same job, we are not creating __count.csv file !!!!!")
    except Exception as ex:
        # Handle exceptions
        print(f"An error occurred: {str(ex)}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    # Read database connection details from the config file
    if len(sys.argv) < 2:
        print("Usage: python3 script.py <config_file_name> <mssql_db_user> <mssql_db_password>")
        traceback.print_exc()
        sys.exit(1)

    config_file_path = sys.argv[1]
    config = read_config_file(config_file_path)

    main(config)
