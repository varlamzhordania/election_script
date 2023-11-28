import requests
import mysql.connector
from mysql.connector import errorcode
import time


# Function to make API call
def get_api_data(api_url):
    response = requests.get(api_url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: Unable to fetch data from the API. Status code: {response.status_code}")
        return None


# Function to connect to MySQL database
def connect_to_database():
    try:
        connection = mysql.connector.connect(
            host='your_mysql_host',
            user='your_mysql_user',
            password='your_mysql_password',
            database='your_mysql_database'
        )
        return connection
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Error: Access denied. Check your MySQL credentials.")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Error: Database does not exist.")
        else:
            print(f"Error: {err}")
        return None


# Function to check if data already exists in the table
def data_exists(cursor, table_name, unique_field, unique_value):
    cursor.execute(f"SELECT * FROM {table_name} WHERE {unique_field} = %s", (unique_value,))
    return cursor.fetchone() is not None


# Function to insert data into MySQL database for the government table
def insert_government_data(cursor, data):
    if not data_exists(cursor, 'government', 'district_ocd_id', data['district']['district_ocd_id']):
        cursor.execute(
            '''
                        INSERT INTO government (
                            district_ocd_id,
                            district_name,
                            district_country,
                            district_type,
                            government_functions_details,
                            government_functions_updated,
                            concurrent_elections,
                            institution_id,
                            institution_name,
                            institutiion_incumbency_allowed,
                            insititution_term_length,
                            insititution_term_limit,
                            insititution_term_details,
                            insititution_election_frequency,
                            institution_compulsory_voting,
                            prominent_political_groups_data,
                            prominent_political_groups_comments,
                            campaign_finance_legislation_exists,
                            campaign_finance_details,
                            official_results_data
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ''', (
                data['district']['district_ocd_id'],
                data['district']['district_name'],
                data['district']['district_country'],
                data['district']['district_type'],
                data['government_functions']['details'],
                data['government_functions']['updated'],
                data['concurrent_elections'],
                0,  # Replace with the actual institution_id if available
                data['institution']['name'],
                data['institution']['incumbency_allowed'],
                data['institution']['term_length'],
                data['institution']['term_limit'],
                data['institution']['term_details'],
                data['institution']['election_frequency'],
                data['institution']['compulsory_voting'],
                data['prominent_political_groups']['data'],
                data['prominent_political_groups']['comments'],
                data['campaign_finance']['legislation_exists'],
                data['campaign_finance']['details'],
                data['official_results_date']
            )
        )


# Function to insert data into MySQL database for the election table
def insert_election_data(cursor, data):
    government_record_id = insert_government_data(cursor, data)

    if not data_exists(cursor, 'election', 'election_id', data['election_id']):
        cursor.execute(
            '''
                        INSERT INTO election (
                            election_id,
                            government_record_id,
                            election_key,
                            election_date_updated,
                            election_name,
                            election_name_encoding,
                            election_issues,
                            is_snap_election,
                            original_election_year,
                            election_range_start_date,
                            election_range_end_date,
                            is_delayed_covid19,
                            covid_effects,
                            election_declared_start_date,
                            election_declared_end_date,
                            election_blackout_start_date,
                            election_blackout_end_date,
                            election_type,
                            election_scope,
                            electoral_system,
                            electoral_system_other,
                            election_commission_name,
                            administering_election_commission_website,
                            election_commission_historical_context,
                            source,
                            third_party_verified_is_verified,
                            third_party_verified_date
                        )
                        VALUES (%s, %s, %s, CURRENT_TIMESTAMP(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ''', (
                data['election_id'],
                government_record_id,  # Replace with the actual government_record_id if available
                data['election_key'],
                data['election_name']['en_US'],
                'en_US',  # Assuming 'en_US' is the language for election_name
                data['election_issues'],
                data['is_snap_election'],
                data['original_election_year'],
                data['election_range_start_date'],
                data['election_range_end_date'],
                data['is_delayed_covid19'],
                data['covid_effects'],
                data['election_declared_start_date'],
                data['election_declared_end_date'],
                data['election_blackout_start_date'],
                data['election_blackout_end_date'],
                data['election_type'],
                data['election_scope'],
                data['electoral_system'],
                data['electoral_system_other'],
                data['election_commission_name'],
                data['administering_election_commission_website'],
                data['election_commission_historical_context'],
                data['source'],
                data['third_party_verified']['is_verified'],
                data['third_party_verified']['date']
            )
        )


# Function to insert data into MySQL database for the updates table
def insert_updates_data(cursor, data):
    cursor.execute(
        '''
                INSERT INTO updates (
                    election_id,
                    basic_updated,
                    status_updated,
                    voters_updated,
                    institute_updated,
                    events_updated,
                    issues_updated,
                    inforights_updated
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ''', (
            data['election_id'],
            data['detail_updates']['basic_updated'],
            data['detail_updates']['status_updated'],
            data['detail_updates']['voters_updated'],
            data['detail_updates']['institue_updated'],
            data['detail_updates']['events_updated'],
            data['detail_updates']['issues_updated'],
            data['detail_updates']['inforights_updated']
        )
    )


# Function to insert data into MySQL database for the voter table
def insert_voter_data(cursor, data):
    if not data_exists(cursor, 'voter', 'election_id', data['election_id']):
        cursor.execute(
            '''
                        INSERT INTO voter (
                            election_id,
                            historical_voter_turnout_data,
                            historical_voter_turnout_comments,
                            voter_registration_day,
                            voter_registration_deadline,
                            voting_age_minimum_inclusive,
                            eligible_voters,
                            first_time_voters,
                            voting_methods_type,
                            voting_methods_primary,
                            voting_methods_start_date,
                            voting_methods_end_date,
                            voting_methods_execuse_required,
                            voting_methods_instructions
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ''', (
                data['election_id'],
                data['historical_voter_turnout']['data'],
                data['historical_voter_turnout']['comments'],
                data['voter_registration_day'],
                data['voter_registration_deadline'],
                data['voting_age_minimum_inclusive'],
                data['eligible_voters'],
                data['first_time_voters'],
                data['voting_methods'][0]['type'],  # Assuming only one voting method is present
                data['voting_methods'][0]['primary'],
                data['voting_methods'][0]['start'],
                data['voting_methods'][0]['end'],
                data['voting_methods'][0]['excuse-required'],
                data['voting_methods'][0]['instructions']
            )
        )


# Replace 'YOUR_API_ENDPOINT' with the actual API endpoint
api_endpoint = 'YOUR_API_ENDPOINT'

# Connect to MySQL database
connection = connect_to_database()
if connection is None:
    exit()

cursor = connection.cursor()

try:
    # Make API call
    api_data = get_api_data(api_endpoint)

    if api_data:
        # Insert data into the database for each table
        insert_election_data(cursor, api_data)
        insert_updates_data(cursor, api_data)
        insert_voter_data(cursor, api_data)

        print("Data inserted into the database.")
except KeyboardInterrupt:
    print("Script terminated by user.")
except Exception as e:
    print("Error : ", e)
finally:
    # Close the database connection
    connection.close()
