import requests
import pyodbc
import json
from datetime import datetime

# Function to make API call
def get_api_data(api_url, token):
    response = requests.get(
        api_url, headers={
            "Authorization": f"Token {token}"
        }
    )
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: Unable to fetch data from the API. Status code: {response.status_code}")
        return None

# Function to connect to Microsoft SQL Server database
def connect_to_database():
    try:
        connection = pyodbc.connect(
            'DRIVER={SQL Server};'
            'SERVER=localhost;'
            'DATABASE=election;'
            'UID=your_username;'
            'PWD=your_password'
        )
        return connection
    except pyodbc.Error as err:
        print(f"Error: {err}")
        return None

# Function to check if data already exists in the table
def data_exists(cursor, table_name, unique_field, unique_value):
    cursor.execute(f"SELECT * FROM {table_name} WHERE {unique_field} = ?", unique_value)
    return cursor.fetchone() is not None

# Function to insert data into Microsoft SQL Server database for the government table
def insert_government_data(cursor, data):
    district_ocd_id = data['district']['district_ocd_id']

    cursor.execute(
        '''
        SELECT government_record_id FROM government WHERE district_ocd_id = ?
        ''', district_ocd_id
    )

    existing_record = cursor.fetchone()

    if existing_record:
        # If the row already exists, return the existing government_record_id
        return existing_record[0]
    else:
        # If the row doesn't exist, insert a new row and return the auto-incremented government_record_id
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
                institution_incumbency_allowed,
                insititution_term_length,
                insititution_term_limit,
                institution_term_details,
                insititution_election_frequency,
                institution_compulsory_voting,
                prominent_political_groups_data,
                prominent_political_groups_comments,
                campaign_finance_legislation_exists,
                campaign_finance_details,
                official_results_data
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                district_ocd_id,
                data['district']['district_name'],
                data['district']['district_country'],
                data['district']['district_type'],
                data['government_functions']['details'],
                data['government_functions']['updated'],
                json.dumps(data['concurrent_elections']),
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

        # Return the auto-incremented government_record_id
        return cursor.execute("SELECT SCOPE_IDENTITY()").fetchone()[0]

# Function to insert data into Microsoft SQL Server database for the election table
def insert_election_data(cursor, data, government_record_id):
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
                        VALUES (?, ?, ?, CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        return cursor.execute("SELECT SCOPE_IDENTITY()").fetchone()[0]
    else:
        cursor.execute(
            '''
            SELECT election_record_id FROM election WHERE election_id = ?
            ''', (data['election_id'],)
        )
        existing_record = cursor.fetchone()
        return existing_record[0]

# Function to insert data into Microsoft SQL Server database for the updates table
def insert_updates_data(cursor, data, election_record_id):
    if not data_exists(cursor, 'updates', 'election_record_id', election_record_id):
        cursor.execute(
            '''
                    INSERT INTO updates (
                        election_record_id,
                        basic_updated,
                        status_updated,
                        voters_updated,
                        institute_updated,
                        events_updated,
                        issues_updated,
                        inforights_updated
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                election_record_id,
                data['detail_updates']['basic_updated'],
                data['detail_updates']['status_updated'],
                data['detail_updates']['voters_updated'],
                data['detail_updates']['institue_updated'],
                data['detail_updates']['events_updated'],
                data['detail_updates']['issues_updated'],
                data['detail_updates']['inforights_updated']
            )
        )

# Function to insert data into Microsoft SQL Server database for the voter table
def insert_voter_data(cursor, data, election_record_id):
    if not data_exists(cursor, 'voter', 'election_record_id', election_record_id):
        voting_methods = data['voting_methods']
        voting_methods_type = ""
        voting_methods_primary = ""
        voting_methods_start_date = ""
        voting_methods_end_date = ""
        voting_methods_execuse_required = ""
        voting_methods_instructions = ""
        if voting_methods:
            for method in voting_methods:
                voting_methods_type += f"{method['type']};;"
                voting_methods_primary += f"{method['primary']};;"
                voting_methods_start_date += f"{method['start']};;"
                voting_methods_end_date += f"{method['end']};;"
                voting_methods_excuse_required += f"{method['excuse-required']};;"
                voting_methods_instructions += f"{method['instructions']};;"

        cursor.execute(
            '''
                        INSERT INTO voter (
                            election_record_id,
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
                            voting_methods_excuse_required,
                            voting_methods_instructions
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                election_record_id,
                data['historical_voter_turnout']['data'],
                data['historical_voter_turnout']['comments'],
                data['voter_registration_day'],
                data['voter_registration_deadline'],
                data['voting_age_minimum_inclusive'],
                data['eligible_voters'],
                data['first_time_voters'],
                voting_methods_type,
                voting_methods_primary,
                voting_methods_start_date,
                voting_methods_end_date,
                voting_methods_excuse_required,
                voting_methods_instructions,
            )
        )

def insert_candidate_data(cursor, data, election_record_id):
    candidate_selection = data['candidate_selection']
    candidate_declare_start_date = data['candidate_declare_start_date']
    candidate_declare_end_date = data['candidate_declare_end_date']
    election_candidate_filing_deadline = data['election_candidate_filing_deadline']
    if not data_exists(cursor, 'candidate', 'election_record_id', election_record_id):
        cursor.execute(
            '''
            INSERT INTO candidate (
                election_record_id,
                candidate_selectiion,
                candidate_declare_start_date,
                candidate_declare_end_date,
                election_candidate_filing_deadline
            )
            VALUES (?, ?, ?, ?, ?)
            ''', (
                election_record_id,
                candidate_selection,
                candidate_declare_start_date,
                candidate_declare_end_date,
                election_candidate_filing_deadline
            )
        )
        # Return the auto-incremented candidate_record_id
        return cursor.execute("SELECT SCOPE_IDENTITY()").fetchone()[0]

# Replace 'YOUR_API_ENDPOINT' with the actual API endpoint
api_endpoint = 'https://electionguide.org/api/v2/elections_demo/'
api_token = 'defad26f5b65919a9fbcd545c9a78a903dafd7d6'

# Connect to Microsoft SQL Server database
connection = connect_to_database()
if connection is None:
    exit()

cursor = connection.cursor()

try:
    # Make API call
    api_data = get_api_data(api_endpoint, api_token)

    if api_data:
        for obj in api_data:
            # Insert data into the database for each table
            print("==============")
            print(f"starting insert government, election id : {obj['election_id']}")
            government_record_id = insert_government_data(cursor, obj)
            print("==============")
            print(f"starting insert election, election id : {obj['election_id']}")
            election_record_id = insert_election_data(cursor, obj, government_record_id)
            print("==============")
            print(f"starting insert updates, election id : {obj['election_id']}")
            insert_updates_data(cursor, obj, election_record_id)
            print("==============")
            print(f"starting insert voter, election id : {obj['election_id']}")
            insert_voter_data(cursor, obj, election_record_id)
            print("==============")
            print(f"starting insert candidate, election id : {obj['election_id']}")
            insert_candidate_data(cursor, obj, election_record_id)

        print("Data inserted into the database.")
except KeyboardInterrupt:
    print("Script terminated by user.")
except Exception as e:
    print("Error : ", e)
finally:
    # Close the database connection
    connection.close()
