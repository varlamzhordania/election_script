import requests
import mysql.connector
from mysql.connector import errorcode
from datetime import datetime, timedelta
import json


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


# Function to connect to MySQL database
def connect_to_database():
    try:
        connection = mysql.connector.connect(
            host='localhost',
            user='root',
            password='',
            database='election'
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


def data_exists_eguide_election(cursor, election_id):
    cursor.execute("SELECT * FROM eguide_election WHERE election_id = %s", (election_id,))
    return cursor.fetchone() is not None


def insert_eguide_election_data(cursor, data):
    election_id = data['election_id']

    # Check if data already exists
    if data_exists_eguide_election(cursor, election_id):
        print(f"Data already exists for election_id: {election_id}. Skipping insertion.")
        return

    # Extracting specific fields from the JSON data
    election_data = {
        'election_id': election_id,
        'election_name_encode': 'en_US',  # Assuming 'en_US' is the language for election_name
        'election_name': data['election_name']['en_US'],
        'election_date_updated': data.get('date_updated', None),
        'election_issues': data.get('election_issues', None),
        'is_snap_election': data.get('is_snap_election', None),
        'original_election_year': data.get('original_election_year', None),
        'election_range_start_date': data.get('election_range_start_date', None),
        'c': data.get('election_range_end_date', None),
        'is_delayed_covid19': data.get('is_delayed_covid19', None),
        'covid_effects': data.get('covid_effects', None),
        'election_declared_start_date': data.get('election_declared_start_date', None),
        'election_declared_end_date': data.get('election_declared_end_date', None),
        'election_blackout_start_date': data.get('election_blackout_start_date', None),
        'election_blackout_end_date': data.get('election_blackout_end_date', None),
        'election_type': data.get('election_type', None),
        'election_scope': data.get('election_scope', None),
        'electoral_system': data.get('electoral_system', None),
        'election_commission_name': data.get('election_commission_name', None),
        'administring_election_commission_website': data.get('administering_election_commission_website', None),
        'government_functions': data['government_functions']['details'],
        'government_functions_updated_date': data['government_functions']['updated'],
        'voter_registration_day_deadline': data.get('voter_registration_day', None),
        'voting_age_minimum_inclusive': data.get('voting_age_minimum_inclusive', None),
        'eligible_voters': data.get('eligible_voters', None),
        'first_time_voters': data.get('first_time_voters', None),
        'voting_methods_type': ';'.join([method['type'] for method in data.get('voting_methods', [])]),
        'voting_methods_primary': ';'.join([method['primary'] for method in data.get('voting_methods', [])]),
        'voting_methods_start_date': ';'.join([method['start'] for method in data.get('voting_methods', [])]),
        'voting_methods_end_date': ';'.join([method['end'] for method in data.get('voting_methods', [])]),
        'voting_methods_execuse_required': ';'.join([method['excuse-required'] for method in data.get('voting_methods', [])]),
        'voting_methods_instructions': ';'.join([method['instructions'] for method in data.get('voting_methods', [])]),
    }

    # Calculate election_range_end_date
    if election_data['election_range_start_date']:
        start_date = datetime.strptime(election_data['election_range_start_date'], '%Y-%m-%d')
        six_months = start_date + timedelta(days=180)
        election_data['election_range_end_date'] = six_months.strftime('%Y-%m-%d')

    # Inserting data into the eguide_election table
    cursor.execute(
        '''
        INSERT INTO eguide_election (
            election_id,
            election_name_encode,
            election_name,
            election_date_updated,
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
            election_commission_name,
            administring_election_commission_website,
            government_functions,
            government_functions_updated_date,
            voter_registration_day_deadline,
            voting_age_minimum_inclusive,
            eligible_voters,
            first_time_voters,
            voting_methods_type,
            voting_methods_primary,
            voting_methods_start_date,
            voting_methods_end_date,
            voting_methods_execuse_required,
            voting_methods_instructions,
        )
        VALUES (
            %(election_id)s,
            %(election_name_encode)s,
            %(election_name)s,
            %(election_date_updated)s,
            %(election_issues)s,
            %(is_snap_election)s,
            %(original_election_year)s,
            %(election_range_start_date)s,
            %(election_range_end_date)s,
            %(is_delayed_covid19)s,
            %(covid_effects)s,
            %(election_declared_start_date)s,
            %(election_declared_end_date)s,
            %(election_blackout_start_date)s,
            %(election_blackout_end_date)s,
            %(election_type)s,
            %(election_scope)s,
            %(electoral_system)s,
            %(election_commission_name)s,
            %(administring_election_commission_website)s,
            %(government_functions)s,
            %(government_functions_updated_date)s,
            %(voter_registration_day_deadline)s,
            %(voting_age_minimum_inclusive)s,
            %(eligible_voters)s,
            %(first_time_voters)s,
            %(voting_methods_type)s,
            %(voting_methods_primary)s,
            %(voting_methods_start_date)s,
            %(voting_methods_end_date)s,
            %(voting_methods_execuse_required)s,
            %(voting_methods_instructions)s,
        )
        ''', election_data
    )


# Replace 'YOUR_API_ENDPOINT' with the actual API endpoint
api_endpoint = 'https://electionguide.org/api/v2/elections_demo/'
api_token = 'defad26f5b65919a9fbcd545c9a78a903dafd7d6'

# Connect to MySQL database
connection = connect_to_database()
if connection is None:
    exit()

cursor = connection.cursor()

try:
    # Make API call
    api_data = get_api_data(api_endpoint, api_token)

    if api_data:
        for object in api_data:
            print("==============")
            print(f"starting insert eguide_election, election id : {object['election_id']}")
            insert_eguide_election_data(cursor, object)

        print("Data inserted into the database.")
except KeyboardInterrupt:
    print("Script terminated by user.")
except Exception as e:
    print("Error : ", e)
finally:
    # Close the database connection
    connection.close()
