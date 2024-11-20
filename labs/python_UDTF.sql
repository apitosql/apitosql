-- Used library: requests
-- Used Snowflake objects with dependecy order: RULE, EXTERNAL ACCESS INTEGRATION, FUNCTION
-- Type of function: Python UDTF
-- Simple exp: User give white list, we create intergation from this list and function get JSON payload flatten JSON payload
-- Operation type: row based
-- Cost: I need to wait for at least 2 hours
-- Used url: https://jsonplaceholder.typicode.com/todos
-- Result 200 rows (3 granulity) are fetched and process takes 2.4s
-- Query ID: 01b85450-0000-3b43-0000-1aa900041146

use database capybara;

CREATE OR REPLACE NETWORK RULE typicode_maps_network_rule
MODE = EGRESS
TYPE = HOST_PORT
VALUE_LIST = ('jsonplaceholder.typicode.com');

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION
typicode_maps_access_integration
ALLOWED_NETWORK_RULES = (typicode_maps_network_rule)
ENABLED = true;


CREATE OR REPLACE FUNCTION api_json_to_flatten()
    RETURNS TABLE (id STRING, title STRING, completed BOOLEAN)
    LANGUAGE PYTHON
    RUNTIME_VERSION = 3.8
    PACKAGES=('requests')
    EXTERNAL_ACCESS_INTEGRATIONS = (typicode_maps_access_integration)
    HANDLER = 'handler'
AS
$$
import requests
class handler:
    def process(self):
        # API endpoint to get all todos
        api_endpoint = 'https://jsonplaceholder.typicode.com/todos'
        response = requests.get(api_endpoint)
    
        # Check if the request was successful
        if response.status_code == 200:
            json_data = response.json()
        
            # Yield each todo item as a row
            for todo in json_data:
                yield todo['id'], todo['title'], todo['completed']
        else:
            # Handle the case where the API request fails
            raise Exception(f"API request failed with status code {response.status_code}")
$$;


SELECT * FROM TABLE(CAPYBARA.PUBLIC.api_json_to_flatten());



