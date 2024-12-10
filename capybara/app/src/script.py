# import json
import simplejson as json
import requests
from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark import Row
import _snowflake
import pandas as pd

def init_app(session: Session, config) -> str:
  """
    Initializes function API endpoints with access to the secret and API integration.

    Args:
      session (Session): An active session object for authentication and communication.
      config (Any): The configuration settings for the connector.

    Returns:
      str: A status message indicating the result of the provisioning process.
   """
  external_access_integration_name = config['external_access_integration_name']

  alter_function_sql = f'''
    ALTER FUNCTION code_schema.api_to_sql() SET 
    EXTERNAL_ACCESS_INTEGRATIONS = ({external_access_integration_name})'''
  
  session.sql(alter_function_sql).collect()

  return 'Snowflake translation app initialized'

import requests

def api_to_sql():
    # API endpoint to get all todos
    api_endpoint = 'https://jsonplaceholder.typicode.com/todos'
    response = requests.get(api_endpoint)
    
    # Check if the request was successful
    if response.status_code == 200:
        json_data = response.json()
        
        # Combine all todos into a single string
        all_todos = "\n".join(str(todo) for todo in json_data)
        return all_todos
    
    else:
        # Return error message if the request fails
        return f"Failed to fetch data. Status code: {response.status_code}"
        
    