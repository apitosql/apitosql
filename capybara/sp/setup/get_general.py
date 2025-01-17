import _snowflake
import json
import pandas
import uuid
from datetime import datetime
import re


def extract_config_name(sql_query):
    match = re.search(r"\bFROM\b\s+([\w\.]+)", sql_query, re.IGNORECASE)  
    if match:
        return match.group(1).strip()
    return None
    
def main(session, sql_code ):
    ress = []
    config_name = extract_config_name(sql_code)

    # generate unique identifier for temp procedures 
    random_id = str(uuid.uuid4()).replace('-', '')[:5]

    # create child sp for async run
    parent_sp = 'load_data_api_' + random_id
    child_sp = 'load_page_api_' + random_id

    # create child sp
    sql = f'''
        CREATE OR REPLACE  procedure core.{child_sp} (url string, query_params_str string, load_untill int, ppc int,ppc_field string, pn_field string, init_pg int, root string, tab string)
        RETURNS STRING
        LANGUAGE PYTHON
        RUNTIME_VERSION = 3.8
        HANDLER = 'child_api.load_data_with_offset'
        EXTERNAL_ACCESS_INTEGRATIONS = (api_int)
        PACKAGES = ('snowflake-snowpark-python','requests','pandas')
        IMPORTS = ('@~/prep/child_api.py')
    '''
    result = session.sql(sql).collect()
    ress.append( result[0]['status'])

    # create temp parent sp and run everything
    sql = f'''
        with get_data_api as procedure ( child_sp string, sql_code string)
        RETURNS STRING
        LANGUAGE PYTHON
        RUNTIME_VERSION = 3.8
        HANDLER = 'parent_api.get_data_api'
        EXTERNAL_ACCESS_INTEGRATIONS = (apisan_int)
        PACKAGES = ('snowflake-snowpark-python','requests','pandas')
        imports = ('@~/prep/parent_api.py')
        call get_data_api('core.{child_sp}', 'select * from apisan where money = 44 and teri = ''lu34te'' ')

    '''
    result = session.sql(sql).collect()
    ress.append( str(result[0]))

    # drop child sp
    sql = f'''
        DROP PROCEDURE {child_sp} ( string,  string,  int,  int, string,  string,  int,  string,  string)
    
    '''
    result = session.sql(sql).collect()
    ress.append( result[0]['status'])
    return str(ress)
