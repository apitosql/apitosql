import _snowflake
import json
import pandas
import uuid
from datetime import datetime

def insert_to_table(session, config_name, base_url, api_params_str, headers_str):
    api_params_str = api_params_str.replace(" ", "").replace("'", "")
    api_params = json.loads(api_params_str)
    config_id = str(uuid.uuid4())
    data = {
            "config_id": config_id ,
            "config_name": config_name,
            "date_created" : datetime.now(),
            "headers": f'{headers_str}',
            "base_url": base_url
            }
    data_ti = [{**data, **api_params}]
    df = pandas.DataFrame.from_records(data_ti)
    snowpark_df = session.create_dataframe(data=df)
    snowpark_df.write.mode("append").save_as_table('conn_metadata')   

    # writing dummy to run config
    run_data = [{
            "config_id": config_id,
            "config_name": config_name,
            "desc": 'initial',
            "run_time" : datetime.now(),
            "load_status": False,
            'query': 'n'
            }]
    df = pandas.DataFrame.from_records(run_data)
    snowpark_df = session.create_dataframe(data=df)
    snowpark_df.write.mode("append").save_as_table('run_metadata') 

def main(session, config_name, base_url, secret_token, api_params_str, headers_str):

        # insert into configs table
        insert_to_table(session, config_name, base_url, api_params_str, headers_str)

        # creationg necesary resouces
        base_url = base_url.replace('https://', '').replace('http://', '')
        sql_statements = [
            f"""
            CREATE OR REPLACE NETWORK RULE {config_name}_as
            MODE = EGRESS
            TYPE = HOST_PORT
            VALUE_LIST = ('{base_url}') ;
            """,
            f"""
            CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION {config_name}_int
            ALLOWED_NETWORK_RULES = ({config_name}_as)
            ENABLED = true;
            """,
            f"""
            CREATE OR REPLACE SECRET {config_name}_key
              TYPE = GENERIC_STRING
              SECRET_STRING = '{secret_token}';

            """
            
        ]
        results = []
        for sql in sql_statements:
            result = session.sql(sql).collect()
            results.append(result[0]['status'])
        
        return str(results)

