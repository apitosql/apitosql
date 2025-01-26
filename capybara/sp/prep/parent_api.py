import _snowflake
import requests
import json
import pandas
import re
from datetime import datetime
import time

ses = requests.Session()
baseurl = 'https://bigquery.googleapis.com/bigquery/v2/projects/'
sk_coef = 50

def extract_connection_name(sql_query):
    match = re.search(r"\bFROM\b\s+([\w\.]+)", sql_query, re.IGNORECASE)  
    if match:
        return match.group(1).strip()
    return None

def get_params(sql_code):
    pattern = r"(\w+)\s*(<=|>=|<|>|=)\s*('[^']*'|\d+\.\d+|\d+|\d{4}-\d{2}-\d{2})"
    matches = re.findall(pattern, sql_code)
    result_dict = {}
    for match in matches:
        attribute = match[0]
        operator = match[1]
        value = int(match[2].strip("'")) if match[2].strip("'").isnumeric() else match[2].strip("'")
        result_dict[attribute] = value
    return result_dict

def get_configs(session, connection_name):
    metadata_query = f"""
        SELECT *
        FROM code_schema.conn_metadata
        WHERE "config_name" = '{connection_name}'
    """
    results = session.sql(metadata_query).collect()
    for row in results:
        return row

def get_run_configs(session, connection_name, new_query):
    new_query = new_query.replace("'", "")
    metadata_query = f"""
        SELECT *
        FROM code_schema.run_metadata
        WHERE "config_name" = '{connection_name}'
        AND "run_time" = (
            SELECT MAX("run_time") 
            FROM code_schema.run_metadata 
            WHERE "config_name" = '{connection_name}' AND "load_status" = true
        ) 
        AND HASH('{new_query}') = HASH("query")
    """
    results = session.sql(metadata_query).collect()
    for row in results:
        return row

def insert_run_configs(connection_name, data):
    metadata_query = f"""
        INSERT INTO code_schema.run_meta
        (SELECT '{connection_name}', '{data["run_time"]}', '{data["status"]}')
    """
    session.sql(metadata_query).collect()

def get_data_api(session, child_sp, sql_code):
    connection_name = extract_connection_name(sql_code)
    if not connection_name:
        return 'SOURCE NOT FOUND'
    
    run_configs = get_run_configs(session, connection_name, sql_code)
    configs = get_configs(session, connection_name)
    
    query_params = get_params(sql_code)
    query_params_str = json.dumps(query_params)

    async_coef = 20
    url = configs['base_url'] + configs['path']
    headers = json.loads(configs['headers'])
    headers = {'Content-Type': 'application/json'}
    pag_type = configs['pag_type']
    all_count_field = configs['all_count_field']
    cursor_field = configs.get('cursor_field')  # Field for the cursor token
    root = configs['data_root']
    table = configs['table_name']
    
    if pag_type == 'page': 
        data = {
            configs['per_page_count_field']: 1,
            configs['page_num_field']: 1
        }
        response = requests.post(url, headers=headers, data=json.dumps({**data, **query_params}))
        response_data = response.json()
        pages_count = response_data[root][all_count_field]
         
        pages_count = 5
        ppsp = int(pages_count / min(pages_count, async_coef))
        query_list = []
        cur_page = 1
        iter = 0
        while cur_page <= pages_count:
            iter += 1
            sql = f"CALL {child_sp}('{url}', '{query_params_str}', {cur_page + ppsp}, {configs['per_page_count']}, '{configs['per_page_count_field']}', '{configs['page_num_field']}', {cur_page}, '{root}', '{table}')"
            queryid = session.sql(sql).collect_nowait().query_id
            async_job = session.create_async_job(queryid)
            query_list.append(queryid)
            if iter % async_coef == 0:
                while len(query_list) > 0:
                    for qry_id in query_list:
                        if async_job.is_done():
                            query_list.remove(qry_id)
                    time.sleep(2)
            cur_page += ppsp
        while len(query_list) > 0:
            for qry_id in query_list:
                if async_job.is_done():
                    query_list.remove(qry_id)
            time.sleep(2)
    
    elif pag_type == 'cursor':
        next_cursor = None
        query_list = []
        iter = 0
        
        while True:
            data = {**query_params}
            if next_cursor:
                data[cursor_field] = next_cursor
            
            response = requests.post(url, headers=headers, data=json.dumps(data))
            response_data = response.json()
            records = response_data[root]
            next_cursor = response_data.get('next_cursor')  # Update the cursor
            
            if not records:
                break
            
            iter += 1
            sql = f"CALL {child_sp}('{url}', '{query_params_str}', '{next_cursor}', '{root}', '{table}')"
            queryid = session.sql(sql).collect_nowait().query_id
            async_job = session.create_async_job(queryid)
            query_list.append(queryid)
            
            if iter % async_coef == 0:
                while len(query_list) > 0:
                    for qry_id in query_list:
                        if async_job.is_done():
                            query_list.remove(qry_id)
                    time.sleep(2)
            
            if not next_cursor:
                break
        
        while len(query_list) > 0:
            for qry_id in query_list:
                if async_job.is_done():
                    query_list.remove(qry_id)
            time.sleep(2)
    else:
        return 'Pagination Type Not Found'

    # Insert run configurations
    # insert_run_configs(connection_name, data)
    
    return 'Done'
