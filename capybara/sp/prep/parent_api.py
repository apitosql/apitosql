import _snowflake
import requests
import json
import pandas
import re
from datetime import datetime
import time 

ses = requests.Session()
baseurl='https://bigquery.googleapis.com/bigquery/v2/projects/'
sk_coef = 50

def extract_connection_name(sql_query):
    match = re.search(r"\bFROM\b\s+([\w\.]+)", sql_query, re.IGNORECASE)  
    if match:
        return match.group(1).strip()
    return None
    
def get_params(sql_code):
    import re
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
    new_query = new_query.replace("'","")
    metadata_query = f"""
        SELECT *
        FROM code_schema.run_metadata
        WHERE "config_name" = '{connection_name}'
        AND "run_time" = (select max("run_time") from code_schema.run_metadata WHERE "config_name" = '{connection_name}' and "load_status" = true ) 
        AND HASH('{new_query}') = HASH("query")
    """
    results = session.sql(metadata_query ).collect()
    for row in results:
        return row
        
def insert_run_configs(connection_name, data):
    f"""
        INSERT INTO code_schema.run_meta  
        (SELECT )
    """
    session.sql(metadata_query).collect()
    
        
def get_data_api(session, child_sp , sql_code):
    connection_name = extract_connection_name(sql_code)
    if not connection_name:
        return 'SOURCE NOT FOUND'
    run_configs = get_run_configs(session, connection_name, sql_code)
    configs = get_configs(session, connection_name)
    #if run_configs and -run_configs["run_time"] + datetime.now < configs["refresh_time"]:
        #return 'DONE'
        
    query_params = get_params(sql_code)
    query_params_str = json.dumps(query_params)

    async_coef = 20
    url = configs['base_url'] + configs['path']
    headers = json.loads(configs['headers'])
    headers = {'Content-Type': 'application/json'}
    pag_type = configs['pag_type']
    #max_result_cnt = configs['max_result_cnt']
    all_count_field = configs['all_count_field']
    ppc_field = configs['per_page_count_field']  
    ppc = configs['per_page_count']
    pn_field = configs['page_num_field']  
    root = configs['data_root']  
    meta_root = configs['meta_root']  
    table = configs['table_name'] 
    
    # decide what to do based on pag type
    if pag_type == 'page': 
        data = {
            f"{ppc_field}": 1,
            f"{pn_field}": 1
        }
        response = requests.post(url, headers=headers, data=json.dumps({**data, **query_params}))
        response_data = response.json()
        pages_count = response_data[meta_root][all_count_field]
         
        pages_count = 5
        ppsp = int(pages_count/ min(pages_count , async_coef ))
        query_list = []
        cur_page = 1
        iter = 0
        while cur_page <= pages_count:
            iter = iter+1
            sql = f"CALL {child_sp}('{url}', '{query_params_str}', {cur_page+ ppsp}, {ppc}, '{ppc_field}', '{pn_field}', {cur_page}, '{root}', '{table}')"
            queryid = session.sql(sql).collect_nowait().query_id
            async_job = session.create_async_job(queryid)
            query_list.append(queryid)
            if iter%async_coef == 0:
                while len(query_list) > 0:
                    for qry_id in query_list:
                        if async_job.is_done():
                            query_list.remove(qry_id)  
                    time.sleep(2)
            cur_page = cur_page + ppsp
        while len(query_list) > 0:
            for qry_id in query_list:
                if async_job.is_done():
                    query_list.remove(qry_id)  
            time.sleep(2)
    else:
        return 'Pagination Type Not Found'

    # inseert configs 
    #insert_run_configs(data)
    
    return 'Done'