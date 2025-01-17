import _snowflake
import requests
import json
import pandas
ses = requests.Session()
#key = _snowflake.get_generic_secret_string('apikey')
headers = {'Content-Type': 'application/json'}
def load_data_with_offset(session, url, query_params_str, load_untill, ppc, ppc_field, pn_field, init_pg, root, tab):
    query_params =  json.loads(query_params_str)
    all_data = []
    cnt = []
    cur_pg = init_pg
    while cur_pg <= load_untill:
        data = {
            f"{ppc_field}": ppc,
            f"{pn_field}": cur_pg
        }
        response = requests.post(url, headers=headers, data=json.dumps({**data, **query_params}))
        if(response.status_code != 200):
            return str(response.reason)
        response_data = response.json()
        all_data.extend(response_data[root])  
        cnt.append(len(response_data[root]))
        cur_pg += 1
    df = pandas.DataFrame.from_records(all_data)
    snowpark_df = session.create_dataframe(data=df)
    snowpark_df.write.mode("append").save_as_table(tab)   
    return 'done'
