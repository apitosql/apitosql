-- This is the setup script that runs while installing a Snowflake Native App in a consumer account.
-- To write this script, you can familiarize yourself with some of the following concepts:
-- Application Roles
-- Versioned Schemas
-- UDFs/Procs
-- Extension Code
-- Refer to https://docs.snowflake.com/en/developer-guide/native-apps/creating-setup-script for a detailed understanding of this file.

CREATE OR ALTER VERSIONED SCHEMA core;

-- The rest of this script is left blank for purposes of your learning and exploration.


CREATE APPLICATION ROLE IF NOT EXISTS app_public;
CREATE SCHEMA IF NOT EXISTS core;
GRANT USAGE ON SCHEMA core TO APPLICATION ROLE app_public;


create or replace TABLE core.CONN_METADATA (
	"config_id" VARCHAR(16777216),
	"config_name" VARCHAR(16777216),
	"date_created" TIMESTAMP_NTZ(9),
	"headers" VARCHAR(16777216),
	"base_url" VARCHAR(16777216),
	"auth_type" VARCHAR(16777216),
	"path" VARCHAR(16777216),
	"data_root" VARCHAR(16777216),
	"meta_root" VARCHAR(16777216),
	"table_name" VARCHAR(16777216),
	"pag_type" VARCHAR(16777216),
	"per_page_count" NUMBER(38,0),
	"all_count_field" VARCHAR(16777216),
	"per_page_count_field" VARCHAR(16777216),
	"page_num_field" VARCHAR(16777216)
);

-- GRANT USAGE ON TABLE core.CONN_METADATA TO APPLICATION ROLE app_public;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE core.CONN_METADATA TO APPLICATION ROLE app_public;


create or replace TABLE core.API_RUN_META (
	CONFIG_ID VARCHAR(36) NOT NULL DEFAULT UUID_STRING(),
	CONFIG_NAME VARCHAR(100),
	RUN_TIME TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
	SQL_HASH VARCHAR(16777216),
	IS_LOADED BOOLEAN
);

-- GRANT USAGE ON TABLE core.API_RUN_META TO APPLICATION ROLE app_public;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE core.API_RUN_META TO APPLICATION ROLE app_public;


CREATE OR REPLACE PROCEDURE core.create_source(config_name string, base_url string, secret_token string, api_params_str string, headers_str string)
RETURNS string
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8' -- Or your desired runtime version
HANDLER = 'create_source.main' -- The entry point function in your Python code
PACKAGES = ('snowflake-snowpark-python', 'pandas')
IMPORTS = ('/setup/create_source.py');

-- GRANT USAGE ON PROCEDURE core.get_general create_source(config_name string, base_url string, secret_token string, api_params_str string, headers_str string) TO APPLICATION ROLE app_public;
GRANT USAGE ON PROCEDURE core.create_source(STRING, STRING, STRING, STRING, STRING) TO APPLICATION ROLE app_public;


CREATE OR REPLACE PROCEDURE core.get_general ( sql_code string)
RETURNS string
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8' -- Or your desired runtime version
HANDLER = 'get_general.main' -- The entry point function in your Python code
PACKAGES = ('snowflake-snowpark-python', 'pandas')
IMPORTS = ('/setup/get_general.py');


-- GRANT USAGE ON PROCEDURE core.get_general ( sql_code string) TO APPLICATION ROLE app_public;
GRANT USAGE ON PROCEDURE core.get_general(STRING) TO APPLICATION ROLE app_public;
