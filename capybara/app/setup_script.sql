
CREATE APPLICATION ROLE IF NOT EXISTS app_public;
CREATE SCHEMA IF NOT EXISTS core;
GRANT USAGE ON SCHEMA core TO APPLICATION ROLE app_public;


CREATE OR ALTER VERSIONED SCHEMA code_schema;
GRANT USAGE ON SCHEMA code_schema TO APPLICATION ROLE app_public;

CREATE OR REPLACE PROCEDURE code_schema.init_app(config variant)
  RETURNS string
  LANGUAGE python
  runtime_version = '3.8'
  packages = ('snowflake-snowpark-python', 'requests', 'simplejson')
  imports = ('/src/script.py')
  handler = 'script.init_app';

GRANT USAGE ON PROCEDURE code_schema.init_app(variant) TO APPLICATION ROLE app_public;

CREATE OR REPLACE FUNCTION code_schema.api_to_sql()
  RETURNS string
  LANGUAGE python
  runtime_version = '3.8'
  packages = ('snowflake-snowpark-python', 'requests', 'simplejson')
  imports = ('/src/script.py')
  handler = 'script.api_to_sql';

GRANT USAGE ON FUNCTION code_schema.api_to_sql() TO APPLICATION ROLE app_public;