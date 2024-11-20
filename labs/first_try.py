create database capybara
create schema code_schema


CREATE OR REPLACE NETWORK RULE sport_as
 MODE = EGRESS
 TYPE = HOST_PORT
 VALUE_LIST = ('odds.p.rapidapi.com');

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION sport_int
 ALLOWED_NETWORK_RULES = (sport_as)
 ENABLED = true;

Create or replace PROCEDURE my_proc(input_table string)
RETURNS string
language python
runtime_version=3.8
packages=('pandas','snowflake-snowpark-python')
EXTERNAL_ACCESS_INTEGRATIONS = (sport_int)

HANDLER = 'run'
AS
$$
imp
