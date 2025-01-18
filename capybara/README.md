# Hi there 👋

![Architecture Diagram](https://github.com/apitosql/apitosql/blob/main/imgs/s_cap_1.drawio%20(1).png)

# ApiToSQL 🔄

Transform external API data into SQL tables seamlessly with Snowflake integration.

## Project Overview 🎯

ApiToSQL is created for secure and efficient API-to-SQL pipelines using Snowflake's external access features. It simplifies the process of fetching data from external APIs and transforming it into structured SQL tables in Snowflake for end users such as Data Analysts, Scientists, Engineers as etc.

## Architecture 🏗️

The project implements a three-layer architecture:
1. **Security Layer**: Network rules and external access integrations
2. **Processing Layer**: Python UDTFs and stored procedures
3. **Data Layer**: Structured SQL tables and schemas

## Project Structure 📁

Breakdown of early stage solution:
- `first_try.py` - setup/database creation
- `generation_ext_access_integration.sql` - xxternal access integration management (APIs)
- `generation_network_rule.sql` - network rule generation
- `python_UDTF.sql` - udfs (python)
- `setup_script.sql` - Snowflake Native App setup (as I understood it's template, Shahin?)

## Getting Started 🚀

### Prerequisites

- Snowflake account with appropriate privileges
- Access to external APIs
- Python 3.8+ (is it mandatory to have 3.8+?)
- Required Python packages (initial):
  - requests
  - pandas (why pandas? why not polars or pyarrow?) polars should be faster for json parsing (specially large jsons).
  - snowflake-snowpark-python

### Basic Setup

1. Create the database and schema:
```sql
CREATE DATABASE capybara;
CREATE SCHEMA code_schema;
```

2. Set up network rules:
```sql
CALL create_network_rule(
    'typicode_maps_network_rule_1',     
    'EGRESS',                          
    'HOST_PORT',                       
    'jsonplaceholder.typicode.com',    
    'This is a test network rule'      
);
```

3. Configure external access integration:
```sql
CALL create_external_access_integration_with_network_rule(
    'typicode_maps_access_integration_v1',    
    'typicode_maps_network_rule',          
    TRUE,                                  
    'This is an access integration with a network rule'
);
```

4. Deploy the Python UDTF:
```sql
CREATE OR REPLACE FUNCTION api_json_to_flatten()
    RETURNS TABLE (id STRING, title STRING, completed BOOLEAN)
    LANGUAGE PYTHON
    RUNTIME_VERSION = 3.8
    PACKAGES=('requests')
    EXTERNAL_ACCESS_INTEGRATIONS = (typicode_maps_access_integration)
    HANDLER = 'handler';
```

## Usage Example 💡

Query transformed data:
```sql
SELECT * FROM TABLE(CAPYBARA.PUBLIC.api_json_to_flatten());
```

## License 📝

...


## Deployment Guide 🚀

### Prerequisites
- Snowflake account
- Snowflake CLI installed

### 1. Install Snowflake CLI
```bash
# For macOS (using Homebrew)
brew install --cask snowflake-cli
```

### 2. Configure Snowflake Connection
```bash
mkdir -p ~/.snowflake
touch ~/.snowflake/config.toml
```

Add Connection Details to config.toml

Edit the config.toml file with the following Snowflake connection details:

account = "ZQ72407"

host = "jrzozaq-capybara_dev.snowflakecomputing.com"

user = "CAPYBARA"

password = "lickurass"

organization = "JRZOZAQ"

role = "ACCOUNTADMIN"

warehouse = "CAPYBARA"

database = "CAPYBARA"


### 3. Test the Connection
Once the configuration is complete, test the Snowflake connection:

```bash
snow connection test -c capybara
```


### 4. Run Snowflake Application
To run a Snowflake application with the configured connection:

```bash
snow app run -c capybara
```