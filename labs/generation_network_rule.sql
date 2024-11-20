USE DATABASE capybara;

CREATE OR REPLACE PROCEDURE create_network_rule(
    rule_name STRING,                     -- Name of the network rule
    mode STRING ,         -- Network rule mode (e.g., 'EGRESS')
    type STRING,      -- Type of network rule (e.g., 'HOST_PORT')
    value_list STRING,                    -- List of values (e.g., hostnames, IPs, etc.)
    comment STRING DEFAULT NULL           -- Optional comment
)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    // Access parameters via the 'arguments' array
    let rule_name = arguments[0];
    let mode = arguments[1];
    let type = arguments[2];
    let value_list = arguments[3];
    let comment = arguments[4];

    // Verify the mode and type to avoid invalid values
    let ruleMode = (mode === 'EGRESS' || mode === 'INGRESS') ? mode : 'EGRESS';
    let ruleType = (type === 'HOST_PORT' || type === 'ANOTHER_VALID_TYPE') ? type : 'HOST_PORT';

    // Start building the DDL statement for NETWORK RULE
    let ddl = `CREATE NETWORK RULE ${rule_name} MODE = ${ruleMode} TYPE = ${ruleType}`;

    // Add the value list (ensure it's a comma-separated list of values)
    if (value_list) {
        ddl += ` VALUE_LIST = (${value_list.split(',').map(value => `'${value.trim()}'`).join(", ")})`;
    }

    // Add comment if provided
    if (comment) {
        ddl += ` COMMENT = '${comment}'`;
    }

    ddl += ";";

    // Execute the generated DDL statement
    snowflake.execute({ sqlText: ddl });

    // Return the executed DDL for reference
    return ddl;
$$;

-- Usage
CALL create_network_rule(
    'typicode_maps_network_rule_1',         -- rule_name
    'EGRESS',                              -- mode (e.g., 'EGRESS')
    'HOST_PORT',                           -- type (e.g., 'HOST_PORT')
    'jsonplaceholder.typicode.com',        -- value_list (comma-separated if multiple)
    'This is a test network rule'          -- comment (optional)
);
