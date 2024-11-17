USE DATABASE capybara;

CREATE OR REPLACE PROCEDURE create_external_access_integration_with_network_rule(
    integration_name STRING,                       -- Name of the external access integration
    allowed_network_rule STRING,                   -- Name of the allowed network rule to associate
    enabled BOOLEAN DEFAULT TRUE,                  -- Whether the integration is enabled
    comment STRING DEFAULT NULL                    -- Optional comment
)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    // Access parameters via the 'arguments' array
    let integration_name = arguments[0];
    let allowed_network_rule = arguments[1];
    let enabled = arguments[2];
    let comment = arguments[3];

    // Start building the DDL statement for EXTERNAL ACCESS INTEGRATION
    let ddl = `CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION ${integration_name}`;

    // Add the ALLOWED_NETWORK_RULES parameter
    if (allowed_network_rule) {
        ddl += ` ALLOWED_NETWORK_RULES = (${allowed_network_rule})`;
    }

    // Set the ENABLED option
    ddl += ` ENABLED = ${enabled ? 'TRUE' : 'FALSE'}`;

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
CALL create_external_access_integration_with_network_rule(
    'typicode_maps_access_integration_v1',    -- integration_name
    'typicode_maps_network_rule',          -- allowed_network_rule
    TRUE,                                  -- enabled
    'This is an access integration with a network rule' -- comment (optional)
);
