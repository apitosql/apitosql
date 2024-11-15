CREATE OR REPLACE PROCEDURE create_external_access_integration(
    integration_name STRING,
    type STRING DEFAULT 'EXTERNAL',
    external_oauth_any_role_mode STRING DEFAULT NULL,
    allowed_oauth_uris ARRAY DEFAULT NULL,
    allowed_vpc_ids ARRAY DEFAULT NULL,
    allowed_account_ids ARRAY DEFAULT NULL,
    allowed_azure_storage_uris ARRAY DEFAULT NULL,
    enabled BOOLEAN DEFAULT TRUE,
    comment STRING DEFAULT NULL
)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    // Start building the DDL statement
    let ddl = `CREATE EXTERNAL ACCESS INTEGRATION ${integration_name} TYPE = ${type}`;
    
    if (external_oauth_any_role_mode) {
        ddl += ` EXTERNAL_OAUTH_ANY_ROLE_MODE = '${external_oauth_any_role_mode}'`;
    }
    
    if (allowed_oauth_uris && allowed_oauth_uris.length > 0) {
        ddl += ` ALLOWED_OAUTH_URIS = (${allowed_oauth_uris.map(uri => `'${uri}'`).join(", ")})`;
    }
    
    if (allowed_vpc_ids && allowed_vpc_ids.length > 0) {
        ddl += ` ALLOWED_VPC_IDS = (${allowed_vpc_ids.map(vpc_id => `'${vpc_id}'`).join(", ")})`;
    }
    
    if (allowed_account_ids && allowed_account_ids.length > 0) {
        ddl += ` ALLOWED_ACCOUNT_IDS = (${allowed_account_ids.map(account_id => `'${account_id}'`).join(", ")})`;
    }
    
    if (allowed_azure_storage_uris && allowed_azure_storage_uris.length > 0) {
        ddl += ` ALLOWED_AZURE_STORAGE_URIS = (${allowed_azure_storage_uris.map(uri => `'${uri}'`).join(", ")})`;
    }
    
    ddl += ` ENABLED = ${enabled ? 'TRUE' : 'FALSE'}`;
    
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
CALL create_external_access_integration(
    'MY_EXTERNAL_ACCESS_INTEGRATION',
    'HOST_PORT',
    NULL,
    ARRAY_CONSTRUCT('jsonplaceholder.typicode.com')
);
