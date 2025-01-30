use role sysadmin;

create or alter database edw;

use edw;

create or alter schema sales_bronze;
create or alter schema sales_silver;
create or alter schema sales_gold;

use edw.sales_bronze;

create or alter stage sales_stage_adls
    url = 'azure://dev01debidwcentraladls2.blob.core.windows.net/edw-snowflake'
    credentials = (
        azure_sas_token = 'sp=racwdlm&st=2025-01-30T18:24:25Z&se=2025-03-31T01:24:25Z&spr=https&sv=2022-11-02&sr=c&sig=cWcSDGIFGHDFHKriFiWhCnDWswr%2BBmVQ66BO4WP9Uw4%3D'
    );

--list @sales_bronze.sales_stage_adls;

create or replace file format csv_no_header
    type = 'csv'
    field_delimiter = ';'
    skip_header = 0
    null_if = ('null');
