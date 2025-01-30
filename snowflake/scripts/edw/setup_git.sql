use role accountadmin;
use edw.public;

-- create or replace secret git_rarpal_secret
--     type = password
--     username = ''
--     password = '';

create or replace api integration git_rarpal_integration
    api_provider = git_https_api
    api_allowed_prefixes = ('https://github.com/rarpal')
    allowed_authentication_secrets = (git_rarpal_secret)
    enabled = true;

create or replace git repository git_rarpal_edw_snowflake
    api_integration = git_rarpal_integration
    git_credentials = git_rarpal_secret
    origin = 'https://github.com/rarpal/edw-snowflake.git';

