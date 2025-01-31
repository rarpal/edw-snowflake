use role sysadmin;
use edw.sales_silver;

CREATE OR REPLACE PROCEDURE EDW.SALES_SILVER.SP_SALES_BRONZE_SILVER()
RETURNS TABLE ()
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS
$$
# SP used to populate SALES_BRONZE.SALES table  

import snowflake.snowpark as snowpark
import datetime
from snowflake.snowpark.functions import col
from snowflake.snowpark import functions as f
from snowflake.snowpark.functions import when_matched, when_not_matched

cts= snowpark.functions.current_timestamp()

def main(session: snowpark.Session): 
   
    SRC_SILVER_BRONZE = ''EDW.SALES_BRONZE.VW_SALES_BRONZE_SILVER''
    source= session.table(SRC_SILVER_BRONZE)   
    source=source.withColumn(''IS_FLAGGED'', f.when(col(''ORDER_DATE'') < ''2019-08-01'',''True'').otherwise(''False''))
    source=source.withColumn(''CREATED_TS'', cts)
    source=source.withColumn(''MODIFIED_TS'',cts)
    target= session.table(''EDW.SALES_SILVER.SALES'')
    source=source.dropDuplicates("SALES_ORDER_NUMBER","ORDER_DATE","EMAIL","ITEM")
    target.merge(source,(target["SALES_ORDER_NUMBER"] == source["SALES_ORDER_NUMBER"]) 
                           & (target["ORDER_DATE"] == source["ORDER_DATE"])  
                           & (target["EMAIL"] == source["EMAIL"])  
                           & (target["ITEM"] == source["ITEM"]),
        [when_matched().update({"SALES_ORDER_LINENUMBER": source["SALES_ORDER_LINENUMBER"],
                                "EMAIL": source["EMAIL"], 
                                "QUANTITY": source["QUANTITY"], 
                                "UNITPRICE": source["QUANTITY"], 
                                "TAX": source["TAX"], 
                                "MODIFIED_TS": source["MODIFIED_TS"] 
                                 }), 
        when_not_matched().insert({ "SALES_ORDER_NUMBER": source["SALES_ORDER_NUMBER"],
                                     "SALES_ORDER_LINENUMBER": source["SALES_ORDER_LINENUMBER"],
                                     "ORDER_DATE": source["ORDER_DATE"],
                                     "CUSTOMER_NAME": source["CUSTOMER_NAME"],
                                     "EMAIL": source["EMAIL"],
                                     "ITEM": source["ITEM"],
                                     "QUANTITY": source["QUANTITY"],
                                     "UNITPRICE": source["UNITPRICE"],
                                     "TAX": source["TAX"],
                                     "FILE_ROW_NUMBER": source["FILE_ROW_NUMBER"],
                                     "FILE_CONTENT_KEY": source["FILE_CONTENT_KEY"],
                                     "FILE_NAME": source["FILE_NAME"],
                                     "CREATED_TS": source["CREATED_TS"],
                                     "MODIFIED_TS": source["MODIFIED_TS"],
                                     "STG_MODIFIED_TS": source["STG_MODIFIED_TS"]
                                   })])
    print(target.collect())
    return source
    $$;
