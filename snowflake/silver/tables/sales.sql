use role sysadmin;
use edw.sales_silver;

create or replace TABLE EDW.SALES_SILVER.SALES (
 SALES_ORDER_NUMBER VARCHAR(16777216),
 SALES_ORDER_LINENUMBER NUMBER(1,0),
 ORDER_DATE DATE,
 CUSTOMER_NAME VARCHAR(16777216),
 EMAIL VARCHAR(16777216),
 ITEM VARCHAR(16777216),
 QUANTITY NUMBER(2,0),
 UNITPRICE NUMBER(8,4),
 TAX NUMBER(7,4),
 FILE_ROW_NUMBER NUMBER(10,0),
 FILE_CONTENT_KEY VARCHAR(16777216),
 FILE_NAME VARCHAR(16777216),
 CREATED_TS TIMESTAMP_NTZ(9),
 MODIFIED_TS TIMESTAMP_NTZ(9),
 STG_MODIFIED_TS TIMESTAMP_NTZ(9)
);