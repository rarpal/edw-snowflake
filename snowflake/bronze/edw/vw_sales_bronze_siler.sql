use edw.sales_bronze;

create or replace view EDW.SALES_BRONZE.VW_SALES_BRONZE_SILVER(
 SALES_ORDER_NUMBER,
 SALES_ORDER_LINENUMBER,
 ORDER_DATE,
 CUSTOMER_NAME,
 EMAIL,
 ITEM,
 QUANTITY,
 UNITPRICE,
 TAX,
 FILE_ROW_NUMBER,
 FILE_CONTENT_KEY,
 FILE_NAME,
 STG_MODIFIED_TS
) as
select 
        $1::TEXT as Sales_Order_Number,
        $2::NUMBER(1, 0) as Sales_Order_LineNumber,
        $3::DATE as Order_Date,
        $4::TEXT as Customer_Name,
        $5::TEXT as Email,
        $6::TEXT as Item,
        $7::NUMBER(1, 0) as Quantity,
        $8::NUMBER(8, 4) as UnitPrice,
        $9::NUMBER(7, 4) as Tax,
        metadata$file_row_number file_row_number,
        metadata$file_content_key file_content_key,
        metadata$filename as file_name,
        metadata$file_last_modified stg_modified_ts
fROM  @EDW.SALES_BRONZE.SALES_STAGE (file_format => 'csv_no_header', pattern=>'[Sales]*.*csv')