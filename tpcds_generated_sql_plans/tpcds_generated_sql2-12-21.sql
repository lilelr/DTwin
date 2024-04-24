-- 1.1 minutes 100 GB


WITH
tb2 AS (
SELECT Store_Sales.ss_quantity,Store_Sales.ss_wholesale_cost,Store_Sales.ss_list_price,Store_Sales.ss_sales_price,Store_Sales.ss_ext_wholesale_cost,Store_Sales.ss_ext_tax,Store_Sales.ss_net_paid,Store_Sales.ss_net_paid_inc_tax,Store_Sales.ss_customer_sk,Store_Sales.ss_sold_time_sk,Store_Sales.ss_sold_date_sk
FROM Store_Sales where ss_ext_discount_amt=13

) ,
 tb3 AS (
SELECT Customer.c_customer_sk,Customer.c_customer_id,Customer.c_current_cdemo_sk,Customer.c_current_hdemo_sk,Customer.c_current_addr_sk,Customer.c_first_shipto_date_sk,Customer.c_first_sales_date_sk,Customer.c_salutation,Customer.c_first_name,Customer.c_last_name,Customer.c_preferred_cust_flag,Customer.c_birth_day,Customer.c_birth_month,Customer.c_birth_year,Customer.c_birth_country,Customer.c_login,Customer.c_email_address
FROM Customer


) ,
tb1 AS (
SELECT Store_Sales.ss_sold_date_sk,Store_Sales.ss_sold_time_sk,Store_Sales.ss_item_sk,Store_Sales.ss_customer_sk,Store_Sales.ss_cdemo_sk,Store_Sales.ss_hdemo_sk,Store_Sales.ss_addr_sk,Store_Sales.ss_store_sk,Store_Sales.ss_promo_sk,Store_Sales.ss_ticket_number,Store_Sales.ss_quantity,Store_Sales.ss_wholesale_cost,Store_Sales.ss_list_price,Store_Sales.ss_sales_price,Store_Sales.ss_ext_discount_amt,Store_Sales.ss_ext_sales_price,Store_Sales.ss_ext_wholesale_cost,Store_Sales.ss_ext_list_price,Store_Sales.ss_ext_tax,Store_Sales.ss_coupon_amt,Store_Sales.ss_net_paid,Store_Sales.ss_net_paid_inc_tax,Store_Sales.ss_net_profit
FROM Store_Sales where ss_sales_price=13

) ,
 tb4 AS (
SELECT  tb2.ss_customer_sk, tb2.ss_sold_time_sk, tb2.ss_sold_date_sk, tb3.c_customer_id, tb3.c_salutation, tb3.c_last_name, SUM(ss_quantity) AS ss_quantity_SUM, SUM(ss_wholesale_cost) AS ss_wholesale_cost_SUM
FROM tb2 , tb3
WHERE tb2.ss_customer_sk = tb3.c_customer_sk
 GROUP BY tb2.ss_customer_sk,tb2.ss_sold_time_sk,tb2.ss_sold_date_sk,tb3.c_customer_id,tb3.c_salutation,tb3.c_last_name) ,
tb5 AS (
SELECT tb1.ss_sold_date_sk,tb1.ss_sold_time_sk,tb1.ss_item_sk,tb1.ss_customer_sk,tb1.ss_cdemo_sk,tb1.ss_hdemo_sk,tb1.ss_addr_sk,tb1.ss_store_sk,tb1.ss_promo_sk,tb1.ss_ticket_number,tb1.ss_quantity,tb1.ss_wholesale_cost,tb1.ss_list_price,tb1.ss_sales_price,tb1.ss_ext_discount_amt,tb1.ss_ext_sales_price,tb1.ss_ext_wholesale_cost,tb1.ss_ext_list_price,tb1.ss_ext_tax,tb1.ss_coupon_amt,tb1.ss_net_paid,tb1.ss_net_paid_inc_tax,tb1.ss_net_profit , tb4.ss_quantity_SUM
--      ,tb4.ss_wholesale_cost_SUM,tb4.ss_customer_sk,tb4.ss_sold_time_sk,tb4.ss_sold_date_sk
FROM tb1 , tb4
WHERE tb1.ss_sold_date_sk = tb4.ss_sold_date_sk
 ) ,
 tb6 AS (
SELECT Date_Dim.d_date_sk,Date_Dim.d_date_id,Date_Dim.d_date,Date_Dim.d_month_seq,Date_Dim.d_week_seq,Date_Dim.d_quarter_seq,Date_Dim.d_year,Date_Dim.d_dow,Date_Dim.d_moy,Date_Dim.d_dom,Date_Dim.d_qoy,Date_Dim.d_fy_year,Date_Dim.d_fy_quarter_seq,Date_Dim.d_fy_week_seq,Date_Dim.d_day_name ,Date_Dim.d_quarter_name ,Date_Dim.d_holiday ,Date_Dim.d_weekend,Date_Dim.d_following_holiday,Date_Dim.d_first_dom,Date_Dim.d_last_dom,Date_Dim.d_same_day_ly,Date_Dim.d_same_day_lq,Date_Dim.d_current_day,Date_Dim.d_current_week,Date_Dim.d_current_month ,Date_Dim.d_current_quarter,Date_Dim.d_current_year
FROM Date_Dim


) ,
tb8 AS (
SELECT Store_Sales.ss_quantity,Store_Sales.ss_wholesale_cost,Store_Sales.ss_sales_price,Store_Sales.ss_ext_discount_amt,Store_Sales.ss_ext_list_price,Store_Sales.ss_coupon_amt,Store_Sales.ss_net_paid,Store_Sales.ss_addr_sk,Store_Sales.ss_sold_time_sk,Store_Sales.ss_sold_date_sk
FROM Store_Sales where ss_list_price=10

) ,
 tb9 AS (
SELECT Customer_Address.ca_address_sk,Customer_Address.ca_address_id,Customer_Address.ca_street_number,Customer_Address.ca_street_name,Customer_Address.ca_street_type,Customer_Address.ca_suite_number,Customer_Address.ca_city,Customer_Address.ca_county,Customer_Address.ca_state,Customer_Address.ca_zip,Customer_Address.ca_country,Customer_Address.ca_gmt_offset,Customer_Address.ca_location_type
FROM Customer_Address where ca_gmt_offset=-5

) ,
tb7 AS (
SELECT tb5.ss_sold_date_sk,tb5.ss_sold_time_sk,tb5.ss_item_sk,tb5.ss_customer_sk,tb5.ss_cdemo_sk,tb5.ss_hdemo_sk,tb5.ss_addr_sk,tb5.ss_store_sk,tb5.ss_promo_sk,tb5.ss_ticket_number,tb5.ss_quantity,tb5.ss_wholesale_cost,tb5.ss_list_price,tb5.ss_sales_price,tb5.ss_ext_discount_amt,tb5.ss_ext_sales_price,tb5.ss_ext_wholesale_cost,tb5.ss_ext_list_price,tb5.ss_ext_tax,tb5.ss_coupon_amt,tb5.ss_net_paid,tb5.ss_net_paid_inc_tax,tb5.ss_net_profit , tb6.d_date_sk,tb6.d_date_id,tb6.d_date,tb6.d_month_seq,tb6.d_week_seq,tb6.d_quarter_seq,tb6.d_year,tb6.d_dow,tb6.d_moy,tb6.d_dom,tb6.d_qoy,tb6.d_fy_year,tb6.d_fy_quarter_seq,tb6.d_fy_week_seq,tb6.d_day_name ,tb6.d_quarter_name ,tb6.d_holiday ,tb6.d_weekend,tb6.d_following_holiday,tb6.d_first_dom,tb6.d_last_dom,tb6.d_same_day_ly,tb6.d_same_day_lq,tb6.d_current_day,tb6.d_current_week,tb6.d_current_month ,tb6.d_current_quarter,tb6.d_current_year
FROM tb5 , tb6
WHERE tb5.ss_sold_date_sk = tb6.d_date_sk
 ) ,
 tb10 AS (
SELECT  tb8.ss_addr_sk, tb8.ss_sold_time_sk, tb8.ss_sold_date_sk, tb9.ca_address_id, tb9.ca_street_name, SUM(ss_quantity) AS ss_quantity_SUM, COUNT(ss_wholesale_cost) AS ss_wholesale_cost_COUNT
FROM tb8 , tb9
WHERE tb8.ss_addr_sk = tb9.ca_address_sk
 GROUP BY tb8.ss_addr_sk,tb8.ss_sold_time_sk,tb8.ss_sold_date_sk,tb9.ca_address_id,tb9.ca_street_name)
SELECT tb7.ss_sold_date_sk,tb7.ss_sold_time_sk,tb7.ss_item_sk,tb7.ss_customer_sk,tb7.ss_cdemo_sk,tb7.ss_hdemo_sk,tb7.ss_addr_sk,tb7.ss_store_sk,tb7.ss_promo_sk,tb7.ss_ticket_number,tb7.ss_quantity,tb7.ss_wholesale_cost,tb7.ss_list_price,tb7.ss_sales_price,tb7.ss_ext_discount_amt,tb7.ss_ext_sales_price,tb7.ss_ext_wholesale_cost,tb7.ss_ext_list_price,tb7.ss_ext_tax,tb7.ss_coupon_amt,tb7.ss_net_paid,tb7.ss_net_paid_inc_tax,tb7.ss_net_profit , tb10.ss_quantity_SUM,tb10.ss_wholesale_cost_COUNT,tb10.ss_addr_sk,tb10.ss_sold_time_sk,tb10.ss_sold_date_sk
FROM tb7 , tb10
WHERE tb7.ss_sold_date_sk = tb10.ss_sold_date_sk

 LIMIT 100;