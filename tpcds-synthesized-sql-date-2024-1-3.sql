
---new sql--2024-01-03 14:09:19-----
long_chain is ['FileScan FactTable', 'Filter', 'BroadcastHashJoin', 'Project', 'HashAggregate', 'Union', 'HashAggregate', 'TakeOrderedAndProject', '']
WITH 
tb2 AS (
SELECT Store_Returns.sr_return_amt_inc_tax,Store_Returns.sr_fee,Store_Returns.sr_return_ship_cost,Store_Returns.sr_store_credit,Store_Returns.sr_customer_sk,Store_Returns.sr_returned_date_sk,Store_Returns.sr_returned_date_sk
FROM Store_Returns where sr_return_quantity=300
 
) , 
 tb3 AS (
SELECT Customer.c_customer_sk,Customer.c_customer_id,Customer.c_current_cdemo_sk,Customer.c_current_hdemo_sk,Customer.c_current_addr_sk,Customer.c_first_shipto_date_sk,Customer.c_first_sales_date_sk,Customer.c_salutation,Customer.c_first_name,Customer.c_last_name,Customer.c_preferred_cust_flag,Customer.c_birth_day,Customer.c_birth_month,Customer.c_birth_year,Customer.c_birth_country,Customer.c_login,Customer.c_email_address
FROM Customer 
 
 
) , 
tb1 AS (
SELECT Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number,Store_Returns.sr_return_quantity,Store_Returns.sr_return_amt,Store_Returns.sr_return_tax,Store_Returns.sr_return_amt_inc_tax,Store_Returns.sr_fee,Store_Returns.sr_return_ship_cost,Store_Returns.sr_refunded_cash,Store_Returns.sr_reversed_charge,Store_Returns.sr_store_credit,Store_Returns.sr_net_loss
FROM Store_Returns where sr_return_amt=100
 
) , 
 tb4 AS (
SELECT  tb2.sr_customer_sk, tb2.sr_returned_date_sk, tb2.sr_returned_date_sk, tb3.c_salutation, COUNT(sr_return_amt_inc_tax) AS sr_return_amt_inc_tax_COUNT, SUM(sr_fee) AS sr_fee_SUM
FROM tb2 , tb3
WHERE tb2.sr_customer_sk = tb3.c_customer_sk
 GROUP BY tb2.sr_customer_sk,tb2.sr_returned_date_sk,tb2.sr_returned_date_sk,tb3.c_salutation) 
SELECT  tb1.sr_returned_date_sk, tb1.sr_return_time_sk, tb1.sr_item_sk, tb1.sr_customer_sk, tb1.sr_cdemo_sk, tb1.sr_hdemo_sk, tb1.sr_addr_sk, tb1.sr_store_sk, tb1.sr_reason_sk, tb1.sr_ticket_number, tb4.sr_fee_SUM, tb1.sr_returned_date_sk, tb1.sr_return_time_sk, tb1.sr_item_sk, tb1.sr_customer_sk, tb1.sr_cdemo_sk, tb1.sr_hdemo_sk, tb1.sr_addr_sk, tb1.sr_store_sk, tb1.sr_reason_sk, tb1.sr_ticket_number, tb4.sr_return_amt_inc_tax_COUNT, SUM(sr_return_quantity) AS sr_return_quantity_SUM, SUM(sr_return_quantity) AS sr_return_quantity_SUM, SUM(sr_return_amt) AS sr_return_amt_SUM
FROM tb1 , tb4
WHERE tb1.sr_returned_date_sk = tb4.sr_returned_date_sk
 GROUP BY tb1.sr_returned_date_sk,tb1.sr_return_time_sk,tb1.sr_item_sk,tb1.sr_customer_sk,tb1.sr_cdemo_sk,tb1.sr_hdemo_sk,tb1.sr_addr_sk,tb1.sr_store_sk,tb1.sr_reason_sk,tb1.sr_ticket_number,tb4.sr_fee_SUM,tb1.sr_returned_date_sk,tb1.sr_return_time_sk,tb1.sr_item_sk,tb1.sr_customer_sk,tb1.sr_cdemo_sk,tb1.sr_hdemo_sk,tb1.sr_addr_sk,tb1.sr_store_sk,tb1.sr_reason_sk,tb1.sr_ticket_number,tb4.sr_return_amt_inc_tax_COUNT
 LIMIT 100;
---new sql--2024-01-03 14:09:19-----
long_chain is ['FileScan FactTable', 'Filter', 'BroadcastHashJoin', 'Project', 'BroadcastHashJoin', 'Project', 'HashAggregate', 'TakeOrderedAndProject', '']
WITH 
tb8 AS (
SELECT Catalog_Sales.cs_ext_tax,Catalog_Sales.cs_coupon_amt,Catalog_Sales.cs_ext_ship_cost,Catalog_Sales.cs_net_paid,Catalog_Sales.cs_net_paid_inc_ship_tax,Catalog_Sales.cs_net_profit,Catalog_Sales.cs_bill_cdemo_sk,Catalog_Sales.cs_sold_date_sk,Catalog_Sales.cs_sold_time_sk,Catalog_Sales.cs_bill_customer_sk,Catalog_Sales.cs_item_sk
FROM Catalog_Sales where cs_quantity=100
 
) , 
 tb9 AS (
SELECT Customer_Demographics.cd_demo_sk,Customer_Demographics.cd_gender,Customer_Demographics.cd_marital_status,Customer_Demographics.cd_education_status,Customer_Demographics.cd_purchase_estimate,Customer_Demographics.cd_credit_rating,Customer_Demographics.cd_dep_count,Customer_Demographics.cd_dep_employed_count,Customer_Demographics.cd_dep_college_count
FROM Customer_Demographics where cd_marital_status='College'
 
) , 
tb10 AS (
SELECT  tb8.cs_bill_cdemo_sk, tb8.cs_sold_date_sk, tb8.cs_sold_time_sk, tb8.cs_bill_customer_sk, tb8.cs_item_sk, tb9.cd_gender, tb9.cd_education_status, tb9.cd_purchase_estimate, AVG(cs_ext_tax) AS cs_ext_tax_AVG, SUM(cs_coupon_amt) AS cs_coupon_amt_SUM
FROM tb8 , tb9
WHERE tb8.cs_bill_cdemo_sk = tb9.cd_demo_sk
 GROUP BY tb8.cs_bill_cdemo_sk,tb8.cs_sold_date_sk,tb8.cs_sold_time_sk,tb8.cs_bill_customer_sk,tb8.cs_item_sk,tb9.cd_gender,tb9.cd_education_status,tb9.cd_purchase_estimate) , 
 tb11 AS (
SELECT Date_Dim.d_date_sk,Date_Dim.d_date_id,Date_Dim.d_date,Date_Dim.d_month_seq,Date_Dim.d_week_seq,Date_Dim.d_quarter_seq,Date_Dim.d_year,Date_Dim.d_dow,Date_Dim.d_moy,Date_Dim.d_dom,Date_Dim.d_qoy,Date_Dim.d_fy_year,Date_Dim.d_fy_quarter_seq,Date_Dim.d_fy_week_seq,Date_Dim.d_day_name ,Date_Dim.d_quarter_name ,Date_Dim.d_holiday ,Date_Dim.d_weekend,Date_Dim.d_following_holiday,Date_Dim.d_first_dom,Date_Dim.d_last_dom,Date_Dim.d_same_day_ly,Date_Dim.d_same_day_lq,Date_Dim.d_current_day,Date_Dim.d_current_week,Date_Dim.d_current_month ,Date_Dim.d_current_quarter,Date_Dim.d_current_year
FROM Date_Dim where d_year=2002
 
) , 
tb7 AS (
SELECT Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number,Store_Returns.sr_return_quantity,Store_Returns.sr_return_amt,Store_Returns.sr_return_tax,Store_Returns.sr_return_amt_inc_tax,Store_Returns.sr_fee,Store_Returns.sr_return_ship_cost,Store_Returns.sr_refunded_cash,Store_Returns.sr_reversed_charge,Store_Returns.sr_store_credit,Store_Returns.sr_net_loss
FROM Store_Returns where sr_return_amt=300
 
) , 
 tb12 AS (
SELECT  tb10.cs_bill_cdemo_sk, tb10.cs_sold_date_sk, tb10.cs_sold_time_sk, tb10.cs_bill_customer_sk, tb10.cs_item_sk, tb11.d_date_id, SUM(cs_ext_tax_AVG) AS cs_ext_tax_AVG_SUM
FROM tb10 , tb11
WHERE tb10.cs_sold_date_sk = tb11.d_date_sk
 GROUP BY tb10.cs_bill_cdemo_sk,tb10.cs_sold_date_sk,tb10.cs_sold_time_sk,tb10.cs_bill_customer_sk,tb10.cs_item_sk,tb11.d_date_id) , 
tb13 AS (
SELECT tb7.sr_returned_date_sk,tb7.sr_return_time_sk,tb7.sr_item_sk,tb7.sr_customer_sk,tb7.sr_cdemo_sk,tb7.sr_hdemo_sk,tb7.sr_addr_sk,tb7.sr_store_sk,tb7.sr_reason_sk,tb7.sr_ticket_number,tb7.sr_return_quantity,tb7.sr_return_amt,tb7.sr_return_tax,tb7.sr_return_amt_inc_tax,tb7.sr_fee,tb7.sr_return_ship_cost,tb7.sr_refunded_cash,tb7.sr_reversed_charge,tb7.sr_store_credit,tb7.sr_net_loss , tb12.cs_ext_tax_AVG_SUM,tb12.cs_bill_cdemo_sk,tb12.cs_sold_date_sk,tb12.cs_sold_time_sk,tb12.cs_bill_customer_sk,tb12.cs_item_sk
FROM tb7 , tb12
WHERE tb7.sr_customer_sk = tb12.cs_bill_customer_sk
 ) , 
 tb14 AS (
SELECT Date_Dim.d_date_sk,Date_Dim.d_date_id,Date_Dim.d_date,Date_Dim.d_month_seq,Date_Dim.d_week_seq,Date_Dim.d_quarter_seq,Date_Dim.d_year,Date_Dim.d_dow,Date_Dim.d_moy,Date_Dim.d_dom,Date_Dim.d_qoy,Date_Dim.d_fy_year,Date_Dim.d_fy_quarter_seq,Date_Dim.d_fy_week_seq,Date_Dim.d_day_name ,Date_Dim.d_quarter_name ,Date_Dim.d_holiday ,Date_Dim.d_weekend,Date_Dim.d_following_holiday,Date_Dim.d_first_dom,Date_Dim.d_last_dom,Date_Dim.d_same_day_ly,Date_Dim.d_same_day_lq,Date_Dim.d_current_day,Date_Dim.d_current_week,Date_Dim.d_current_month ,Date_Dim.d_current_quarter,Date_Dim.d_current_year
FROM Date_Dim 
 
 
) 
SELECT  tb13.sr_returned_date_sk, tb13.sr_return_time_sk, tb13.sr_item_sk, tb13.sr_customer_sk, tb13.sr_cdemo_sk, tb13.sr_hdemo_sk, tb13.sr_addr_sk, tb13.sr_store_sk, tb13.sr_reason_sk, tb13.sr_ticket_number, tb14.d_date_id, tb14.d_week_seq, AVG(sr_return_quantity) AS sr_return_quantity_AVG
FROM tb13 , tb14
WHERE tb13.sr_returned_date_sk = tb14.d_date_sk
 GROUP BY tb13.sr_returned_date_sk,tb13.sr_return_time_sk,tb13.sr_item_sk,tb13.sr_customer_sk,tb13.sr_cdemo_sk,tb13.sr_hdemo_sk,tb13.sr_addr_sk,tb13.sr_store_sk,tb13.sr_reason_sk,tb13.sr_ticket_number,tb14.d_date_id,tb14.d_week_seq
 LIMIT 100;
---new sql--2024-01-03 14:09:19-----
long_chain is ['FileScan FactTable', 'Filter', 'BroadcastHashJoin', 'Project', 'BroadcastHashJoin', '']
WITH 
tb26 AS (
SELECT Catalog_Sales.cs_quantity,Catalog_Sales.cs_wholesale_cost,Catalog_Sales.cs_list_price,Catalog_Sales.cs_sales_price,Catalog_Sales.cs_ext_discount_amt,Catalog_Sales.cs_ext_wholesale_cost,Catalog_Sales.cs_ext_tax,Catalog_Sales.cs_coupon_amt,Catalog_Sales.cs_ext_ship_cost,Catalog_Sales.cs_net_paid,Catalog_Sales.cs_net_paid_inc_tax,Catalog_Sales.cs_net_paid_inc_ship,Catalog_Sales.cs_net_paid_inc_ship_tax,Catalog_Sales.cs_item_sk,Catalog_Sales.cs_bill_cdemo_sk,Catalog_Sales.cs_sold_date_sk,Catalog_Sales.cs_bill_customer_sk,Catalog_Sales.cs_item_sk
FROM Catalog_Sales where cs_net_profit=300
 
) , 
 tb27 AS (
SELECT Item.i_item_sk,Item.i_item_id,Item.i_rec_start_date,Item.i_rec_end_date,Item.i_item_desc,Item.i_current_price,Item.i_wholesale_cost,Item.i_brand_id,Item.i_brand,Item.i_class_id,Item.i_class,Item.i_category_id,Item.i_category,Item.i_manufact_id,Item.i_manufact,Item.i_size,Item.i_formulation,Item.i_color,Item.i_units,Item.i_container,Item.i_manager_id,Item.i_product_name
FROM Item where i_manager_id=8
 
) , 
tb28 AS (
SELECT  tb26.cs_item_sk, tb26.cs_bill_cdemo_sk, tb26.cs_sold_date_sk, tb26.cs_bill_customer_sk, tb26.cs_item_sk, tb27.i_item_id, tb27.i_rec_start_date, MAX(cs_quantity) AS cs_quantity_MAX, SUM(cs_wholesale_cost) AS cs_wholesale_cost_SUM, COUNT(cs_list_price) AS cs_list_price_COUNT
FROM tb26 , tb27
WHERE tb26.cs_item_sk = tb27.i_item_sk
 GROUP BY tb26.cs_item_sk,tb26.cs_bill_cdemo_sk,tb26.cs_sold_date_sk,tb26.cs_bill_customer_sk,tb26.cs_item_sk,tb27.i_item_id,tb27.i_rec_start_date) , 
 tb29 AS (
SELECT Customer_Demographics.cd_demo_sk,Customer_Demographics.cd_gender,Customer_Demographics.cd_marital_status,Customer_Demographics.cd_education_status,Customer_Demographics.cd_purchase_estimate,Customer_Demographics.cd_credit_rating,Customer_Demographics.cd_dep_count,Customer_Demographics.cd_dep_employed_count,Customer_Demographics.cd_dep_college_count
FROM Customer_Demographics where cd_education_status='College'
 
) , 
tb25 AS (
SELECT Store_Sales.ss_sold_date_sk,Store_Sales.ss_sold_time_sk,Store_Sales.ss_item_sk,Store_Sales.ss_customer_sk,Store_Sales.ss_cdemo_sk,Store_Sales.ss_hdemo_sk,Store_Sales.ss_addr_sk,Store_Sales.ss_store_sk,Store_Sales.ss_promo_sk,Store_Sales.ss_ticket_number,Store_Sales.ss_quantity,Store_Sales.ss_wholesale_cost,Store_Sales.ss_list_price,Store_Sales.ss_sales_price,Store_Sales.ss_ext_discount_amt,Store_Sales.ss_ext_sales_price,Store_Sales.ss_ext_wholesale_cost,Store_Sales.ss_ext_list_price,Store_Sales.ss_ext_tax,Store_Sales.ss_coupon_amt,Store_Sales.ss_net_paid,Store_Sales.ss_net_paid_inc_tax,Store_Sales.ss_net_profit
FROM Store_Sales where ss_ext_discount_amt=10
 
) , 
 tb30 AS (
SELECT  tb28.cs_item_sk, tb28.cs_bill_cdemo_sk, tb28.cs_sold_date_sk, tb28.cs_bill_customer_sk, tb28.cs_item_sk, tb29.cd_gender, tb29.cd_marital_status, tb29.cd_education_status, MAX(cs_quantity_MAX) AS cs_quantity_MAX_MAX, MAX(cs_wholesale_cost_SUM) AS cs_wholesale_cost_SUM_MAX
FROM tb28 , tb29
WHERE tb28.cs_bill_cdemo_sk = tb29.cd_demo_sk
 GROUP BY tb28.cs_item_sk,tb28.cs_bill_cdemo_sk,tb28.cs_sold_date_sk,tb28.cs_bill_customer_sk,tb28.cs_item_sk,tb29.cd_gender,tb29.cd_marital_status,tb29.cd_education_status) , 
tb32 AS (
SELECT Store_Sales.ss_quantity,Store_Sales.ss_sales_price,Store_Sales.ss_ext_sales_price,Store_Sales.ss_ext_list_price,Store_Sales.ss_net_paid_inc_tax,Store_Sales.ss_net_profit,Store_Sales.ss_addr_sk,Store_Sales.ss_store_sk,Store_Sales.ss_cdemo_sk,Store_Sales.ss_sold_date_sk
FROM Store_Sales where ss_sales_price=10
 
) , 
 tb33 AS (
SELECT Customer_Address.ca_address_sk,Customer_Address.ca_address_id,Customer_Address.ca_street_number,Customer_Address.ca_street_name,Customer_Address.ca_street_type,Customer_Address.ca_suite_number,Customer_Address.ca_city,Customer_Address.ca_county,Customer_Address.ca_state,Customer_Address.ca_zip,Customer_Address.ca_country,Customer_Address.ca_gmt_offset,Customer_Address.ca_location_type
FROM Customer_Address where ca_gmt_offset=-5
 
) , 
tb34 AS (
SELECT  tb32.ss_addr_sk, tb32.ss_store_sk, tb32.ss_cdemo_sk, tb32.ss_sold_date_sk, tb33.ca_street_type, tb33.ca_city, MAX(ss_quantity) AS ss_quantity_MAX, AVG(ss_sales_price) AS ss_sales_price_AVG
FROM tb32 , tb33
WHERE tb32.ss_addr_sk = tb33.ca_address_sk
 GROUP BY tb32.ss_addr_sk,tb32.ss_store_sk,tb32.ss_cdemo_sk,tb32.ss_sold_date_sk,tb33.ca_street_type,tb33.ca_city) , 
 tb35 AS (
SELECT Store.s_store_sk,Store.s_store_id,Store.s_rec_start_date,Store.s_rec_end_date,Store.s_closed_date_sk,Store.s_store_name,Store.s_number_employees,Store.s_floor_space,Store.s_hours,Store.S_manager,Store.S_market_id,Store.S_geography_class,Store.S_market_desc,Store.s_market_manager,Store.s_division_id,Store.s_division_name,Store.s_company_id,Store.s_company_name,Store.s_street_number,Store.s_street_name,Store.s_street_type,Store.s_suite_number,Store.s_city,Store.s_county,Store.s_state,Store.s_zip,Store.s_country,Store.s_gmt_offset,Store.s_tax_precentage
FROM Store 
 
 
) , 
tb31 AS (
SELECT tb25.ss_sold_date_sk,tb25.ss_sold_time_sk,tb25.ss_item_sk,tb25.ss_customer_sk,tb25.ss_cdemo_sk,tb25.ss_hdemo_sk,tb25.ss_addr_sk,tb25.ss_store_sk,tb25.ss_promo_sk,tb25.ss_ticket_number,tb25.ss_quantity,tb25.ss_wholesale_cost,tb25.ss_list_price,tb25.ss_sales_price,tb25.ss_ext_discount_amt,tb25.ss_ext_sales_price,tb25.ss_ext_wholesale_cost,tb25.ss_ext_list_price,tb25.ss_ext_tax,tb25.ss_coupon_amt,tb25.ss_net_paid,tb25.ss_net_paid_inc_tax,tb25.ss_net_profit , tb30.cs_quantity_MAX_MAX,tb30.cs_wholesale_cost_SUM_MAX,tb30.cs_item_sk,tb30.cs_bill_cdemo_sk,tb30.cs_sold_date_sk,tb30.cs_bill_customer_sk,tb30.cs_item_sk
FROM tb25 , tb30
WHERE tb25.ss_customer_sk = tb30.cs_bill_customer_sk
 ) , 
 tb36 AS (
SELECT  tb34.ss_addr_sk, tb34.ss_store_sk, tb34.ss_cdemo_sk, tb34.ss_sold_date_sk, tb35.s_rec_end_date, SUM(ss_quantity_MAX) AS ss_quantity_MAX_SUM, SUM(ss_sales_price_AVG) AS ss_sales_price_AVG_SUM
FROM tb34 , tb35
WHERE tb34.ss_store_sk = tb35.s_store_sk
 GROUP BY tb34.ss_addr_sk,tb34.ss_store_sk,tb34.ss_cdemo_sk,tb34.ss_sold_date_sk,tb35.s_rec_end_date) 
SELECT tb31.ss_sold_date_sk,tb31.ss_sold_time_sk,tb31.ss_item_sk,tb31.ss_customer_sk,tb31.ss_cdemo_sk,tb31.ss_hdemo_sk,tb31.ss_addr_sk,tb31.ss_store_sk,tb31.ss_promo_sk,tb31.ss_ticket_number,tb31.ss_quantity,tb31.ss_wholesale_cost,tb31.ss_list_price,tb31.ss_sales_price,tb31.ss_ext_discount_amt,tb31.ss_ext_sales_price,tb31.ss_ext_wholesale_cost,tb31.ss_ext_list_price,tb31.ss_ext_tax,tb31.ss_coupon_amt,tb31.ss_net_paid,tb31.ss_net_paid_inc_tax,tb31.ss_net_profit , tb36.ss_quantity_MAX_SUM,tb36.ss_sales_price_AVG_SUM
FROM tb31 , tb36
WHERE tb31.ss_sold_date_sk = tb36.ss_sold_date_sk
 
 LIMIT 100;
---new sql--2024-01-03 14:09:19-----
long_chain is ['FileScan FactTable', 'Filter', 'Project', 'HashAggregate', 'Filter', 'Project', 'BroadcastHashJoin', 'Project', 'HashAggregate', 'Union', 'Expand', 'HashAggregate', 'TakeOrderedAndProject', '']
WITH 
tb38 AS (
SELECT Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number,Store_Returns.sr_return_quantity,Store_Returns.sr_return_amt,Store_Returns.sr_return_tax,Store_Returns.sr_return_amt_inc_tax,Store_Returns.sr_fee,Store_Returns.sr_return_ship_cost,Store_Returns.sr_refunded_cash,Store_Returns.sr_reversed_charge,Store_Returns.sr_store_credit,Store_Returns.sr_net_loss
FROM Store_Returns where sr_return_quantity=200
 
) , 
 tb39 AS (
SELECT Date_Dim.d_date_sk,Date_Dim.d_date_id,Date_Dim.d_date,Date_Dim.d_month_seq,Date_Dim.d_week_seq,Date_Dim.d_quarter_seq,Date_Dim.d_year,Date_Dim.d_dow,Date_Dim.d_moy,Date_Dim.d_dom,Date_Dim.d_qoy,Date_Dim.d_fy_year,Date_Dim.d_fy_quarter_seq,Date_Dim.d_fy_week_seq,Date_Dim.d_day_name ,Date_Dim.d_quarter_name ,Date_Dim.d_holiday ,Date_Dim.d_weekend,Date_Dim.d_following_holiday,Date_Dim.d_first_dom,Date_Dim.d_last_dom,Date_Dim.d_same_day_ly,Date_Dim.d_same_day_lq,Date_Dim.d_current_day,Date_Dim.d_current_week,Date_Dim.d_current_month ,Date_Dim.d_current_quarter,Date_Dim.d_current_year,Date_Dim.d_quarter_seq * 2 ,Date_Dim.d_year * 2 ,Date_Dim.d_moy * 2 ,Date_Dim.d_dom * 2 ,Date_Dim.d_qoy * 2 ,Date_Dim.d_fy_year * 2 
FROM Date_Dim where d_year=1993
 
) 
SELECT  tb38.sr_returned_date_sk, tb38.sr_return_time_sk, tb38.sr_item_sk, tb38.sr_customer_sk, tb38.sr_cdemo_sk, tb38.sr_hdemo_sk, tb38.sr_addr_sk, tb38.sr_store_sk, tb38.sr_reason_sk, tb38.sr_ticket_number, tb39.d_date_id, tb39.d_month_seq, tb38.sr_returned_date_sk, tb38.sr_return_time_sk, tb38.sr_item_sk, tb38.sr_customer_sk, tb38.sr_cdemo_sk, tb38.sr_hdemo_sk, tb38.sr_addr_sk, tb38.sr_store_sk, tb38.sr_reason_sk, tb38.sr_ticket_number, tb39.d_month_seq, tb39.d_year, MAX(sr_return_quantity) AS sr_return_quantity_MAX, COUNT(sr_return_quantity) AS sr_return_quantity_COUNT, SUM(sr_return_amt) AS sr_return_amt_SUM, SUM(sr_return_tax) AS sr_return_tax_SUM
FROM tb38 , tb39
WHERE tb38.sr_returned_date_sk = tb39.d_date_sk
 GROUP BY tb38.sr_returned_date_sk,tb38.sr_return_time_sk,tb38.sr_item_sk,tb38.sr_customer_sk,tb38.sr_cdemo_sk,tb38.sr_hdemo_sk,tb38.sr_addr_sk,tb38.sr_store_sk,tb38.sr_reason_sk,tb38.sr_ticket_number,tb39.d_date_id,tb39.d_month_seq,tb38.sr_returned_date_sk,tb38.sr_return_time_sk,tb38.sr_item_sk,tb38.sr_customer_sk,tb38.sr_cdemo_sk,tb38.sr_hdemo_sk,tb38.sr_addr_sk,tb38.sr_store_sk,tb38.sr_reason_sk,tb38.sr_ticket_number,tb39.d_month_seq,tb39.d_year
 LIMIT 100;
---new sql--2024-01-03 14:33:03-----
long_chain is ['FileScan FactTable', 'Filter', 'Project', 'HashAggregate', 'Filter', 'Project', 'BroadcastHashJoin', 'Project', 'BroadcastHashJoin', 'Project', 'HashAggregate', 'TakeOrderedAndProject', '']
WITH 
tb2 AS (
SELECT Store_Sales.ss_sales_price,Store_Sales.ss_ext_discount_amt,Store_Sales.ss_ext_sales_price,Store_Sales.ss_ext_wholesale_cost,Store_Sales.ss_ext_list_price,Store_Sales.ss_net_profit,Store_Sales.ss_sold_date_sk,Store_Sales.ss_addr_sk,Store_Sales.ss_customer_sk,Store_Sales.ss_item_sk,Store_Sales.ss_ticket_number
FROM Store_Sales where ss_sales_price=10
 
) , 
 tb3 AS (
SELECT Date_Dim.d_date_sk,Date_Dim.d_date_id,Date_Dim.d_date,Date_Dim.d_month_seq,Date_Dim.d_week_seq,Date_Dim.d_quarter_seq,Date_Dim.d_year,Date_Dim.d_dow,Date_Dim.d_moy,Date_Dim.d_dom,Date_Dim.d_qoy,Date_Dim.d_fy_year,Date_Dim.d_fy_quarter_seq,Date_Dim.d_fy_week_seq,Date_Dim.d_day_name ,Date_Dim.d_quarter_name ,Date_Dim.d_holiday ,Date_Dim.d_weekend,Date_Dim.d_following_holiday,Date_Dim.d_first_dom,Date_Dim.d_last_dom,Date_Dim.d_same_day_ly,Date_Dim.d_same_day_lq,Date_Dim.d_current_day,Date_Dim.d_current_week,Date_Dim.d_current_month ,Date_Dim.d_current_quarter,Date_Dim.d_current_year
FROM Date_Dim where d_moy=8
 
) , 
tb1 AS (
SELECT Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number,Store_Returns.sr_return_quantity,Store_Returns.sr_return_amt,Store_Returns.sr_return_tax,Store_Returns.sr_return_amt_inc_tax,Store_Returns.sr_fee,Store_Returns.sr_return_ship_cost,Store_Returns.sr_refunded_cash,Store_Returns.sr_reversed_charge,Store_Returns.sr_store_credit,Store_Returns.sr_net_loss
FROM Store_Returns where sr_return_amt=300
 
) , 
 tb4 AS (
SELECT  tb2.ss_sold_date_sk, tb2.ss_addr_sk, tb2.ss_customer_sk, tb2.ss_item_sk, tb2.ss_ticket_number, tb3.d_date, tb3.d_month_seq, tb3.d_week_seq, MAX(ss_sales_price) AS ss_sales_price_MAX
FROM tb2 , tb3
WHERE tb2.ss_sold_date_sk = tb3.d_date_sk
 GROUP BY tb2.ss_sold_date_sk,tb2.ss_addr_sk,tb2.ss_customer_sk,tb2.ss_item_sk,tb2.ss_ticket_number,tb3.d_date,tb3.d_month_seq,tb3.d_week_seq) , 
tb5 AS (
SELECT tb1.sr_returned_date_sk,tb1.sr_return_time_sk,tb1.sr_item_sk,tb1.sr_customer_sk,tb1.sr_cdemo_sk,tb1.sr_hdemo_sk,tb1.sr_addr_sk,tb1.sr_store_sk,tb1.sr_reason_sk,tb1.sr_ticket_number,tb1.sr_return_quantity,tb1.sr_return_amt,tb1.sr_return_tax,tb1.sr_return_amt_inc_tax,tb1.sr_fee,tb1.sr_return_ship_cost,tb1.sr_refunded_cash,tb1.sr_reversed_charge,tb1.sr_store_credit,tb1.sr_net_loss , tb4.ss_sales_price_MAX,tb4.ss_sold_date_sk,tb4.ss_addr_sk,tb4.ss_customer_sk,tb4.ss_item_sk,tb4.ss_ticket_number
FROM tb1 , tb4
WHERE tb1.sr_ticket_number = tb4.ss_ticket_number
 ) , 
 tb6 AS (
SELECT Date_Dim.d_date_sk,Date_Dim.d_date_id,Date_Dim.d_date,Date_Dim.d_month_seq,Date_Dim.d_week_seq,Date_Dim.d_quarter_seq,Date_Dim.d_year,Date_Dim.d_dow,Date_Dim.d_moy,Date_Dim.d_dom,Date_Dim.d_qoy,Date_Dim.d_fy_year,Date_Dim.d_fy_quarter_seq,Date_Dim.d_fy_week_seq,Date_Dim.d_day_name ,Date_Dim.d_quarter_name ,Date_Dim.d_holiday ,Date_Dim.d_weekend,Date_Dim.d_following_holiday,Date_Dim.d_first_dom,Date_Dim.d_last_dom,Date_Dim.d_same_day_ly,Date_Dim.d_same_day_lq,Date_Dim.d_current_day,Date_Dim.d_current_week,Date_Dim.d_current_month ,Date_Dim.d_current_quarter,Date_Dim.d_current_year,Date_Dim.d_month_seq * 2 ,Date_Dim.d_week_seq * 2 ,Date_Dim.d_fy_week_seq * 2 ,Date_Dim.d_same_day_ly * 2 ,Date_Dim.d_same_day_lq * 2 
FROM Date_Dim where d_moy=8
 
) 
SELECT  tb5.sr_returned_date_sk, tb5.sr_return_time_sk, tb5.sr_item_sk, tb5.sr_customer_sk, tb5.sr_cdemo_sk, tb5.sr_hdemo_sk, tb5.sr_addr_sk, tb5.sr_store_sk, tb5.sr_reason_sk, tb5.sr_ticket_number, tb6.d_date, SUM(sr_return_quantity) AS sr_return_quantity_SUM
FROM tb5 , tb6
WHERE tb5.sr_returned_date_sk = tb6.d_date_sk
 GROUP BY tb5.sr_returned_date_sk,tb5.sr_return_time_sk,tb5.sr_item_sk,tb5.sr_customer_sk,tb5.sr_cdemo_sk,tb5.sr_hdemo_sk,tb5.sr_addr_sk,tb5.sr_store_sk,tb5.sr_reason_sk,tb5.sr_ticket_number,tb6.d_date
 LIMIT 100;
---new sql--2024-01-03 14:33:03-----
long_chain is ['FileScan FactTable', 'Filter', 'Sort', 'SortMergeJoin', 'BroadcastHashJoin', '']
WITH 
tb8 AS (
SELECT Store_Sales.ss_sold_date_sk,Store_Sales.ss_sold_time_sk,Store_Sales.ss_item_sk,Store_Sales.ss_customer_sk,Store_Sales.ss_cdemo_sk,Store_Sales.ss_hdemo_sk,Store_Sales.ss_addr_sk,Store_Sales.ss_store_sk,Store_Sales.ss_promo_sk,Store_Sales.ss_ticket_number,Store_Sales.ss_quantity,Store_Sales.ss_wholesale_cost,Store_Sales.ss_list_price,Store_Sales.ss_sales_price,Store_Sales.ss_ext_discount_amt,Store_Sales.ss_ext_sales_price,Store_Sales.ss_ext_wholesale_cost,Store_Sales.ss_ext_list_price,Store_Sales.ss_ext_tax,Store_Sales.ss_coupon_amt,Store_Sales.ss_net_paid,Store_Sales.ss_net_paid_inc_tax,Store_Sales.ss_net_profit
FROM Store_Sales where ss_sales_price=13
 
) , 
 tb9 AS (
SELECT Date_Dim.d_date_sk,Date_Dim.d_date_id,Date_Dim.d_date,Date_Dim.d_month_seq,Date_Dim.d_week_seq,Date_Dim.d_quarter_seq,Date_Dim.d_year,Date_Dim.d_dow,Date_Dim.d_moy,Date_Dim.d_dom,Date_Dim.d_qoy,Date_Dim.d_fy_year,Date_Dim.d_fy_quarter_seq,Date_Dim.d_fy_week_seq,Date_Dim.d_day_name ,Date_Dim.d_quarter_name ,Date_Dim.d_holiday ,Date_Dim.d_weekend,Date_Dim.d_following_holiday,Date_Dim.d_first_dom,Date_Dim.d_last_dom,Date_Dim.d_same_day_ly,Date_Dim.d_same_day_lq,Date_Dim.d_current_day,Date_Dim.d_current_week,Date_Dim.d_current_month ,Date_Dim.d_current_quarter,Date_Dim.d_current_year,Date_Dim.d_month_seq * 2 ,Date_Dim.d_qoy * 2 ,Date_Dim.d_fy_year * 2 ,Date_Dim.d_fy_quarter_seq * 2 ,Date_Dim.d_first_dom * 2 ,Date_Dim.d_same_day_lq * 2 
FROM Date_Dim where d_quarter_name ='2001Q1'
 
) , 
tb11 AS (
SELECT Store_Sales.ss_quantity,Store_Sales.ss_wholesale_cost,Store_Sales.ss_list_price,Store_Sales.ss_sales_price,Store_Sales.ss_ext_wholesale_cost,Store_Sales.ss_ext_tax,Store_Sales.ss_coupon_amt,Store_Sales.ss_net_paid_inc_tax,Store_Sales.ss_net_profit,Store_Sales.ss_promo_sk,Store_Sales.ss_store_sk,Store_Sales.ss_addr_sk,Store_Sales.ss_sold_date_sk
FROM Store_Sales where ss_ext_discount_amt=12
 
) , 
 tb12 AS (
SELECT Promotion.p_promo_sk,Promotion.p_promo_id,Promotion.p_start_date_sk,Promotion.p_end_date_sk,Promotion.p_item_sk,Promotion.p_cost,Promotion.p_response_target,Promotion.p_promo_name,Promotion.p_channel_dmail,Promotion.p_channel_catalog,Promotion.p_channel_tv,Promotion.p_channel_radio,Promotion.p_channel_press,Promotion.p_channel_demo,Promotion.p_channel_details,Promotion.p_purpose,Promotion.p_discount_active,Promotion.p_channel_email,Promotion.p_channel_event
FROM Promotion where p_channel_email='N'
 
) , 
tb13 AS (
SELECT  tb11.ss_promo_sk, tb11.ss_store_sk, tb11.ss_addr_sk, tb11.ss_sold_date_sk, tb12.p_promo_id, AVG(ss_quantity) AS ss_quantity_AVG, MAX(ss_wholesale_cost) AS ss_wholesale_cost_MAX, AVG(ss_list_price) AS ss_list_price_AVG
FROM tb11 , tb12
WHERE tb11.ss_promo_sk = tb12.p_promo_sk
 GROUP BY tb11.ss_promo_sk,tb11.ss_store_sk,tb11.ss_addr_sk,tb11.ss_sold_date_sk,tb12.p_promo_id) , 
 tb14 AS (
SELECT Store.s_store_sk,Store.s_store_id,Store.s_rec_start_date,Store.s_rec_end_date,Store.s_closed_date_sk,Store.s_store_name,Store.s_number_employees,Store.s_floor_space,Store.s_hours,Store.S_manager,Store.S_market_id,Store.S_geography_class,Store.S_market_desc,Store.s_market_manager,Store.s_division_id,Store.s_division_name,Store.s_company_id,Store.s_company_name,Store.s_street_number,Store.s_street_name,Store.s_street_type,Store.s_suite_number,Store.s_city,Store.s_county,Store.s_state,Store.s_zip,Store.s_country,Store.s_gmt_offset,Store.s_tax_precentage
FROM Store 
 
 
) , 
tb10 AS (
SELECT tb8.ss_sold_date_sk,tb8.ss_sold_time_sk,tb8.ss_item_sk,tb8.ss_customer_sk,tb8.ss_cdemo_sk,tb8.ss_hdemo_sk,tb8.ss_addr_sk,tb8.ss_store_sk,tb8.ss_promo_sk,tb8.ss_ticket_number,tb8.ss_quantity,tb8.ss_wholesale_cost,tb8.ss_list_price,tb8.ss_sales_price,tb8.ss_ext_discount_amt,tb8.ss_ext_sales_price,tb8.ss_ext_wholesale_cost,tb8.ss_ext_list_price,tb8.ss_ext_tax,tb8.ss_coupon_amt,tb8.ss_net_paid,tb8.ss_net_paid_inc_tax,tb8.ss_net_profit , tb9.d_date_id,tb9.d_month_seq,tb9.d_week_seq,tb9.d_quarter_seq,tb9.d_year,tb9.d_dow,tb9.d_moy,tb9.d_dom,tb9.d_qoy,tb9.d_fy_year,tb9.d_fy_quarter_seq,tb9.d_fy_week_seq,tb9.d_day_name ,tb9.d_quarter_name ,tb9.d_holiday ,tb9.d_weekend,tb9.d_following_holiday,tb9.d_first_dom,tb9.d_last_dom,tb9.d_same_day_ly,tb9.d_same_day_lq,tb9.d_current_day,tb9.d_current_week,tb9.d_current_month ,tb9.d_current_quarter,tb9.d_current_year,tb9.d_month_seq * 2 ,tb9.d_qoy * 2 ,tb9.d_fy_year * 2 ,tb9.d_fy_quarter_seq * 2 ,tb9.d_first_dom * 2 ,tb9.d_same_day_lq * 2 
FROM tb8 , tb9
WHERE tb8.ss_sold_date_sk = tb9.d_date_sk
 ) , 
 tb15 AS (
SELECT  tb13.ss_promo_sk, tb13.ss_store_sk, tb13.ss_addr_sk, tb13.ss_sold_date_sk, tb14.s_store_id, tb14.s_rec_start_date, SUM(ss_quantity_AVG) AS ss_quantity_AVG_SUM
FROM tb13 , tb14
WHERE tb13.ss_store_sk = tb14.s_store_sk
 GROUP BY tb13.ss_promo_sk,tb13.ss_store_sk,tb13.ss_addr_sk,tb13.ss_sold_date_sk,tb14.s_store_id,tb14.s_rec_start_date) 
SELECT tb10.ss_sold_date_sk,tb10.ss_sold_time_sk,tb10.ss_item_sk,tb10.ss_customer_sk,tb10.ss_cdemo_sk,tb10.ss_hdemo_sk,tb10.ss_addr_sk,tb10.ss_store_sk,tb10.ss_promo_sk,tb10.ss_ticket_number,tb10.ss_quantity,tb10.ss_wholesale_cost,tb10.ss_list_price,tb10.ss_sales_price,tb10.ss_ext_discount_amt,tb10.ss_ext_sales_price,tb10.ss_ext_wholesale_cost,tb10.ss_ext_list_price,tb10.ss_ext_tax,tb10.ss_coupon_amt,tb10.ss_net_paid,tb10.ss_net_paid_inc_tax,tb10.ss_net_profit , tb15.ss_quantity_AVG_SUM
FROM tb10 , tb15
WHERE tb10.ss_sold_date_sk = tb15.ss_sold_date_sk
 
 LIMIT 100;
---new sql--2024-01-03 14:33:03-----
long_chain is ['FileScan FactTable', 'Filter', 'BroadcastHashJoin', 'Project', 'BroadcastHashJoin', '']
WITH 
tb17 AS (
SELECT Store_Sales.ss_sold_date_sk,Store_Sales.ss_sold_time_sk,Store_Sales.ss_item_sk,Store_Sales.ss_customer_sk,Store_Sales.ss_cdemo_sk,Store_Sales.ss_hdemo_sk,Store_Sales.ss_addr_sk,Store_Sales.ss_store_sk,Store_Sales.ss_promo_sk,Store_Sales.ss_ticket_number,Store_Sales.ss_quantity,Store_Sales.ss_wholesale_cost,Store_Sales.ss_list_price,Store_Sales.ss_sales_price,Store_Sales.ss_ext_discount_amt,Store_Sales.ss_ext_sales_price,Store_Sales.ss_ext_wholesale_cost,Store_Sales.ss_ext_list_price,Store_Sales.ss_ext_tax,Store_Sales.ss_coupon_amt,Store_Sales.ss_net_paid,Store_Sales.ss_net_paid_inc_tax,Store_Sales.ss_net_profit
FROM Store_Sales where ss_sales_price=13
 
) , 
 tb18 AS (
SELECT Date_Dim.d_date_sk,Date_Dim.d_date_id,Date_Dim.d_date,Date_Dim.d_month_seq,Date_Dim.d_week_seq,Date_Dim.d_quarter_seq,Date_Dim.d_year,Date_Dim.d_dow,Date_Dim.d_moy,Date_Dim.d_dom,Date_Dim.d_qoy,Date_Dim.d_fy_year,Date_Dim.d_fy_quarter_seq,Date_Dim.d_fy_week_seq,Date_Dim.d_day_name ,Date_Dim.d_quarter_name ,Date_Dim.d_holiday ,Date_Dim.d_weekend,Date_Dim.d_following_holiday,Date_Dim.d_first_dom,Date_Dim.d_last_dom,Date_Dim.d_same_day_ly,Date_Dim.d_same_day_lq,Date_Dim.d_current_day,Date_Dim.d_current_week,Date_Dim.d_current_month ,Date_Dim.d_current_quarter,Date_Dim.d_current_year,Date_Dim.d_month_seq * 2 ,Date_Dim.d_week_seq * 2 ,Date_Dim.d_dow * 2 ,Date_Dim.d_dom * 2 ,Date_Dim.d_fy_year * 2 ,Date_Dim.d_last_dom * 2 
FROM Date_Dim where d_quarter_name ='2001Q2'
 
) , 
tb20 AS (
SELECT Customer.c_customer_id,Customer.c_salutation,Customer.c_first_name,Customer.c_last_name,Customer.c_preferred_cust_flag,Customer.c_login,Customer.c_email_address,Customer.c_current_hdemo_sk,Customer.c_current_cdemo_sk,Customer.c_customer_sk
FROM Customer 
 
 
) , 
 tb21 AS (
SELECT Household_Demographics.hd_demo_sk,Household_Demographics.hd_income_band_sk,Household_Demographics.hd_buy_potential,Household_Demographics.hd_dep_count,Household_Demographics.hd_vehicle_count
FROM Household_Demographics where hd_dep_count=1
 
) , 
tb19 AS (
SELECT tb17.ss_sold_date_sk,tb17.ss_sold_time_sk,tb17.ss_item_sk,tb17.ss_customer_sk,tb17.ss_cdemo_sk,tb17.ss_hdemo_sk,tb17.ss_addr_sk,tb17.ss_store_sk,tb17.ss_promo_sk,tb17.ss_ticket_number,tb17.ss_quantity,tb17.ss_wholesale_cost,tb17.ss_list_price,tb17.ss_sales_price,tb17.ss_ext_discount_amt,tb17.ss_ext_sales_price,tb17.ss_ext_wholesale_cost,tb17.ss_ext_list_price,tb17.ss_ext_tax,tb17.ss_coupon_amt,tb17.ss_net_paid,tb17.ss_net_paid_inc_tax,tb17.ss_net_profit , tb18.d_date_id,tb18.d_month_seq,tb18.d_week_seq,tb18.d_quarter_seq,tb18.d_year,tb18.d_dow,tb18.d_moy,tb18.d_dom,tb18.d_qoy,tb18.d_fy_year,tb18.d_fy_quarter_seq,tb18.d_fy_week_seq,tb18.d_day_name ,tb18.d_quarter_name ,tb18.d_holiday ,tb18.d_weekend,tb18.d_following_holiday,tb18.d_first_dom,tb18.d_last_dom,tb18.d_same_day_ly,tb18.d_same_day_lq,tb18.d_current_day,tb18.d_current_week,tb18.d_current_month ,tb18.d_current_quarter,tb18.d_current_year,tb18.d_month_seq * 2 ,tb18.d_week_seq * 2 ,tb18.d_dow * 2 ,tb18.d_dom * 2 ,tb18.d_fy_year * 2 ,tb18.d_last_dom * 2 
FROM tb17 , tb18
WHERE tb17.ss_sold_date_sk = tb18.d_date_sk
 ) , 
 tb22 AS (
SELECT tb20.c_customer_id,tb20.c_salutation,tb20.c_first_name,tb20.c_last_name,tb20.c_preferred_cust_flag,tb20.c_login,tb20.c_email_address,tb20.c_current_hdemo_sk,tb20.c_current_cdemo_sk,tb20.c_customer_sk , tb21.hd_demo_sk,tb21.hd_income_band_sk,tb21.hd_buy_potential,tb21.hd_dep_count,tb21.hd_vehicle_count
FROM tb20 , tb21
WHERE tb20.c_current_hdemo_sk = tb21.hd_demo_sk
 ) 
SELECT tb19.ss_sold_date_sk,tb19.ss_sold_time_sk,tb19.ss_item_sk,tb19.ss_customer_sk,tb19.ss_cdemo_sk,tb19.ss_hdemo_sk,tb19.ss_addr_sk,tb19.ss_store_sk,tb19.ss_promo_sk,tb19.ss_ticket_number,tb19.ss_quantity,tb19.ss_wholesale_cost,tb19.ss_list_price,tb19.ss_sales_price,tb19.ss_ext_discount_amt,tb19.ss_ext_sales_price,tb19.ss_ext_wholesale_cost,tb19.ss_ext_list_price,tb19.ss_ext_tax,tb19.ss_coupon_amt,tb19.ss_net_paid,tb19.ss_net_paid_inc_tax,tb19.ss_net_profit , tb22.c_current_hdemo_sk,tb22.c_current_cdemo_sk,tb22.c_customer_sk
FROM tb19 , tb22
WHERE tb19.ss_customer_sk = tb22.c_customer_sk
 
 LIMIT 100;
---new sql--2024-01-03 14:33:03-----
long_chain is ['FileScan FactTable', 'Filter', 'Project', 'HashAggregate', 'Filter', 'Project', 'BroadcastHashJoin', 'Project', 'BroadcastHashJoin', '']
WITH 
tb24 AS (
SELECT Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number,Store_Returns.sr_return_quantity,Store_Returns.sr_return_amt,Store_Returns.sr_return_tax,Store_Returns.sr_return_amt_inc_tax,Store_Returns.sr_fee,Store_Returns.sr_return_ship_cost,Store_Returns.sr_refunded_cash,Store_Returns.sr_reversed_charge,Store_Returns.sr_store_credit,Store_Returns.sr_net_loss
FROM Store_Returns where sr_return_quantity=300
 
) , 
 tb25 AS (
SELECT Date_Dim.d_date_sk,Date_Dim.d_date_id,Date_Dim.d_date,Date_Dim.d_month_seq,Date_Dim.d_week_seq,Date_Dim.d_quarter_seq,Date_Dim.d_year,Date_Dim.d_dow,Date_Dim.d_moy,Date_Dim.d_dom,Date_Dim.d_qoy,Date_Dim.d_fy_year,Date_Dim.d_fy_quarter_seq,Date_Dim.d_fy_week_seq,Date_Dim.d_day_name ,Date_Dim.d_quarter_name ,Date_Dim.d_holiday ,Date_Dim.d_weekend,Date_Dim.d_following_holiday,Date_Dim.d_first_dom,Date_Dim.d_last_dom,Date_Dim.d_same_day_ly,Date_Dim.d_same_day_lq,Date_Dim.d_current_day,Date_Dim.d_current_week,Date_Dim.d_current_month ,Date_Dim.d_current_quarter,Date_Dim.d_current_year,Date_Dim.d_month_seq * 2 ,Date_Dim.d_week_seq * 2 ,Date_Dim.d_year * 2 ,Date_Dim.d_moy * 2 ,Date_Dim.d_dom * 2 ,Date_Dim.d_qoy * 2 ,Date_Dim.d_fy_quarter_seq * 2 ,Date_Dim.d_first_dom * 2 ,Date_Dim.d_same_day_ly * 2 ,Date_Dim.d_same_day_lq * 2 
FROM Date_Dim where d_quarter_name ='2001Q1'
 
) , 
tb26 AS (
SELECT tb24.sr_returned_date_sk,tb24.sr_return_time_sk,tb24.sr_item_sk,tb24.sr_customer_sk,tb24.sr_cdemo_sk,tb24.sr_hdemo_sk,tb24.sr_addr_sk,tb24.sr_store_sk,tb24.sr_reason_sk,tb24.sr_ticket_number,tb24.sr_return_quantity,tb24.sr_return_amt,tb24.sr_return_tax,tb24.sr_return_amt_inc_tax,tb24.sr_fee,tb24.sr_return_ship_cost,tb24.sr_refunded_cash,tb24.sr_reversed_charge,tb24.sr_store_credit,tb24.sr_net_loss , tb25.d_date_id,tb25.d_month_seq,tb25.d_week_seq,tb25.d_quarter_seq,tb25.d_year,tb25.d_dow,tb25.d_moy,tb25.d_dom,tb25.d_qoy,tb25.d_fy_year,tb25.d_fy_quarter_seq,tb25.d_fy_week_seq,tb25.d_day_name ,tb25.d_quarter_name ,tb25.d_holiday ,tb25.d_weekend,tb25.d_following_holiday,tb25.d_first_dom,tb25.d_last_dom,tb25.d_same_day_ly,tb25.d_same_day_lq,tb25.d_current_day,tb25.d_current_week,tb25.d_current_month ,tb25.d_current_quarter,tb25.d_current_year,tb25.d_month_seq * 2 ,tb25.d_week_seq * 2 ,tb25.d_year * 2 ,tb25.d_moy * 2 ,tb25.d_dom * 2 ,tb25.d_qoy * 2 ,tb25.d_fy_quarter_seq * 2 ,tb25.d_first_dom * 2 ,tb25.d_same_day_ly * 2 ,tb25.d_same_day_lq * 2 
FROM tb24 , tb25
WHERE tb24.sr_returned_date_sk = tb25.d_date_sk
 ) , 
 tb27 AS (
SELECT Time_Dim.t_time_sk,Time_Dim.t_time_id,Time_Dim.t_time,Time_Dim.t_hour,Time_Dim.t_minute,Time_Dim.t_second,Time_Dim.t_am_pm,Time_Dim.t_shift,Time_Dim.t_sub_shift,Time_Dim.t_meal_time
FROM Time_Dim 
 
 
) 
SELECT tb26.sr_returned_date_sk,tb26.sr_return_time_sk,tb26.sr_item_sk,tb26.sr_customer_sk,tb26.sr_cdemo_sk,tb26.sr_hdemo_sk,tb26.sr_addr_sk,tb26.sr_store_sk,tb26.sr_reason_sk,tb26.sr_ticket_number,tb26.sr_return_quantity,tb26.sr_return_amt,tb26.sr_return_tax,tb26.sr_return_amt_inc_tax,tb26.sr_fee,tb26.sr_return_ship_cost,tb26.sr_refunded_cash,tb26.sr_reversed_charge,tb26.sr_store_credit,tb26.sr_net_loss , tb27.t_time_sk,tb27.t_time_id,tb27.t_time,tb27.t_hour,tb27.t_minute,tb27.t_second,tb27.t_am_pm,tb27.t_shift,tb27.t_sub_shift,tb27.t_meal_time
FROM tb26 , tb27
WHERE tb26.sr_return_time_sk = tb27.t_time_sk
 
 LIMIT 100;
---new sql--2024-01-03 14:38:04-----
long_chain is ['FileScan FactTable', 'Filter', 'Project', 'HashAggregate', 'Filter', 'Project', 'BroadcastHashJoin', 'Project', 'BroadcastHashJoin', 'Project', 'HashAggregate', 'TakeOrderedAndProject', '']
WITH 
tb2 AS (
SELECT Store_Sales.ss_sales_price,Store_Sales.ss_ext_discount_amt,Store_Sales.ss_ext_wholesale_cost,Store_Sales.ss_ext_tax,Store_Sales.ss_coupon_amt,Store_Sales.ss_net_paid_inc_tax,Store_Sales.ss_net_profit,Store_Sales.ss_customer_sk,Store_Sales.ss_sold_time_sk,Store_Sales.ss_sold_date_sk
FROM Store_Sales where ss_ext_discount_amt=12
 
) , 
 tb3 AS (
SELECT Customer.c_customer_sk,Customer.c_customer_id,Customer.c_current_cdemo_sk,Customer.c_current_hdemo_sk,Customer.c_current_addr_sk,Customer.c_first_shipto_date_sk,Customer.c_first_sales_date_sk,Customer.c_salutation,Customer.c_first_name,Customer.c_last_name,Customer.c_preferred_cust_flag,Customer.c_birth_day,Customer.c_birth_month,Customer.c_birth_year,Customer.c_birth_country,Customer.c_login,Customer.c_email_address
FROM Customer 
 
 
) , 
tb1 AS (
SELECT Store_Sales.ss_sold_date_sk,Store_Sales.ss_sold_time_sk,Store_Sales.ss_item_sk,Store_Sales.ss_customer_sk,Store_Sales.ss_cdemo_sk,Store_Sales.ss_hdemo_sk,Store_Sales.ss_addr_sk,Store_Sales.ss_store_sk,Store_Sales.ss_promo_sk,Store_Sales.ss_ticket_number,Store_Sales.ss_quantity,Store_Sales.ss_wholesale_cost,Store_Sales.ss_list_price,Store_Sales.ss_sales_price,Store_Sales.ss_ext_discount_amt,Store_Sales.ss_ext_sales_price,Store_Sales.ss_ext_wholesale_cost,Store_Sales.ss_ext_list_price,Store_Sales.ss_ext_tax,Store_Sales.ss_coupon_amt,Store_Sales.ss_net_paid,Store_Sales.ss_net_paid_inc_tax,Store_Sales.ss_net_profit
FROM Store_Sales where ss_ext_discount_amt=13
 
) , 
 tb4 AS (
SELECT  tb2.ss_customer_sk, tb2.ss_sold_time_sk, tb2.ss_sold_date_sk, tb3.c_customer_id, tb3.c_salutation, SUM(ss_sales_price) AS ss_sales_price_SUM, AVG(ss_ext_discount_amt) AS ss_ext_discount_amt_AVG
FROM tb2 , tb3
WHERE tb2.ss_customer_sk = tb3.c_customer_sk
 GROUP BY tb2.ss_customer_sk,tb2.ss_sold_time_sk,tb2.ss_sold_date_sk,tb3.c_customer_id,tb3.c_salutation) , 
tb6 AS (
SELECT Catalog_Sales.cs_quantity,Catalog_Sales.cs_wholesale_cost,Catalog_Sales.cs_ext_wholesale_cost,Catalog_Sales.cs_ext_tax,Catalog_Sales.cs_coupon_amt,Catalog_Sales.cs_net_paid,Catalog_Sales.cs_net_paid_inc_tax,Catalog_Sales.cs_net_paid_inc_ship,Catalog_Sales.cs_net_paid_inc_ship_tax,Catalog_Sales.cs_net_profit,Catalog_Sales.cs_bill_customer_sk,Catalog_Sales.cs_ship_customer_sk,Catalog_Sales.cs_item_sk,Catalog_Sales.cs_sold_date_sk,Catalog_Sales.cs_bill_customer_sk,Catalog_Sales.cs_item_sk
FROM Catalog_Sales where cs_quantity=100
 
) , 
 tb7 AS (
SELECT Customer.c_customer_sk,Customer.c_customer_id,Customer.c_current_cdemo_sk,Customer.c_current_hdemo_sk,Customer.c_current_addr_sk,Customer.c_first_shipto_date_sk,Customer.c_first_sales_date_sk,Customer.c_salutation,Customer.c_first_name,Customer.c_last_name,Customer.c_preferred_cust_flag,Customer.c_birth_day,Customer.c_birth_month,Customer.c_birth_year,Customer.c_birth_country,Customer.c_login,Customer.c_email_address
FROM Customer 
 
 
) , 
tb8 AS (
SELECT  tb6.cs_bill_customer_sk, tb6.cs_ship_customer_sk, tb6.cs_item_sk, tb6.cs_sold_date_sk, tb6.cs_bill_customer_sk, tb6.cs_item_sk, tb7.c_customer_id, tb7.c_preferred_cust_flag, tb7.c_birth_day, SUM(cs_quantity) AS cs_quantity_SUM, SUM(cs_wholesale_cost) AS cs_wholesale_cost_SUM, SUM(cs_ext_wholesale_cost) AS cs_ext_wholesale_cost_SUM
FROM tb6 , tb7
WHERE tb6.cs_ship_customer_sk = tb7.c_customer_sk and tb6.cs_bill_customer_sk = tb7.c_customer_sk
 GROUP BY tb6.cs_bill_customer_sk,tb6.cs_ship_customer_sk,tb6.cs_item_sk,tb6.cs_sold_date_sk,tb6.cs_bill_customer_sk,tb6.cs_item_sk,tb7.c_customer_id,tb7.c_preferred_cust_flag,tb7.c_birth_day) , 
 tb9 AS (
SELECT Item.i_item_sk,Item.i_item_id,Item.i_rec_start_date,Item.i_rec_end_date,Item.i_item_desc,Item.i_current_price,Item.i_wholesale_cost,Item.i_brand_id,Item.i_brand,Item.i_class_id,Item.i_class,Item.i_category_id,Item.i_category,Item.i_manufact_id,Item.i_manufact,Item.i_size,Item.i_formulation,Item.i_color,Item.i_units,Item.i_container,Item.i_manager_id,Item.i_product_name
FROM Item where i_manager_id=8
 
) , 
tb5 AS (
SELECT tb1.ss_sold_date_sk,tb1.ss_sold_time_sk,tb1.ss_item_sk,tb1.ss_customer_sk,tb1.ss_cdemo_sk,tb1.ss_hdemo_sk,tb1.ss_addr_sk,tb1.ss_store_sk,tb1.ss_promo_sk,tb1.ss_ticket_number,tb1.ss_quantity,tb1.ss_wholesale_cost,tb1.ss_list_price,tb1.ss_sales_price,tb1.ss_ext_discount_amt,tb1.ss_ext_sales_price,tb1.ss_ext_wholesale_cost,tb1.ss_ext_list_price,tb1.ss_ext_tax,tb1.ss_coupon_amt,tb1.ss_net_paid,tb1.ss_net_paid_inc_tax,tb1.ss_net_profit , tb4.ss_sales_price_SUM,tb4.ss_ext_discount_amt_AVG
FROM tb1 , tb4
WHERE tb1.ss_sold_date_sk = tb4.ss_sold_date_sk
 ) , 
 tb10 AS (
SELECT  tb8.cs_bill_customer_sk, tb8.cs_ship_customer_sk, tb8.cs_item_sk, tb8.cs_sold_date_sk, tb8.cs_bill_customer_sk, tb8.cs_item_sk, tb9.i_item_id, tb9.i_rec_start_date, SUM(cs_quantity_SUM) AS cs_quantity_SUM_SUM, SUM(cs_wholesale_cost_SUM) AS cs_wholesale_cost_SUM_SUM
FROM tb8 , tb9
WHERE tb8.cs_item_sk = tb9.i_item_sk
 GROUP BY tb8.cs_bill_customer_sk,tb8.cs_ship_customer_sk,tb8.cs_item_sk,tb8.cs_sold_date_sk,tb8.cs_bill_customer_sk,tb8.cs_item_sk,tb9.i_item_id,tb9.i_rec_start_date) 
SELECT  tb5.ss_sold_date_sk, tb5.ss_sold_time_sk, tb5.ss_item_sk, tb5.ss_customer_sk, tb5.ss_cdemo_sk, tb5.ss_hdemo_sk, tb5.ss_addr_sk, tb5.ss_store_sk, tb5.ss_promo_sk, tb5.ss_ticket_number, tb10.cs_quantity_SUM_SUM, tb10.cs_wholesale_cost_SUM_SUM, SUM(ss_quantity) AS ss_quantity_SUM, SUM(ss_wholesale_cost) AS ss_wholesale_cost_SUM, SUM(ss_list_price) AS ss_list_price_SUM
FROM tb5 , tb10
WHERE tb5.ss_customer_sk = tb10.cs_bill_customer_sk and tb5.ss_item_sk = tb10.cs_item_sk
 GROUP BY tb5.ss_sold_date_sk,tb5.ss_sold_time_sk,tb5.ss_item_sk,tb5.ss_customer_sk,tb5.ss_cdemo_sk,tb5.ss_hdemo_sk,tb5.ss_addr_sk,tb5.ss_store_sk,tb5.ss_promo_sk,tb5.ss_ticket_number,tb10.cs_quantity_SUM_SUM,tb10.cs_wholesale_cost_SUM_SUM
 LIMIT 100;
---new sql--2024-01-03 14:38:05-----
long_chain is ['FileScan FactTable', 'Filter', 'BroadcastHashJoin', 'Project', 'BroadcastHashJoin', '']
WITH 
tb20 AS (
SELECT Store_Returns.sr_return_quantity,Store_Returns.sr_return_amt_inc_tax,Store_Returns.sr_fee,Store_Returns.sr_net_loss,Store_Returns.sr_cdemo_sk,Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_item_sk,Store_Returns.sr_ticket_number
FROM Store_Returns where sr_return_quantity=100
 
) , 
 tb21 AS (
SELECT Customer_Demographics.cd_demo_sk,Customer_Demographics.cd_gender,Customer_Demographics.cd_marital_status,Customer_Demographics.cd_education_status,Customer_Demographics.cd_purchase_estimate,Customer_Demographics.cd_credit_rating,Customer_Demographics.cd_dep_count,Customer_Demographics.cd_dep_employed_count,Customer_Demographics.cd_dep_college_count
FROM Customer_Demographics where cd_marital_status='College'
 
) , 
tb22 AS (
SELECT  tb20.sr_cdemo_sk, tb20.sr_returned_date_sk, tb20.sr_return_time_sk, tb20.sr_customer_sk, tb20.sr_item_sk, tb20.sr_ticket_number, tb21.cd_purchase_estimate, tb21.cd_credit_rating, AVG(sr_return_quantity) AS sr_return_quantity_AVG, SUM(sr_return_amt_inc_tax) AS sr_return_amt_inc_tax_SUM
FROM tb20 , tb21
WHERE tb20.sr_cdemo_sk = tb21.cd_demo_sk
 GROUP BY tb20.sr_cdemo_sk,tb20.sr_returned_date_sk,tb20.sr_return_time_sk,tb20.sr_customer_sk,tb20.sr_item_sk,tb20.sr_ticket_number,tb21.cd_purchase_estimate,tb21.cd_credit_rating) , 
 tb23 AS (
SELECT Date_Dim.d_date_sk,Date_Dim.d_date_id,Date_Dim.d_date,Date_Dim.d_month_seq,Date_Dim.d_week_seq,Date_Dim.d_quarter_seq,Date_Dim.d_year,Date_Dim.d_dow,Date_Dim.d_moy,Date_Dim.d_dom,Date_Dim.d_qoy,Date_Dim.d_fy_year,Date_Dim.d_fy_quarter_seq,Date_Dim.d_fy_week_seq,Date_Dim.d_day_name ,Date_Dim.d_quarter_name ,Date_Dim.d_holiday ,Date_Dim.d_weekend,Date_Dim.d_following_holiday,Date_Dim.d_first_dom,Date_Dim.d_last_dom,Date_Dim.d_same_day_ly,Date_Dim.d_same_day_lq,Date_Dim.d_current_day,Date_Dim.d_current_week,Date_Dim.d_current_month ,Date_Dim.d_current_quarter,Date_Dim.d_current_year
FROM Date_Dim where d_quarter_name ='2001Q2'
 
) , 
tb19 AS (
SELECT Store_Sales.ss_sold_date_sk,Store_Sales.ss_sold_time_sk,Store_Sales.ss_item_sk,Store_Sales.ss_customer_sk,Store_Sales.ss_cdemo_sk,Store_Sales.ss_hdemo_sk,Store_Sales.ss_addr_sk,Store_Sales.ss_store_sk,Store_Sales.ss_promo_sk,Store_Sales.ss_ticket_number,Store_Sales.ss_quantity,Store_Sales.ss_wholesale_cost,Store_Sales.ss_list_price,Store_Sales.ss_sales_price,Store_Sales.ss_ext_discount_amt,Store_Sales.ss_ext_sales_price,Store_Sales.ss_ext_wholesale_cost,Store_Sales.ss_ext_list_price,Store_Sales.ss_ext_tax,Store_Sales.ss_coupon_amt,Store_Sales.ss_net_paid,Store_Sales.ss_net_paid_inc_tax,Store_Sales.ss_net_profit
FROM Store_Sales where ss_ext_discount_amt=13
 
) , 
 tb24 AS (
SELECT  tb22.sr_cdemo_sk, tb22.sr_returned_date_sk, tb22.sr_return_time_sk, tb22.sr_customer_sk, tb22.sr_item_sk, tb22.sr_ticket_number, tb23.d_date_id, AVG(sr_return_quantity_AVG) AS sr_return_quantity_AVG_AVG
FROM tb22 , tb23
WHERE tb22.sr_returned_date_sk = tb23.d_date_sk
 GROUP BY tb22.sr_cdemo_sk,tb22.sr_returned_date_sk,tb22.sr_return_time_sk,tb22.sr_customer_sk,tb22.sr_item_sk,tb22.sr_ticket_number,tb23.d_date_id) , 
tb26 AS (
SELECT Catalog_Sales.cs_quantity,Catalog_Sales.cs_wholesale_cost,Catalog_Sales.cs_ext_discount_amt,Catalog_Sales.cs_ext_wholesale_cost,Catalog_Sales.cs_coupon_amt,Catalog_Sales.cs_ext_ship_cost,Catalog_Sales.cs_net_paid,Catalog_Sales.cs_net_paid_inc_tax,Catalog_Sales.cs_net_paid_inc_ship,Catalog_Sales.cs_net_paid_inc_ship_tax,Catalog_Sales.cs_item_sk,Catalog_Sales.cs_bill_customer_sk,Catalog_Sales.cs_ship_customer_sk,Catalog_Sales.cs_sold_date_sk,Catalog_Sales.cs_bill_customer_sk,Catalog_Sales.cs_item_sk
FROM Catalog_Sales where cs_net_profit=200
 
) , 
 tb27 AS (
SELECT Item.i_item_sk,Item.i_item_id,Item.i_rec_start_date,Item.i_rec_end_date,Item.i_item_desc,Item.i_current_price,Item.i_wholesale_cost,Item.i_brand_id,Item.i_brand,Item.i_class_id,Item.i_class,Item.i_category_id,Item.i_category,Item.i_manufact_id,Item.i_manufact,Item.i_size,Item.i_formulation,Item.i_color,Item.i_units,Item.i_container,Item.i_manager_id,Item.i_product_name
FROM Item where i_manager_id=8
 
) , 
tb28 AS (
SELECT  tb26.cs_item_sk, tb26.cs_bill_customer_sk, tb26.cs_ship_customer_sk, tb26.cs_sold_date_sk, tb26.cs_bill_customer_sk, tb26.cs_item_sk, tb27.i_rec_start_date, MAX(cs_quantity) AS cs_quantity_MAX, COUNT(cs_wholesale_cost) AS cs_wholesale_cost_COUNT, SUM(cs_ext_discount_amt) AS cs_ext_discount_amt_SUM
FROM tb26 , tb27
WHERE tb26.cs_item_sk = tb27.i_item_sk
 GROUP BY tb26.cs_item_sk,tb26.cs_bill_customer_sk,tb26.cs_ship_customer_sk,tb26.cs_sold_date_sk,tb26.cs_bill_customer_sk,tb26.cs_item_sk,tb27.i_rec_start_date) , 
 tb29 AS (
SELECT Customer.c_customer_sk,Customer.c_customer_id,Customer.c_current_cdemo_sk,Customer.c_current_hdemo_sk,Customer.c_current_addr_sk,Customer.c_first_shipto_date_sk,Customer.c_first_sales_date_sk,Customer.c_salutation,Customer.c_first_name,Customer.c_last_name,Customer.c_preferred_cust_flag,Customer.c_birth_day,Customer.c_birth_month,Customer.c_birth_year,Customer.c_birth_country,Customer.c_login,Customer.c_email_address
FROM Customer 
 
 
) , 
tb25 AS (
SELECT tb19.ss_sold_date_sk,tb19.ss_sold_time_sk,tb19.ss_item_sk,tb19.ss_customer_sk,tb19.ss_cdemo_sk,tb19.ss_hdemo_sk,tb19.ss_addr_sk,tb19.ss_store_sk,tb19.ss_promo_sk,tb19.ss_ticket_number,tb19.ss_quantity,tb19.ss_wholesale_cost,tb19.ss_list_price,tb19.ss_sales_price,tb19.ss_ext_discount_amt,tb19.ss_ext_sales_price,tb19.ss_ext_wholesale_cost,tb19.ss_ext_list_price,tb19.ss_ext_tax,tb19.ss_coupon_amt,tb19.ss_net_paid,tb19.ss_net_paid_inc_tax,tb19.ss_net_profit , tb24.sr_return_quantity_AVG_AVG,tb24.sr_cdemo_sk,tb24.sr_returned_date_sk,tb24.sr_return_time_sk,tb24.sr_customer_sk,tb24.sr_item_sk,tb24.sr_ticket_number
FROM tb19 , tb24
WHERE tb19.ss_customer_sk = tb24.sr_customer_sk and tb19.ss_ticket_number = tb24.sr_ticket_number and tb19.ss_item_sk = tb24.sr_item_sk
 ) , 
 tb30 AS (
SELECT  tb28.cs_item_sk, tb28.cs_bill_customer_sk, tb28.cs_ship_customer_sk, tb28.cs_sold_date_sk, tb28.cs_bill_customer_sk, tb28.cs_item_sk, tb29.c_salutation, tb29.c_first_name, tb29.c_preferred_cust_flag, SUM(cs_quantity_MAX) AS cs_quantity_MAX_SUM
FROM tb28 , tb29
WHERE tb28.cs_ship_customer_sk = tb29.c_customer_sk and tb28.cs_bill_customer_sk = tb29.c_customer_sk
 GROUP BY tb28.cs_item_sk,tb28.cs_bill_customer_sk,tb28.cs_ship_customer_sk,tb28.cs_sold_date_sk,tb28.cs_bill_customer_sk,tb28.cs_item_sk,tb29.c_salutation,tb29.c_first_name,tb29.c_preferred_cust_flag) 
SELECT tb25.ss_sold_date_sk,tb25.ss_sold_time_sk,tb25.ss_item_sk,tb25.ss_customer_sk,tb25.ss_cdemo_sk,tb25.ss_hdemo_sk,tb25.ss_addr_sk,tb25.ss_store_sk,tb25.ss_promo_sk,tb25.ss_ticket_number,tb25.ss_quantity,tb25.ss_wholesale_cost,tb25.ss_list_price,tb25.ss_sales_price,tb25.ss_ext_discount_amt,tb25.ss_ext_sales_price,tb25.ss_ext_wholesale_cost,tb25.ss_ext_list_price,tb25.ss_ext_tax,tb25.ss_coupon_amt,tb25.ss_net_paid,tb25.ss_net_paid_inc_tax,tb25.ss_net_profit , tb30.cs_quantity_MAX_SUM,tb30.cs_item_sk,tb30.cs_bill_customer_sk,tb30.cs_ship_customer_sk,tb30.cs_sold_date_sk,tb30.cs_bill_customer_sk,tb30.cs_item_sk
FROM tb25 , tb30
WHERE tb25.ss_item_sk = tb30.cs_item_sk
 
 LIMIT 100;
---new sql--2024-01-03 14:38:05-----
long_chain is ['FileScan FactTable', 'Filter', 'Project', 'HashAggregate', 'Filter', 'Project', 'BroadcastHashJoin', 'Project', 'BroadcastHashJoin', '']
WITH 
tb33 AS (
SELECT Customer.c_salutation,Customer.c_preferred_cust_flag,Customer.c_birth_day,Customer.c_birth_year,Customer.c_birth_country,Customer.c_current_hdemo_sk,Customer.c_current_addr_sk,Customer.c_current_cdemo_sk,Customer.c_customer_sk
FROM Customer 
 
 
) , 
 tb34 AS (
SELECT Household_Demographics.hd_demo_sk,Household_Demographics.hd_income_band_sk,Household_Demographics.hd_buy_potential,Household_Demographics.hd_dep_count,Household_Demographics.hd_vehicle_count
FROM Household_Demographics where hd_dep_count=1
 
) , 
tb35 AS (
SELECT  tb33.c_current_hdemo_sk, tb33.c_current_addr_sk, tb33.c_current_cdemo_sk, tb33.c_customer_sk, tb34.hd_buy_potential, tb34.hd_vehicle_count, SUM(c_birth_day) AS c_birth_day_SUM, SUM(c_birth_year) AS c_birth_year_SUM
FROM tb33 , tb34
WHERE tb33.c_current_hdemo_sk = tb34.hd_demo_sk
 GROUP BY tb33.c_current_hdemo_sk,tb33.c_current_addr_sk,tb33.c_current_cdemo_sk,tb33.c_customer_sk,tb34.hd_buy_potential,tb34.hd_vehicle_count) , 
 tb36 AS (
SELECT Customer_Address.ca_address_sk,Customer_Address.ca_address_id,Customer_Address.ca_street_number,Customer_Address.ca_street_name,Customer_Address.ca_street_type,Customer_Address.ca_suite_number,Customer_Address.ca_city,Customer_Address.ca_county,Customer_Address.ca_state,Customer_Address.ca_zip,Customer_Address.ca_country,Customer_Address.ca_gmt_offset,Customer_Address.ca_location_type
FROM Customer_Address where ca_gmt_offset=-5
 
) , 
tb32 AS (
SELECT Store_Sales.ss_sold_date_sk,Store_Sales.ss_sold_time_sk,Store_Sales.ss_item_sk,Store_Sales.ss_customer_sk,Store_Sales.ss_cdemo_sk,Store_Sales.ss_hdemo_sk,Store_Sales.ss_addr_sk,Store_Sales.ss_store_sk,Store_Sales.ss_promo_sk,Store_Sales.ss_ticket_number,Store_Sales.ss_quantity,Store_Sales.ss_wholesale_cost,Store_Sales.ss_list_price,Store_Sales.ss_sales_price,Store_Sales.ss_ext_discount_amt,Store_Sales.ss_ext_sales_price,Store_Sales.ss_ext_wholesale_cost,Store_Sales.ss_ext_list_price,Store_Sales.ss_ext_tax,Store_Sales.ss_coupon_amt,Store_Sales.ss_net_paid,Store_Sales.ss_net_paid_inc_tax,Store_Sales.ss_net_profit
FROM Store_Sales where ss_ext_discount_amt=10
 
) , 
 tb37 AS (
SELECT  tb35.c_current_hdemo_sk, tb35.c_current_addr_sk, tb35.c_current_cdemo_sk, tb35.c_customer_sk, tb36.ca_street_number, tb36.ca_street_type, tb36.ca_city, MAX(c_birth_day_SUM) AS c_birth_day_SUM_MAX
FROM tb35 , tb36
WHERE tb35.c_current_addr_sk = tb36.ca_address_sk
 GROUP BY tb35.c_current_hdemo_sk,tb35.c_current_addr_sk,tb35.c_current_cdemo_sk,tb35.c_customer_sk,tb36.ca_street_number,tb36.ca_street_type,tb36.ca_city) , 
tb39 AS (
SELECT Store_Sales.ss_ext_discount_amt,Store_Sales.ss_ext_sales_price,Store_Sales.ss_ext_wholesale_cost,Store_Sales.ss_ext_tax,Store_Sales.ss_coupon_amt,Store_Sales.ss_net_profit,Store_Sales.ss_hdemo_sk,Store_Sales.ss_item_sk,Store_Sales.ss_sold_date_sk
FROM Store_Sales where ss_list_price=13
 
) , 
 tb40 AS (
SELECT Household_Demographics.hd_demo_sk,Household_Demographics.hd_income_band_sk,Household_Demographics.hd_buy_potential,Household_Demographics.hd_dep_count,Household_Demographics.hd_vehicle_count
FROM Household_Demographics where hd_dep_count=1
 
) , 
tb38 AS (
SELECT tb32.ss_sold_date_sk,tb32.ss_sold_time_sk,tb32.ss_item_sk,tb32.ss_customer_sk,tb32.ss_cdemo_sk,tb32.ss_hdemo_sk,tb32.ss_addr_sk,tb32.ss_store_sk,tb32.ss_promo_sk,tb32.ss_ticket_number,tb32.ss_quantity,tb32.ss_wholesale_cost,tb32.ss_list_price,tb32.ss_sales_price,tb32.ss_ext_discount_amt,tb32.ss_ext_sales_price,tb32.ss_ext_wholesale_cost,tb32.ss_ext_list_price,tb32.ss_ext_tax,tb32.ss_coupon_amt,tb32.ss_net_paid,tb32.ss_net_paid_inc_tax,tb32.ss_net_profit , tb37.c_birth_day_SUM_MAX,tb37.c_current_hdemo_sk,tb37.c_current_addr_sk,tb37.c_current_cdemo_sk,tb37.c_customer_sk
FROM tb32 , tb37
WHERE tb32.ss_customer_sk = tb37.c_customer_sk
 ) , 
 tb41 AS (
SELECT  tb39.ss_hdemo_sk, tb39.ss_item_sk, tb39.ss_sold_date_sk, tb40.hd_buy_potential, MAX(ss_ext_discount_amt) AS ss_ext_discount_amt_MAX, SUM(ss_ext_sales_price) AS ss_ext_sales_price_SUM, SUM(ss_ext_wholesale_cost) AS ss_ext_wholesale_cost_SUM
FROM tb39 , tb40
WHERE tb39.ss_hdemo_sk = tb40.hd_demo_sk
 GROUP BY tb39.ss_hdemo_sk,tb39.ss_item_sk,tb39.ss_sold_date_sk,tb40.hd_buy_potential) 
SELECT tb38.ss_sold_date_sk,tb38.ss_sold_time_sk,tb38.ss_item_sk,tb38.ss_customer_sk,tb38.ss_cdemo_sk,tb38.ss_hdemo_sk,tb38.ss_addr_sk,tb38.ss_store_sk,tb38.ss_promo_sk,tb38.ss_ticket_number,tb38.ss_quantity,tb38.ss_wholesale_cost,tb38.ss_list_price,tb38.ss_sales_price,tb38.ss_ext_discount_amt,tb38.ss_ext_sales_price,tb38.ss_ext_wholesale_cost,tb38.ss_ext_list_price,tb38.ss_ext_tax,tb38.ss_coupon_amt,tb38.ss_net_paid,tb38.ss_net_paid_inc_tax,tb38.ss_net_profit , tb41.ss_ext_discount_amt_MAX,tb41.ss_ext_sales_price_SUM,tb41.ss_ext_wholesale_cost_SUM
FROM tb38 , tb41
WHERE tb38.ss_sold_date_sk = tb41.ss_sold_date_sk
 
 LIMIT 100;
---new sql--2024-01-03 14:38:05-----
long_chain is ['FileScan FactTable', 'Filter', 'Sort', 'SortMergeJoin', 'BroadcastHashJoin', '']
WITH 
tb43 AS (
SELECT Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number,Store_Returns.sr_return_quantity,Store_Returns.sr_return_amt,Store_Returns.sr_return_tax,Store_Returns.sr_return_amt_inc_tax,Store_Returns.sr_fee,Store_Returns.sr_return_ship_cost,Store_Returns.sr_refunded_cash,Store_Returns.sr_reversed_charge,Store_Returns.sr_store_credit,Store_Returns.sr_net_loss
FROM Store_Returns where sr_return_amt=200
 
) , 
 tb44 AS (
SELECT Date_Dim.d_date_sk,Date_Dim.d_date_id,Date_Dim.d_date,Date_Dim.d_month_seq,Date_Dim.d_week_seq,Date_Dim.d_quarter_seq,Date_Dim.d_year,Date_Dim.d_dow,Date_Dim.d_moy,Date_Dim.d_dom,Date_Dim.d_qoy,Date_Dim.d_fy_year,Date_Dim.d_fy_quarter_seq,Date_Dim.d_fy_week_seq,Date_Dim.d_day_name ,Date_Dim.d_quarter_name ,Date_Dim.d_holiday ,Date_Dim.d_weekend,Date_Dim.d_following_holiday,Date_Dim.d_first_dom,Date_Dim.d_last_dom,Date_Dim.d_same_day_ly,Date_Dim.d_same_day_lq,Date_Dim.d_current_day,Date_Dim.d_current_week,Date_Dim.d_current_month ,Date_Dim.d_current_quarter,Date_Dim.d_current_year
FROM Date_Dim 
 
 
) , 
tb46 AS (
SELECT Store_Sales.ss_wholesale_cost,Store_Sales.ss_list_price,Store_Sales.ss_sales_price,Store_Sales.ss_ext_sales_price,Store_Sales.ss_ext_wholesale_cost,Store_Sales.ss_coupon_amt,Store_Sales.ss_net_paid_inc_tax,Store_Sales.ss_store_sk,Store_Sales.ss_promo_sk,Store_Sales.ss_addr_sk,Store_Sales.ss_customer_sk,Store_Sales.ss_ticket_number,Store_Sales.ss_item_sk
FROM Store_Sales where ss_sales_price=13
 
) , 
 tb47 AS (
SELECT Store.s_store_sk,Store.s_store_id,Store.s_rec_start_date,Store.s_rec_end_date,Store.s_closed_date_sk,Store.s_store_name,Store.s_number_employees,Store.s_floor_space,Store.s_hours,Store.S_manager,Store.S_market_id,Store.S_geography_class,Store.S_market_desc,Store.s_market_manager,Store.s_division_id,Store.s_division_name,Store.s_company_id,Store.s_company_name,Store.s_street_number,Store.s_street_name,Store.s_street_type,Store.s_suite_number,Store.s_city,Store.s_county,Store.s_state,Store.s_zip,Store.s_country,Store.s_gmt_offset,Store.s_tax_precentage
FROM Store 
 
 
) , 
tb48 AS (
SELECT  tb46.ss_store_sk, tb46.ss_promo_sk, tb46.ss_addr_sk, tb46.ss_customer_sk, tb46.ss_ticket_number, tb46.ss_item_sk, tb47.s_rec_start_date, tb47.s_store_name, tb47.s_number_employees, SUM(ss_wholesale_cost) AS ss_wholesale_cost_SUM
FROM tb46 , tb47
WHERE tb46.ss_store_sk = tb47.s_store_sk
 GROUP BY tb46.ss_store_sk,tb46.ss_promo_sk,tb46.ss_addr_sk,tb46.ss_customer_sk,tb46.ss_ticket_number,tb46.ss_item_sk,tb47.s_rec_start_date,tb47.s_store_name,tb47.s_number_employees) , 
 tb49 AS (
SELECT Promotion.p_promo_sk,Promotion.p_promo_id,Promotion.p_start_date_sk,Promotion.p_end_date_sk,Promotion.p_item_sk,Promotion.p_cost,Promotion.p_response_target,Promotion.p_promo_name,Promotion.p_channel_dmail,Promotion.p_channel_catalog,Promotion.p_channel_tv,Promotion.p_channel_radio,Promotion.p_channel_press,Promotion.p_channel_demo,Promotion.p_channel_details,Promotion.p_purpose,Promotion.p_discount_active,Promotion.p_channel_email,Promotion.p_channel_event
FROM Promotion where p_channel_email='N'
 
) , 
tb45 AS (
SELECT tb43.sr_returned_date_sk,tb43.sr_return_time_sk,tb43.sr_item_sk,tb43.sr_customer_sk,tb43.sr_cdemo_sk,tb43.sr_hdemo_sk,tb43.sr_addr_sk,tb43.sr_store_sk,tb43.sr_reason_sk,tb43.sr_ticket_number,tb43.sr_return_quantity,tb43.sr_return_amt,tb43.sr_return_tax,tb43.sr_return_amt_inc_tax,tb43.sr_fee,tb43.sr_return_ship_cost,tb43.sr_refunded_cash,tb43.sr_reversed_charge,tb43.sr_store_credit,tb43.sr_net_loss , tb44.d_date_id,tb44.d_month_seq,tb44.d_week_seq,tb44.d_quarter_seq,tb44.d_year,tb44.d_dow,tb44.d_moy,tb44.d_dom,tb44.d_qoy,tb44.d_fy_year,tb44.d_fy_quarter_seq,tb44.d_fy_week_seq,tb44.d_day_name ,tb44.d_quarter_name ,tb44.d_holiday ,tb44.d_weekend,tb44.d_following_holiday,tb44.d_first_dom,tb44.d_last_dom,tb44.d_same_day_ly,tb44.d_same_day_lq,tb44.d_current_day,tb44.d_current_week,tb44.d_current_month ,tb44.d_current_quarter,tb44.d_current_year
FROM tb43 , tb44
WHERE tb43.sr_returned_date_sk = tb44.d_date_sk
 ) , 
 tb50 AS (
SELECT  tb48.ss_store_sk, tb48.ss_promo_sk, tb48.ss_addr_sk, tb48.ss_customer_sk, tb48.ss_ticket_number, tb48.ss_item_sk, tb49.p_promo_id, tb49.p_cost, MAX(ss_wholesale_cost_SUM) AS ss_wholesale_cost_SUM_MAX
FROM tb48 , tb49
WHERE tb48.ss_promo_sk = tb49.p_promo_sk
 GROUP BY tb48.ss_store_sk,tb48.ss_promo_sk,tb48.ss_addr_sk,tb48.ss_customer_sk,tb48.ss_ticket_number,tb48.ss_item_sk,tb49.p_promo_id,tb49.p_cost) 
SELECT tb45.sr_returned_date_sk,tb45.sr_return_time_sk,tb45.sr_item_sk,tb45.sr_customer_sk,tb45.sr_cdemo_sk,tb45.sr_hdemo_sk,tb45.sr_addr_sk,tb45.sr_store_sk,tb45.sr_reason_sk,tb45.sr_ticket_number,tb45.sr_return_quantity,tb45.sr_return_amt,tb45.sr_return_tax,tb45.sr_return_amt_inc_tax,tb45.sr_fee,tb45.sr_return_ship_cost,tb45.sr_refunded_cash,tb45.sr_reversed_charge,tb45.sr_store_credit,tb45.sr_net_loss , tb50.ss_wholesale_cost_SUM_MAX,tb50.ss_store_sk,tb50.ss_promo_sk,tb50.ss_addr_sk,tb50.ss_customer_sk,tb50.ss_ticket_number,tb50.ss_item_sk
FROM tb45 , tb50
WHERE tb45.sr_item_sk = tb50.ss_item_sk
 
 LIMIT 100;
---new sql--2024-01-03 15:05:27-----
long_chain is ['FileScan FactTable', 'Filter', 'Project', 'HashAggregate', 'Filter', 'Project', 'BroadcastHashJoin', 'Project', 'BroadcastHashJoin', '']
WITH 
tb2 AS (
SELECT Customer.c_customer_id,Customer.c_salutation,Customer.c_first_name,Customer.c_preferred_cust_flag,Customer.c_birth_month,Customer.c_login,Customer.c_first_sales_date_sk,Customer.c_current_addr_sk,Customer.c_current_hdemo_sk,Customer.c_customer_sk
FROM Customer 
 
 
) , 
 tb3 AS (
SELECT Date_Dim.d_date_sk,Date_Dim.d_date_id,Date_Dim.d_date,Date_Dim.d_month_seq,Date_Dim.d_week_seq,Date_Dim.d_quarter_seq,Date_Dim.d_year,Date_Dim.d_dow,Date_Dim.d_moy,Date_Dim.d_dom,Date_Dim.d_qoy,Date_Dim.d_fy_year,Date_Dim.d_fy_quarter_seq,Date_Dim.d_fy_week_seq,Date_Dim.d_day_name ,Date_Dim.d_quarter_name ,Date_Dim.d_holiday ,Date_Dim.d_weekend,Date_Dim.d_following_holiday,Date_Dim.d_first_dom,Date_Dim.d_last_dom,Date_Dim.d_same_day_ly,Date_Dim.d_same_day_lq,Date_Dim.d_current_day,Date_Dim.d_current_week,Date_Dim.d_current_month ,Date_Dim.d_current_quarter,Date_Dim.d_current_year
FROM Date_Dim where d_moy=8
 
) , 
tb4 AS (
SELECT  tb2.c_first_sales_date_sk, tb2.c_current_addr_sk, tb2.c_current_hdemo_sk, tb2.c_customer_sk, tb3.d_month_seq, tb3.d_week_seq, tb3.d_year, SUM(c_birth_month) AS c_birth_month_SUM
FROM tb2 , tb3
WHERE tb2.c_first_sales_date_sk = tb3.d_date_sk
 GROUP BY tb2.c_first_sales_date_sk,tb2.c_current_addr_sk,tb2.c_current_hdemo_sk,tb2.c_customer_sk,tb3.d_month_seq,tb3.d_week_seq,tb3.d_year) , 
 tb5 AS (
SELECT Customer_Address.ca_address_sk,Customer_Address.ca_address_id,Customer_Address.ca_street_number,Customer_Address.ca_street_name,Customer_Address.ca_street_type,Customer_Address.ca_suite_number,Customer_Address.ca_city,Customer_Address.ca_county,Customer_Address.ca_state,Customer_Address.ca_zip,Customer_Address.ca_country,Customer_Address.ca_gmt_offset,Customer_Address.ca_location_type
FROM Customer_Address where ca_county='Rush Country'
 
) , 
tb1 AS (
SELECT Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number,Store_Returns.sr_return_quantity,Store_Returns.sr_return_amt,Store_Returns.sr_return_tax,Store_Returns.sr_return_amt_inc_tax,Store_Returns.sr_fee,Store_Returns.sr_return_ship_cost,Store_Returns.sr_refunded_cash,Store_Returns.sr_reversed_charge,Store_Returns.sr_store_credit,Store_Returns.sr_net_loss
FROM Store_Returns where sr_return_amt=100
 
) , 
 tb6 AS (
SELECT  tb4.c_first_sales_date_sk, tb4.c_current_addr_sk, tb4.c_current_hdemo_sk, tb4.c_customer_sk, tb5.ca_street_name, tb5.ca_street_type, SUM(c_birth_month_SUM) AS c_birth_month_SUM_SUM
FROM tb4 , tb5
WHERE tb4.c_current_addr_sk = tb5.ca_address_sk
 GROUP BY tb4.c_first_sales_date_sk,tb4.c_current_addr_sk,tb4.c_current_hdemo_sk,tb4.c_customer_sk,tb5.ca_street_name,tb5.ca_street_type) , 
tb8 AS (
SELECT Catalog_Sales.cs_quantity,Catalog_Sales.cs_ext_discount_amt,Catalog_Sales.cs_ext_sales_price,Catalog_Sales.cs_ext_wholesale_cost,Catalog_Sales.cs_ext_list_price,Catalog_Sales.cs_ext_tax,Catalog_Sales.cs_coupon_amt,Catalog_Sales.cs_net_paid_inc_ship_tax,Catalog_Sales.cs_bill_customer_sk,Catalog_Sales.cs_ship_customer_sk,Catalog_Sales.cs_sold_date_sk,Catalog_Sales.cs_bill_customer_sk,Catalog_Sales.cs_item_sk
FROM Catalog_Sales where cs_net_profit=200
 
) , 
 tb9 AS (
SELECT Customer.c_customer_sk,Customer.c_customer_id,Customer.c_current_cdemo_sk,Customer.c_current_hdemo_sk,Customer.c_current_addr_sk,Customer.c_first_shipto_date_sk,Customer.c_first_sales_date_sk,Customer.c_salutation,Customer.c_first_name,Customer.c_last_name,Customer.c_preferred_cust_flag,Customer.c_birth_day,Customer.c_birth_month,Customer.c_birth_year,Customer.c_birth_country,Customer.c_login,Customer.c_email_address
FROM Customer 
 
 
) , 
tb7 AS (
SELECT tb1.sr_returned_date_sk,tb1.sr_return_time_sk,tb1.sr_item_sk,tb1.sr_customer_sk,tb1.sr_cdemo_sk,tb1.sr_hdemo_sk,tb1.sr_addr_sk,tb1.sr_store_sk,tb1.sr_reason_sk,tb1.sr_ticket_number,tb1.sr_return_quantity,tb1.sr_return_amt,tb1.sr_return_tax,tb1.sr_return_amt_inc_tax,tb1.sr_fee,tb1.sr_return_ship_cost,tb1.sr_refunded_cash,tb1.sr_reversed_charge,tb1.sr_store_credit,tb1.sr_net_loss , tb6.c_birth_month_SUM_SUM,tb6.c_first_sales_date_sk,tb6.c_current_addr_sk,tb6.c_current_hdemo_sk,tb6.c_customer_sk
FROM tb1 , tb6
WHERE tb1.sr_customer_sk = tb6.c_customer_sk
 ) , 
 tb10 AS (
SELECT  tb8.cs_bill_customer_sk, tb8.cs_ship_customer_sk, tb8.cs_sold_date_sk, tb8.cs_bill_customer_sk, tb8.cs_item_sk, tb9.c_last_name, tb9.c_preferred_cust_flag, SUM(cs_quantity) AS cs_quantity_SUM, SUM(cs_ext_discount_amt) AS cs_ext_discount_amt_SUM
FROM tb8 , tb9
WHERE tb8.cs_bill_customer_sk = tb9.c_customer_sk and tb8.cs_ship_customer_sk = tb9.c_customer_sk
 GROUP BY tb8.cs_bill_customer_sk,tb8.cs_ship_customer_sk,tb8.cs_sold_date_sk,tb8.cs_bill_customer_sk,tb8.cs_item_sk,tb9.c_last_name,tb9.c_preferred_cust_flag) 
SELECT tb7.sr_returned_date_sk,tb7.sr_return_time_sk,tb7.sr_item_sk,tb7.sr_customer_sk,tb7.sr_cdemo_sk,tb7.sr_hdemo_sk,tb7.sr_addr_sk,tb7.sr_store_sk,tb7.sr_reason_sk,tb7.sr_ticket_number,tb7.sr_return_quantity,tb7.sr_return_amt,tb7.sr_return_tax,tb7.sr_return_amt_inc_tax,tb7.sr_fee,tb7.sr_return_ship_cost,tb7.sr_refunded_cash,tb7.sr_reversed_charge,tb7.sr_store_credit,tb7.sr_net_loss , tb10.cs_quantity_SUM,tb10.cs_ext_discount_amt_SUM,tb10.cs_bill_customer_sk,tb10.cs_ship_customer_sk,tb10.cs_sold_date_sk,tb10.cs_bill_customer_sk,tb10.cs_item_sk
FROM tb7 , tb10
WHERE tb7.sr_customer_sk = tb10.cs_bill_customer_sk and tb7.sr_item_sk = tb10.cs_item_sk
 
 LIMIT 100;
---new sql--2024-01-03 15:05:27-----
long_chain is ['FileScan FactTable', 'Filter', 'Project', 'HashAggregate', 'Filter', 'Project', 'BroadcastHashJoin', 'Project', 'Sort', 'SortMergeJoin', 'Project', 'TakeOrderedAndProject', '']
WITH 
tb13 AS (
SELECT Catalog_Sales.cs_quantity,Catalog_Sales.cs_wholesale_cost,Catalog_Sales.cs_list_price,Catalog_Sales.cs_ext_list_price,Catalog_Sales.cs_ext_tax,Catalog_Sales.cs_net_paid,Catalog_Sales.cs_net_paid_inc_ship,Catalog_Sales.cs_net_profit,Catalog_Sales.cs_bill_customer_sk,Catalog_Sales.cs_ship_customer_sk,Catalog_Sales.cs_sold_time_sk,Catalog_Sales.cs_bill_customer_sk,Catalog_Sales.cs_item_sk
FROM Catalog_Sales where cs_quantity=100
 
) , 
 tb14 AS (
SELECT Customer.c_customer_sk,Customer.c_customer_id,Customer.c_current_cdemo_sk,Customer.c_current_hdemo_sk,Customer.c_current_addr_sk,Customer.c_first_shipto_date_sk,Customer.c_first_sales_date_sk,Customer.c_salutation,Customer.c_first_name,Customer.c_last_name,Customer.c_preferred_cust_flag,Customer.c_birth_day,Customer.c_birth_month,Customer.c_birth_year,Customer.c_birth_country,Customer.c_login,Customer.c_email_address
FROM Customer 
 
 
) , 
tb12 AS (
SELECT Store_Sales.ss_sold_date_sk,Store_Sales.ss_sold_time_sk,Store_Sales.ss_item_sk,Store_Sales.ss_customer_sk,Store_Sales.ss_cdemo_sk,Store_Sales.ss_hdemo_sk,Store_Sales.ss_addr_sk,Store_Sales.ss_store_sk,Store_Sales.ss_promo_sk,Store_Sales.ss_ticket_number,Store_Sales.ss_quantity,Store_Sales.ss_wholesale_cost,Store_Sales.ss_list_price,Store_Sales.ss_sales_price,Store_Sales.ss_ext_discount_amt,Store_Sales.ss_ext_sales_price,Store_Sales.ss_ext_wholesale_cost,Store_Sales.ss_ext_list_price,Store_Sales.ss_ext_tax,Store_Sales.ss_coupon_amt,Store_Sales.ss_net_paid,Store_Sales.ss_net_paid_inc_tax,Store_Sales.ss_net_profit
FROM Store_Sales where ss_ext_discount_amt=12
 
) , 
 tb15 AS (
SELECT  tb13.cs_bill_customer_sk, tb13.cs_ship_customer_sk, tb13.cs_sold_time_sk, tb13.cs_bill_customer_sk, tb13.cs_item_sk, tb14.c_salutation, SUM(cs_quantity) AS cs_quantity_SUM, COUNT(cs_wholesale_cost) AS cs_wholesale_cost_COUNT
FROM tb13 , tb14
WHERE tb13.cs_bill_customer_sk = tb14.c_customer_sk
 GROUP BY tb13.cs_bill_customer_sk,tb13.cs_ship_customer_sk,tb13.cs_sold_time_sk,tb13.cs_bill_customer_sk,tb13.cs_item_sk,tb14.c_salutation) , 
tb16 AS (
SELECT tb12.ss_sold_date_sk,tb12.ss_sold_time_sk,tb12.ss_item_sk,tb12.ss_customer_sk,tb12.ss_cdemo_sk,tb12.ss_hdemo_sk,tb12.ss_addr_sk,tb12.ss_store_sk,tb12.ss_promo_sk,tb12.ss_ticket_number,tb12.ss_quantity,tb12.ss_wholesale_cost,tb12.ss_list_price,tb12.ss_sales_price,tb12.ss_ext_discount_amt,tb12.ss_ext_sales_price,tb12.ss_ext_wholesale_cost,tb12.ss_ext_list_price,tb12.ss_ext_tax,tb12.ss_coupon_amt,tb12.ss_net_paid,tb12.ss_net_paid_inc_tax,tb12.ss_net_profit , tb15.cs_quantity_SUM,tb15.cs_wholesale_cost_COUNT,tb15.cs_bill_customer_sk,tb15.cs_ship_customer_sk,tb15.cs_sold_time_sk,tb15.cs_bill_customer_sk,tb15.cs_item_sk
FROM tb12 , tb15
WHERE tb12.ss_item_sk = tb15.cs_item_sk
 ) , 
 tb17 AS (
SELECT Date_Dim.d_date_sk,Date_Dim.d_date_id,Date_Dim.d_date,Date_Dim.d_month_seq,Date_Dim.d_week_seq,Date_Dim.d_quarter_seq,Date_Dim.d_year,Date_Dim.d_dow,Date_Dim.d_moy,Date_Dim.d_dom,Date_Dim.d_qoy,Date_Dim.d_fy_year,Date_Dim.d_fy_quarter_seq,Date_Dim.d_fy_week_seq,Date_Dim.d_day_name ,Date_Dim.d_quarter_name ,Date_Dim.d_holiday ,Date_Dim.d_weekend,Date_Dim.d_following_holiday,Date_Dim.d_first_dom,Date_Dim.d_last_dom,Date_Dim.d_same_day_ly,Date_Dim.d_same_day_lq,Date_Dim.d_current_day,Date_Dim.d_current_week,Date_Dim.d_current_month ,Date_Dim.d_current_quarter,Date_Dim.d_current_year,Date_Dim.d_month_seq * 2 ,Date_Dim.d_week_seq * 2 ,Date_Dim.d_quarter_seq * 2 ,Date_Dim.d_qoy * 2 ,Date_Dim.d_same_day_lq * 2 
FROM Date_Dim where d_year=1993
 
) 
SELECT tb16.ss_sold_date_sk,tb16.ss_sold_time_sk,tb16.ss_item_sk,tb16.ss_customer_sk,tb16.ss_cdemo_sk,tb16.ss_hdemo_sk,tb16.ss_addr_sk,tb16.ss_store_sk,tb16.ss_promo_sk,tb16.ss_ticket_number,tb16.ss_quantity,tb16.ss_wholesale_cost,tb16.ss_list_price,tb16.ss_sales_price,tb16.ss_ext_discount_amt,tb16.ss_ext_sales_price,tb16.ss_ext_wholesale_cost,tb16.ss_ext_list_price,tb16.ss_ext_tax,tb16.ss_coupon_amt,tb16.ss_net_paid,tb16.ss_net_paid_inc_tax,tb16.ss_net_profit , tb17.d_date_id,tb17.d_month_seq,tb17.d_week_seq,tb17.d_quarter_seq,tb17.d_year,tb17.d_dow,tb17.d_moy,tb17.d_dom,tb17.d_qoy,tb17.d_fy_year,tb17.d_fy_quarter_seq,tb17.d_fy_week_seq,tb17.d_day_name ,tb17.d_quarter_name ,tb17.d_holiday ,tb17.d_weekend,tb17.d_following_holiday,tb17.d_first_dom,tb17.d_last_dom,tb17.d_same_day_ly,tb17.d_same_day_lq,tb17.d_current_day,tb17.d_current_week,tb17.d_current_month ,tb17.d_current_quarter,tb17.d_current_year,tb17.d_month_seq * 2 ,tb17.d_week_seq * 2 ,tb17.d_quarter_seq * 2 ,tb17.d_qoy * 2 ,tb17.d_same_day_lq * 2 
FROM tb16 , tb17
WHERE tb16.ss_sold_date_sk = tb17.d_date_sk
 
 LIMIT 100;