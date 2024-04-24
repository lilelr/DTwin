-- http://slave4:18081/history/application_1701917778070_0013/SQL/execution/?id=1

-- 53 s
WITH
tb1 AS (
SELECT Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number,Store_Returns.sr_return_quantity,Store_Returns.sr_return_amt,Store_Returns.sr_return_tax,Store_Returns.sr_return_amt_inc_tax,Store_Returns.sr_fee,Store_Returns.sr_return_ship_cost,Store_Returns.sr_refunded_cash,Store_Returns.sr_reversed_charge,Store_Returns.sr_store_credit,Store_Returns.sr_net_loss
FROM Store_Returns where sr_return_quantity=100

) ,
 tb2 AS (
SELECT Date_Dim.d_date_sk,Date_Dim.d_date_id,Date_Dim.d_date,Date_Dim.d_month_seq,Date_Dim.d_week_seq,Date_Dim.d_quarter_seq,Date_Dim.d_year,Date_Dim.d_dow,Date_Dim.d_moy,Date_Dim.d_dom,Date_Dim.d_qoy,Date_Dim.d_fy_year,Date_Dim.d_fy_quarter_seq,Date_Dim.d_fy_week_seq,Date_Dim.d_day_name ,Date_Dim.d_quarter_name ,Date_Dim.d_holiday ,Date_Dim.d_weekend,Date_Dim.d_following_holiday,Date_Dim.d_first_dom,Date_Dim.d_last_dom,Date_Dim.d_same_day_ly,Date_Dim.d_same_day_lq,Date_Dim.d_current_day,Date_Dim.d_current_week,Date_Dim.d_current_month ,Date_Dim.d_current_quarter,Date_Dim.d_current_year
FROM Date_Dim


) ,
tb3 AS (
SELECT tb1.sr_returned_date_sk,tb1.sr_return_time_sk,tb1.sr_item_sk,tb1.sr_customer_sk,tb1.sr_cdemo_sk,tb1.sr_hdemo_sk,tb1.sr_addr_sk,tb1.sr_store_sk,tb1.sr_reason_sk,tb1.sr_ticket_number,tb1.sr_return_quantity,tb1.sr_return_amt,tb1.sr_return_tax,tb1.sr_return_amt_inc_tax,tb1.sr_fee,tb1.sr_return_ship_cost,tb1.sr_refunded_cash,tb1.sr_reversed_charge,tb1.sr_store_credit,tb1.sr_net_loss , tb2.d_date_sk,tb2.d_date_id,tb2.d_date,tb2.d_month_seq,tb2.d_week_seq,tb2.d_quarter_seq,tb2.d_year,tb2.d_dow,tb2.d_moy,tb2.d_dom,tb2.d_qoy,tb2.d_fy_year,tb2.d_fy_quarter_seq,tb2.d_fy_week_seq,tb2.d_day_name ,tb2.d_quarter_name ,tb2.d_holiday ,tb2.d_weekend,tb2.d_following_holiday,tb2.d_first_dom,tb2.d_last_dom,tb2.d_same_day_ly,tb2.d_same_day_lq,tb2.d_current_day,tb2.d_current_week,tb2.d_current_month ,tb2.d_current_quarter,tb2.d_current_year
FROM tb1 , tb2
WHERE tb1.sr_returned_date_sk = tb2.d_date_sk
 ) ,
 tb4 AS (
SELECT Time_Dim.t_time_sk,Time_Dim.t_time_id,Time_Dim.t_time,Time_Dim.t_hour,Time_Dim.t_minute,Time_Dim.t_second,Time_Dim.t_am_pm,Time_Dim.t_shift,Time_Dim.t_sub_shift,Time_Dim.t_meal_time
FROM Time_Dim


) ,
tb6 AS (
SELECT Catalog_Sales.cs_list_price,Catalog_Sales.cs_ext_discount_amt,Catalog_Sales.cs_ext_wholesale_cost,Catalog_Sales.cs_ext_list_price,Catalog_Sales.cs_ext_tax,Catalog_Sales.cs_coupon_amt,Catalog_Sales.cs_net_paid_inc_ship_tax,Catalog_Sales.cs_item_sk,Catalog_Sales.cs_bill_customer_sk,Catalog_Sales.cs_ship_customer_sk,Catalog_Sales.cs_sold_time_sk,Catalog_Sales.cs_bill_customer_sk,Catalog_Sales.cs_item_sk
FROM Catalog_Sales where cs_net_profit=200

) ,
 tb7 AS (
SELECT Item.i_item_sk,Item.i_item_id,Item.i_rec_start_date,Item.i_rec_end_date,Item.i_item_desc,Item.i_current_price,Item.i_wholesale_cost,Item.i_brand_id,Item.i_brand,Item.i_class_id,Item.i_class,Item.i_category_id,Item.i_category,Item.i_manufact_id,Item.i_manufact,Item.i_size,Item.i_formulation,Item.i_color,Item.i_units,Item.i_container,Item.i_manager_id,Item.i_product_name
FROM Item where i_manufact_id=940

) ,
tb8 AS (
SELECT  tb6.cs_item_sk, tb6.cs_bill_customer_sk, tb6.cs_ship_customer_sk, tb6.cs_sold_time_sk, tb6.cs_bill_customer_sk, tb6.cs_item_sk, tb7.i_item_desc, MAX(cs_list_price) AS cs_list_price_MAX, MAX(cs_ext_discount_amt) AS cs_ext_discount_amt_MAX
FROM tb6 , tb7
WHERE tb6.cs_item_sk = tb7.i_item_sk
 GROUP BY tb6.cs_item_sk,tb6.cs_bill_customer_sk,tb6.cs_ship_customer_sk,tb6.cs_sold_time_sk,tb6.cs_bill_customer_sk,tb6.cs_item_sk,tb7.i_item_desc) ,
 tb9 AS (
SELECT Customer.c_customer_sk,Customer.c_customer_id,Customer.c_current_cdemo_sk,Customer.c_current_hdemo_sk,Customer.c_current_addr_sk,Customer.c_first_shipto_date_sk,Customer.c_first_sales_date_sk,Customer.c_salutation,Customer.c_first_name,Customer.c_last_name,Customer.c_preferred_cust_flag,Customer.c_birth_day,Customer.c_birth_month,Customer.c_birth_year,Customer.c_birth_country,Customer.c_login,Customer.c_email_address
FROM Customer


) ,
tb5 AS (
SELECT tb3.sr_returned_date_sk,tb3.sr_return_time_sk,tb3.sr_item_sk,tb3.sr_customer_sk,tb3.sr_cdemo_sk,tb3.sr_hdemo_sk,tb3.sr_addr_sk,tb3.sr_store_sk,tb3.sr_reason_sk,tb3.sr_ticket_number,tb3.sr_return_quantity,tb3.sr_return_amt,tb3.sr_return_tax,tb3.sr_return_amt_inc_tax,tb3.sr_fee,tb3.sr_return_ship_cost,tb3.sr_refunded_cash,tb3.sr_reversed_charge,tb3.sr_store_credit,tb3.sr_net_loss , tb4.t_time_sk,tb4.t_time_id,tb4.t_time,tb4.t_hour,tb4.t_minute,tb4.t_second,tb4.t_am_pm,tb4.t_shift,tb4.t_sub_shift,tb4.t_meal_time
FROM tb3 , tb4
WHERE tb3.sr_return_time_sk = tb4.t_time_sk
 ) ,
 tb10 AS (
SELECT  tb8.cs_item_sk, tb8.cs_bill_customer_sk, tb8.cs_ship_customer_sk, tb8.cs_sold_time_sk, tb8.cs_bill_customer_sk, tb8.cs_item_sk, tb9.c_last_name, tb9.c_birth_month, tb9.c_birth_year, SUM(cs_list_price_MAX) AS cs_list_price_MAX_SUM, SUM(cs_ext_discount_amt_MAX) AS cs_ext_discount_amt_MAX_SUM
FROM tb8 , tb9
WHERE tb8.cs_bill_customer_sk = tb9.c_customer_sk
 GROUP BY tb8.cs_item_sk,tb8.cs_bill_customer_sk,tb8.cs_ship_customer_sk,tb8.cs_sold_time_sk,tb8.cs_bill_customer_sk,tb8.cs_item_sk,tb9.c_last_name,tb9.c_birth_month,tb9.c_birth_year) ,
tb12 AS (
SELECT Store_Returns.sr_return_quantity,Store_Returns.sr_return_amt,Store_Returns.sr_fee,Store_Returns.sr_return_ship_cost,Store_Returns.sr_refunded_cash,Store_Returns.sr_reversed_charge,Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_returned_date_sk
FROM Store_Returns where sr_return_amt=100

) ,
 tb13 AS (
SELECT Date_Dim.d_date_sk,Date_Dim.d_date_id,Date_Dim.d_date,Date_Dim.d_month_seq,Date_Dim.d_week_seq,Date_Dim.d_quarter_seq,Date_Dim.d_year,Date_Dim.d_dow,Date_Dim.d_moy,Date_Dim.d_dom,Date_Dim.d_qoy,Date_Dim.d_fy_year,Date_Dim.d_fy_quarter_seq,Date_Dim.d_fy_week_seq,Date_Dim.d_day_name ,Date_Dim.d_quarter_name ,Date_Dim.d_holiday ,Date_Dim.d_weekend,Date_Dim.d_following_holiday,Date_Dim.d_first_dom,Date_Dim.d_last_dom,Date_Dim.d_same_day_ly,Date_Dim.d_same_day_lq,Date_Dim.d_current_day,Date_Dim.d_current_week,Date_Dim.d_current_month ,Date_Dim.d_current_quarter,Date_Dim.d_current_year
FROM Date_Dim where d_quarter_name ='2001Q1'

) ,
tb14 AS (
SELECT  tb12.sr_returned_date_sk, tb12.sr_return_time_sk, tb12.sr_cdemo_sk, tb12.sr_returned_date_sk, tb13.d_date_id, tb13.d_date, tb13.d_week_seq, SUM(sr_return_quantity) AS sr_return_quantity_SUM, SUM(sr_return_amt) AS sr_return_amt_SUM, SUM(sr_fee) AS sr_fee_SUM
FROM tb12 , tb13
WHERE tb12.sr_returned_date_sk = tb13.d_date_sk
 GROUP BY tb12.sr_returned_date_sk,tb12.sr_return_time_sk,tb12.sr_cdemo_sk,tb12.sr_returned_date_sk,tb13.d_date_id,tb13.d_date,tb13.d_week_seq) ,
 tb15 AS (
SELECT Time_Dim.t_time_sk,Time_Dim.t_time_id,Time_Dim.t_time,Time_Dim.t_hour,Time_Dim.t_minute,Time_Dim.t_second,Time_Dim.t_am_pm,Time_Dim.t_shift,Time_Dim.t_sub_shift,Time_Dim.t_meal_time
FROM Time_Dim


) ,
tb11 AS (
SELECT tb5.sr_returned_date_sk,tb5.sr_return_time_sk,tb5.sr_item_sk,tb5.sr_customer_sk,tb5.sr_cdemo_sk,tb5.sr_hdemo_sk,tb5.sr_addr_sk,tb5.sr_store_sk,tb5.sr_reason_sk,tb5.sr_ticket_number,tb5.sr_return_quantity,tb5.sr_return_amt,tb5.sr_return_tax,tb5.sr_return_amt_inc_tax,tb5.sr_fee,tb5.sr_return_ship_cost,tb5.sr_refunded_cash,tb5.sr_reversed_charge,tb5.sr_store_credit,tb5.sr_net_loss , tb10.cs_list_price_MAX_SUM,tb10.cs_ext_discount_amt_MAX_SUM,tb10.cs_item_sk,tb10.cs_bill_customer_sk,tb10.cs_ship_customer_sk,tb10.cs_sold_time_sk,tb10.cs_bill_customer_sk,tb10.cs_item_sk
FROM tb5 , tb10
WHERE tb5.sr_customer_sk = tb10.cs_bill_customer_sk and tb5.sr_item_sk = tb10.cs_item_sk
 ) ,
 tb16 AS (
SELECT  tb14.sr_returned_date_sk, tb14.sr_return_time_sk, tb14.sr_cdemo_sk, tb14.sr_returned_date_sk, tb15.t_time_id, tb15.t_sub_shift, tb15.t_meal_time, SUM(sr_return_quantity_SUM) AS sr_return_quantity_SUM_SUM
FROM tb14 , tb15
WHERE tb14.sr_return_time_sk = tb15.t_time_sk
 GROUP BY tb14.sr_returned_date_sk,tb14.sr_return_time_sk,tb14.sr_cdemo_sk,tb14.sr_returned_date_sk,tb15.t_time_id,tb15.t_sub_shift,tb15.t_meal_time)
SELECT  tb11.sr_returned_date_sk, tb11.sr_return_time_sk, tb11.sr_item_sk, tb11.sr_customer_sk, tb11.sr_cdemo_sk, tb11.sr_hdemo_sk, tb11.sr_addr_sk, tb11.sr_store_sk, tb11.sr_reason_sk, tb11.sr_ticket_number, tb16.sr_return_quantity_SUM_SUM, AVG(sr_return_quantity) AS sr_return_quantity_AVG, MAX(sr_return_amt) AS sr_return_amt_MAX, SUM(sr_return_tax) AS sr_return_tax_SUM
FROM tb11 , tb16
WHERE tb11.sr_returned_date_sk = tb16.sr_returned_date_sk
 GROUP BY tb11.sr_returned_date_sk,tb11.sr_return_time_sk,tb11.sr_item_sk,tb11.sr_customer_sk,tb11.sr_cdemo_sk,tb11.sr_hdemo_sk,tb11.sr_addr_sk,tb11.sr_store_sk,tb11.sr_reason_sk,tb11.sr_ticket_number,tb16.sr_return_quantity_SUM_SUM
 LIMIT 100;