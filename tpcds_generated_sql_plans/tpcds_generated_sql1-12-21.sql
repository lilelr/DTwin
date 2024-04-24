-- 1.1 minutes 100 GB
WITH
tb1 AS (
SELECT Store_Sales.ss_sold_date_sk,Store_Sales.ss_sold_time_sk,Store_Sales.ss_item_sk,Store_Sales.ss_customer_sk,Store_Sales.ss_cdemo_sk,Store_Sales.ss_hdemo_sk,Store_Sales.ss_addr_sk,Store_Sales.ss_store_sk,Store_Sales.ss_promo_sk,Store_Sales.ss_ticket_number,Store_Sales.ss_quantity,Store_Sales.ss_wholesale_cost,Store_Sales.ss_list_price,Store_Sales.ss_sales_price,Store_Sales.ss_ext_discount_amt,Store_Sales.ss_ext_sales_price,Store_Sales.ss_ext_wholesale_cost,Store_Sales.ss_ext_list_price,Store_Sales.ss_ext_tax,Store_Sales.ss_coupon_amt,Store_Sales.ss_net_paid,Store_Sales.ss_net_paid_inc_tax,Store_Sales.ss_net_profit
FROM Store_Sales where ss_ext_discount_amt=13

) ,
 tb2 AS (
SELECT Date_Dim.d_date_sk,Date_Dim.d_date_id,Date_Dim.d_date,Date_Dim.d_month_seq,Date_Dim.d_week_seq,Date_Dim.d_quarter_seq,Date_Dim.d_year,Date_Dim.d_dow,Date_Dim.d_moy,Date_Dim.d_dom,Date_Dim.d_qoy,Date_Dim.d_fy_year,Date_Dim.d_fy_quarter_seq,Date_Dim.d_fy_week_seq,Date_Dim.d_day_name ,Date_Dim.d_quarter_name ,Date_Dim.d_holiday ,Date_Dim.d_weekend,Date_Dim.d_following_holiday,Date_Dim.d_first_dom,Date_Dim.d_last_dom,Date_Dim.d_same_day_ly,Date_Dim.d_same_day_lq,Date_Dim.d_current_day,Date_Dim.d_current_week,Date_Dim.d_current_month ,Date_Dim.d_current_quarter,Date_Dim.d_current_year
FROM Date_Dim


) ,
tb4 AS (
SELECT Store_Returns.sr_return_amt,Store_Returns.sr_fee,Store_Returns.sr_return_ship_cost,Store_Returns.sr_reversed_charge,Store_Returns.sr_store_credit,Store_Returns.sr_net_loss,Store_Returns.sr_customer_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_item_sk,Store_Returns.sr_ticket_number
FROM Store_Returns where sr_return_amt=200

) ,
 tb5 AS (
SELECT Customer.c_customer_sk,Customer.c_customer_id,Customer.c_current_cdemo_sk,Customer.c_current_hdemo_sk,Customer.c_current_addr_sk,Customer.c_first_shipto_date_sk,Customer.c_first_sales_date_sk,Customer.c_salutation,Customer.c_first_name,Customer.c_last_name,Customer.c_preferred_cust_flag,Customer.c_birth_day,Customer.c_birth_month,Customer.c_birth_year,Customer.c_birth_country,Customer.c_login,Customer.c_email_address
FROM Customer


) ,
tb6 AS (
SELECT  tb4.sr_customer_sk, tb4.sr_return_time_sk, tb4.sr_cdemo_sk, tb4.sr_customer_sk, tb4.sr_item_sk, tb4.sr_ticket_number, tb5.c_birth_country, tb5.c_login, tb5.c_email_address, SUM(sr_return_amt) AS sr_return_amt_SUM, SUM(sr_fee) AS sr_fee_SUM, MAX(sr_return_ship_cost) AS sr_return_ship_cost_MAX
FROM tb4 , tb5
WHERE tb4.sr_customer_sk = tb5.c_customer_sk
 GROUP BY tb4.sr_customer_sk,tb4.sr_return_time_sk,tb4.sr_cdemo_sk,tb4.sr_customer_sk,tb4.sr_item_sk,tb4.sr_ticket_number,tb5.c_birth_country,tb5.c_login,tb5.c_email_address) ,
 tb7 AS (
SELECT Time_Dim.t_time_sk,Time_Dim.t_time_id,Time_Dim.t_time,Time_Dim.t_hour,Time_Dim.t_minute,Time_Dim.t_second,Time_Dim.t_am_pm,Time_Dim.t_shift,Time_Dim.t_sub_shift,Time_Dim.t_meal_time
FROM Time_Dim


) ,
tb3 AS (
SELECT tb1.ss_sold_date_sk,tb1.ss_sold_time_sk,tb1.ss_item_sk,tb1.ss_customer_sk,tb1.ss_cdemo_sk,tb1.ss_hdemo_sk,tb1.ss_addr_sk,tb1.ss_store_sk,tb1.ss_promo_sk,tb1.ss_ticket_number,tb1.ss_quantity,tb1.ss_wholesale_cost,tb1.ss_list_price,tb1.ss_sales_price,tb1.ss_ext_discount_amt,tb1.ss_ext_sales_price,tb1.ss_ext_wholesale_cost,tb1.ss_ext_list_price,tb1.ss_ext_tax,tb1.ss_coupon_amt,tb1.ss_net_paid,tb1.ss_net_paid_inc_tax,tb1.ss_net_profit , tb2.d_date_sk,tb2.d_date_id,tb2.d_date,tb2.d_month_seq,tb2.d_week_seq,tb2.d_quarter_seq,tb2.d_year,tb2.d_dow,tb2.d_moy,tb2.d_dom,tb2.d_qoy,tb2.d_fy_year,tb2.d_fy_quarter_seq,tb2.d_fy_week_seq,tb2.d_day_name ,tb2.d_quarter_name ,tb2.d_holiday ,tb2.d_weekend,tb2.d_following_holiday,tb2.d_first_dom,tb2.d_last_dom,tb2.d_same_day_ly,tb2.d_same_day_lq,tb2.d_current_day,tb2.d_current_week,tb2.d_current_month ,tb2.d_current_quarter,tb2.d_current_year
FROM tb1 , tb2
WHERE tb1.ss_sold_date_sk = tb2.d_date_sk
 ) ,
 tb8 AS (
SELECT  tb6.sr_customer_sk, tb6.sr_return_time_sk, tb6.sr_cdemo_sk, tb6.sr_customer_sk, tb6.sr_item_sk, tb6.sr_ticket_number, tb7.t_time, tb7.t_hour, SUM(sr_return_amt_SUM) AS sr_return_amt_SUM_SUM, SUM(sr_fee_SUM) AS sr_fee_SUM_SUM, SUM(sr_return_ship_cost_MAX) AS sr_return_ship_cost_MAX_SUM
FROM tb6 , tb7
WHERE tb6.sr_return_time_sk = tb7.t_time_sk
 GROUP BY tb6.sr_customer_sk,tb6.sr_return_time_sk,tb6.sr_cdemo_sk,tb6.sr_customer_sk,tb6.sr_item_sk,tb6.sr_ticket_number,tb7.t_time,tb7.t_hour) ,
tb10 AS (
SELECT Store_Sales.ss_sales_price,Store_Sales.ss_ext_wholesale_cost,Store_Sales.ss_net_profit,Store_Sales.ss_hdemo_sk,Store_Sales.ss_promo_sk,Store_Sales.ss_sold_date_sk,Store_Sales.ss_sold_date_sk
FROM Store_Sales where ss_sales_price=13

) ,
 tb11 AS (
SELECT Household_Demographics.hd_demo_sk,Household_Demographics.hd_income_band_sk,Household_Demographics.hd_buy_potential,Household_Demographics.hd_dep_count,Household_Demographics.hd_vehicle_count
FROM Household_Demographics where hd_dep_count=1

) ,
tb12 AS (
SELECT  tb10.ss_hdemo_sk, tb10.ss_promo_sk, tb10.ss_sold_date_sk, tb10.ss_sold_date_sk, tb11.hd_buy_potential, tb11.hd_vehicle_count, SUM(ss_sales_price) AS ss_sales_price_SUM, SUM(ss_ext_wholesale_cost) AS ss_ext_wholesale_cost_SUM
FROM tb10 , tb11
WHERE tb10.ss_hdemo_sk = tb11.hd_demo_sk
 GROUP BY tb10.ss_hdemo_sk,tb10.ss_promo_sk,tb10.ss_sold_date_sk,tb10.ss_sold_date_sk,tb11.hd_buy_potential,tb11.hd_vehicle_count) ,
 tb13 AS (
SELECT Promotion.p_promo_sk,Promotion.p_promo_id,Promotion.p_start_date_sk,Promotion.p_end_date_sk,Promotion.p_item_sk,Promotion.p_cost,Promotion.p_response_target,Promotion.p_promo_name,Promotion.p_channel_dmail,Promotion.p_channel_catalog,Promotion.p_channel_tv,Promotion.p_channel_radio,Promotion.p_channel_press,Promotion.p_channel_demo,Promotion.p_channel_details,Promotion.p_purpose,Promotion.p_discount_active,Promotion.p_channel_email,Promotion.p_channel_event
FROM Promotion where p_channel_event='N'

) ,
tb9 AS (
SELECT  tb3.ss_sold_date_sk, tb3.ss_sold_time_sk, tb3.ss_item_sk, tb3.ss_customer_sk, tb3.ss_cdemo_sk, tb3.ss_hdemo_sk, tb3.ss_addr_sk, tb3.ss_store_sk, tb3.ss_promo_sk, tb3.ss_ticket_number, tb8.sr_return_amt_SUM_SUM, SUM(ss_quantity) AS ss_quantity_SUM, MAX(ss_wholesale_cost) AS ss_wholesale_cost_MAX
FROM tb3 , tb8
WHERE tb3.ss_item_sk = tb8.sr_item_sk
 GROUP BY tb3.ss_sold_date_sk,tb3.ss_sold_time_sk,tb3.ss_item_sk,tb3.ss_customer_sk,tb3.ss_cdemo_sk,tb3.ss_hdemo_sk,tb3.ss_addr_sk,tb3.ss_store_sk,tb3.ss_promo_sk,tb3.ss_ticket_number,tb8.sr_return_amt_SUM_SUM) ,
 tb14 AS (
SELECT  tb12.ss_hdemo_sk, tb12.ss_promo_sk, tb12.ss_sold_date_sk, tb12.ss_sold_date_sk, tb13.p_promo_name, SUM(ss_sales_price_SUM) AS ss_sales_price_SUM_SUM
FROM tb12 , tb13
WHERE tb12.ss_promo_sk = tb13.p_promo_sk
 GROUP BY tb12.ss_hdemo_sk,tb12.ss_promo_sk,tb12.ss_sold_date_sk,tb12.ss_sold_date_sk,tb13.p_promo_name) ,
tb15 AS (
SELECT tb9.ss_sold_date_sk,tb9.ss_sold_time_sk,tb9.ss_item_sk,tb9.ss_customer_sk,tb9.ss_cdemo_sk,tb9.ss_hdemo_sk,tb9.ss_addr_sk,tb9.ss_store_sk,tb9.ss_promo_sk,tb9.ss_ticket_number, tb14.ss_sales_price_SUM_SUM
--        tb14.ss_hdemo_sk,tb14.ss_promo_sk,tb14.ss_sold_date_sk,tb14.ss_sold_date_sk
FROM tb9 , tb14
WHERE tb9.ss_sold_date_sk = tb14.ss_sold_date_sk
 ) ,
 tb16 AS (
SELECT Time_Dim.t_time_sk,Time_Dim.t_time_id,Time_Dim.t_time,Time_Dim.t_hour,Time_Dim.t_minute,Time_Dim.t_second,Time_Dim.t_am_pm,Time_Dim.t_shift,Time_Dim.t_sub_shift,Time_Dim.t_meal_time
FROM Time_Dim


)
SELECT tb15.ss_sold_date_sk,tb15.ss_sold_time_sk,tb15.ss_item_sk,tb15.ss_customer_sk,tb15.ss_cdemo_sk,tb15.ss_hdemo_sk,tb15.ss_addr_sk,tb15.ss_store_sk,tb15.ss_promo_sk,tb15.ss_ticket_number, tb16.t_time_sk,tb16.t_time_id,tb16.t_time,tb16.t_hour,tb16.t_minute,tb16.t_second,tb16.t_am_pm,tb16.t_shift,tb16.t_sub_shift,tb16.t_meal_time
FROM tb15 , tb16
WHERE tb15.ss_sold_time_sk = tb16.t_time_sk

 LIMIT 100;