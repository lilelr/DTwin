-- 46s

---new sql--2023-12-21 14:20:29-----
-- long_chain is ['FileScan parquetf', 'Filter', 'BroadcastHashJoin', 'Project', 'BroadcastHashJoin', 'Project', 'HashAggregate', 'TakeOrderedAndProject', '']
WITH
tb1 AS (
SELECT Store_Sales.ss_sold_date_sk,Store_Sales.ss_sold_time_sk,Store_Sales.ss_item_sk,Store_Sales.ss_customer_sk,Store_Sales.ss_cdemo_sk,Store_Sales.ss_hdemo_sk,Store_Sales.ss_addr_sk,Store_Sales.ss_store_sk,Store_Sales.ss_promo_sk,Store_Sales.ss_ticket_number,Store_Sales.ss_quantity,Store_Sales.ss_wholesale_cost,Store_Sales.ss_list_price,Store_Sales.ss_sales_price,Store_Sales.ss_ext_discount_amt,Store_Sales.ss_ext_sales_price,Store_Sales.ss_ext_wholesale_cost,Store_Sales.ss_ext_list_price,Store_Sales.ss_ext_tax,Store_Sales.ss_coupon_amt,Store_Sales.ss_net_paid,Store_Sales.ss_net_paid_inc_tax,Store_Sales.ss_net_profit
FROM Store_Sales where ss_ext_discount_amt=12

) ,
 tb2 AS (
SELECT Date_Dim.d_date_sk,Date_Dim.d_date_id,Date_Dim.d_date,Date_Dim.d_month_seq,Date_Dim.d_week_seq,Date_Dim.d_quarter_seq,Date_Dim.d_year,Date_Dim.d_dow,Date_Dim.d_moy,Date_Dim.d_dom,Date_Dim.d_qoy,Date_Dim.d_fy_year,Date_Dim.d_fy_quarter_seq,Date_Dim.d_fy_week_seq,Date_Dim.d_day_name ,Date_Dim.d_quarter_name ,Date_Dim.d_holiday ,Date_Dim.d_weekend,Date_Dim.d_following_holiday,Date_Dim.d_first_dom,Date_Dim.d_last_dom,Date_Dim.d_same_day_ly,Date_Dim.d_same_day_lq,Date_Dim.d_current_day,Date_Dim.d_current_week,Date_Dim.d_current_month ,Date_Dim.d_current_quarter,Date_Dim.d_current_year,Date_Dim.d_week_seq * 2 ,Date_Dim.d_year * 2 ,Date_Dim.d_dow * 2 ,Date_Dim.d_moy * 2 ,Date_Dim.d_dom * 2 ,Date_Dim.d_fy_quarter_seq * 2 ,Date_Dim.d_last_dom * 2
FROM Date_Dim where d_moy=6

) ,
tb3 AS (
SELECT tb1.ss_sold_date_sk,tb1.ss_sold_time_sk,tb1.ss_item_sk,tb1.ss_customer_sk,tb1.ss_cdemo_sk,tb1.ss_hdemo_sk,tb1.ss_addr_sk,tb1.ss_store_sk,tb1.ss_promo_sk,tb1.ss_ticket_number,tb1.ss_quantity,tb1.ss_wholesale_cost,tb1.ss_list_price,tb1.ss_sales_price,tb1.ss_ext_discount_amt,tb1.ss_ext_sales_price,tb1.ss_ext_wholesale_cost,tb1.ss_ext_list_price,tb1.ss_ext_tax,tb1.ss_coupon_amt,tb1.ss_net_paid,tb1.ss_net_paid_inc_tax,tb1.ss_net_profit , tb2.d_date_sk,tb2.d_date_id,tb2.d_date,tb2.d_month_seq,tb2.d_week_seq,tb2.d_quarter_seq,tb2.d_year,tb2.d_dow,tb2.d_moy,tb2.d_dom,tb2.d_qoy,tb2.d_fy_year,tb2.d_fy_quarter_seq,tb2.d_fy_week_seq,tb2.d_day_name ,tb2.d_quarter_name ,tb2.d_holiday ,tb2.d_weekend,tb2.d_following_holiday,tb2.d_first_dom,tb2.d_last_dom,tb2.d_same_day_ly,tb2.d_same_day_lq,tb2.d_current_day,tb2.d_current_week,tb2.d_current_month ,tb2.d_current_quarter,tb2.d_current_year,tb2.d_week_seq * 2 ,tb2.d_year * 2 ,tb2.d_dow * 2 ,tb2.d_moy * 2 ,tb2.d_dom * 2 ,tb2.d_fy_quarter_seq * 2 ,tb2.d_last_dom * 2
FROM tb1 , tb2
WHERE tb1.ss_sold_date_sk = tb2.d_date_sk
 ) ,
 tb4 AS (
SELECT Time_Dim.t_time_sk,Time_Dim.t_time_id,Time_Dim.t_time,Time_Dim.t_hour,Time_Dim.t_minute,Time_Dim.t_second,Time_Dim.t_am_pm,Time_Dim.t_shift,Time_Dim.t_sub_shift,Time_Dim.t_meal_time
FROM Time_Dim


)
SELECT  tb3.ss_sold_date_sk, tb3.ss_sold_time_sk, tb3.ss_item_sk, tb3.ss_customer_sk, tb3.ss_cdemo_sk, tb3.ss_hdemo_sk, tb3.ss_addr_sk, tb3.ss_store_sk, tb3.ss_promo_sk, tb3.ss_ticket_number, tb4.t_hour, tb4.t_minute, tb4.t_second, SUM(ss_quantity) AS ss_quantity_SUM
FROM tb3 , tb4
WHERE tb3.ss_sold_time_sk = tb4.t_time_sk
 GROUP BY tb3.ss_sold_date_sk,tb3.ss_sold_time_sk,tb3.ss_item_sk,tb3.ss_customer_sk,tb3.ss_cdemo_sk,tb3.ss_hdemo_sk,tb3.ss_addr_sk,tb3.ss_store_sk,tb3.ss_promo_sk,tb3.ss_ticket_number,tb4.t_hour,tb4.t_minute,tb4.t_second
 LIMIT 100;