
---new sql--2023-12-21 15:42:21----- 41 s
-- long_chain is ['FileScan parquetf', 'Filter', 'BroadcastHashJoin', 'Project', 'BroadcastHashJoin', 'Project', 'HashAggregate', 'Sort', 'Window', 'Project', 'sort', 'SortMergeJoin', 'Project', 'TakeOrderedAndProject', '']

WITH
tb1 AS (
SELECT Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number,Store_Returns.sr_return_quantity,Store_Returns.sr_return_amt,Store_Returns.sr_return_tax,Store_Returns.sr_return_amt_inc_tax,Store_Returns.sr_fee,Store_Returns.sr_return_ship_cost,Store_Returns.sr_refunded_cash,Store_Returns.sr_reversed_charge,Store_Returns.sr_store_credit,Store_Returns.sr_net_loss
FROM Store_Returns where sr_return_amt=200

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
tb5 AS (
SELECT  tb3.sr_returned_date_sk, tb3.sr_return_time_sk, tb3.sr_item_sk, tb3.sr_customer_sk, tb3.sr_cdemo_sk, tb3.sr_hdemo_sk, tb3.sr_addr_sk, tb3.sr_store_sk, tb3.sr_reason_sk, tb3.sr_ticket_number, tb4.t_time_sk, tb4.t_second, SUM(sr_return_quantity) AS sr_return_quantity_SUM, MAX(sr_return_amt) AS sr_return_amt_MAX, AVG(sr_return_tax) AS sr_return_tax_AVG
FROM tb3 , tb4
WHERE tb3.sr_return_time_sk = tb4.t_time_sk
 GROUP BY tb3.sr_returned_date_sk,tb3.sr_return_time_sk,tb3.sr_item_sk,tb3.sr_customer_sk,tb3.sr_cdemo_sk,tb3.sr_hdemo_sk,tb3.sr_addr_sk,tb3.sr_store_sk,tb3.sr_reason_sk,tb3.sr_ticket_number,tb4.t_time_sk,tb4.t_second) ,
 tb6 AS (
SELECT Item.i_item_sk,Item.i_item_id,Item.i_rec_start_date,Item.i_rec_end_date,Item.i_item_desc,Item.i_current_price,Item.i_wholesale_cost,Item.i_brand_id,Item.i_brand,Item.i_class_id,Item.i_class,Item.i_category_id,Item.i_category,Item.i_manufact_id,Item.i_manufact,Item.i_size,Item.i_formulation,Item.i_color,Item.i_units,Item.i_container,Item.i_manager_id,Item.i_product_name,Item.i_current_price * 2 ,Item.i_wholesale_cost * 2 ,Item.i_brand_id * 2 ,Item.i_manager_id * 2
FROM Item where i_category='Books'

)
SELECT tb5.sr_returned_date_sk,tb5.sr_return_time_sk,tb5.sr_item_sk,tb5.sr_customer_sk,tb5.sr_cdemo_sk,tb5.sr_hdemo_sk,tb5.sr_addr_sk,tb5.sr_store_sk,tb5.sr_reason_sk,tb5.sr_ticket_number, tb6.i_item_sk,tb6.i_item_id,tb6.i_rec_start_date,tb6.i_rec_end_date,tb6.i_item_desc,tb6.i_current_price,tb6.i_wholesale_cost,tb6.i_brand_id,tb6.i_brand,tb6.i_class_id,tb6.i_class,tb6.i_category_id,tb6.i_category,tb6.i_manufact_id,tb6.i_manufact,tb6.i_size,tb6.i_formulation,tb6.i_color,tb6.i_units,tb6.i_container,tb6.i_manager_id,tb6.i_product_name,tb6.i_current_price * 2 ,tb6.i_wholesale_cost * 2 ,tb6.i_brand_id * 2 ,tb6.i_manager_id * 2 , row_number() over (partition by tb6.i_size order by tb6.i_manager_id ) as r1
FROM tb5 , tb6
WHERE tb5.sr_item_sk = tb6.i_item_sk

 LIMIT 100;