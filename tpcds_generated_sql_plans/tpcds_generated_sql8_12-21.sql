WITH
tb1 AS (
SELECT Store_Sales.ss_sold_date_sk,Store_Sales.ss_sold_time_sk,Store_Sales.ss_item_sk,Store_Sales.ss_customer_sk,Store_Sales.ss_cdemo_sk,Store_Sales.ss_hdemo_sk,Store_Sales.ss_addr_sk,Store_Sales.ss_store_sk,Store_Sales.ss_promo_sk,Store_Sales.ss_ticket_number,Store_Sales.ss_quantity,Store_Sales.ss_wholesale_cost,Store_Sales.ss_list_price,Store_Sales.ss_sales_price,Store_Sales.ss_ext_discount_amt,Store_Sales.ss_ext_sales_price,Store_Sales.ss_ext_wholesale_cost,Store_Sales.ss_ext_list_price,Store_Sales.ss_ext_tax,Store_Sales.ss_coupon_amt,Store_Sales.ss_net_paid,Store_Sales.ss_net_paid_inc_tax,Store_Sales.ss_net_profit
FROM Store_Sales where ss_list_price=13

) ,
 tb2 AS (
SELECT Date_Dim.d_date_sk,Date_Dim.d_date_id,Date_Dim.d_date,Date_Dim.d_month_seq,Date_Dim.d_week_seq,Date_Dim.d_quarter_seq,Date_Dim.d_year,Date_Dim.d_dow,Date_Dim.d_moy,Date_Dim.d_dom,Date_Dim.d_qoy,Date_Dim.d_fy_year,Date_Dim.d_fy_quarter_seq,Date_Dim.d_fy_week_seq,Date_Dim.d_day_name ,Date_Dim.d_quarter_name ,Date_Dim.d_holiday ,Date_Dim.d_weekend,Date_Dim.d_following_holiday,Date_Dim.d_first_dom,Date_Dim.d_last_dom,Date_Dim.d_same_day_ly,Date_Dim.d_same_day_lq,Date_Dim.d_current_day,Date_Dim.d_current_week,Date_Dim.d_current_month ,Date_Dim.d_current_quarter,Date_Dim.d_current_year
FROM Date_Dim


) ,
tb3 AS (
SELECT tb1.ss_sold_date_sk,tb1.ss_sold_time_sk,tb1.ss_item_sk,tb1.ss_customer_sk,tb1.ss_cdemo_sk,tb1.ss_hdemo_sk,tb1.ss_addr_sk,tb1.ss_store_sk,tb1.ss_promo_sk,tb1.ss_ticket_number,tb1.ss_quantity,tb1.ss_wholesale_cost,tb1.ss_list_price,tb1.ss_sales_price,tb1.ss_ext_discount_amt,tb1.ss_ext_sales_price,tb1.ss_ext_wholesale_cost,tb1.ss_ext_list_price,tb1.ss_ext_tax,tb1.ss_coupon_amt,tb1.ss_net_paid,tb1.ss_net_paid_inc_tax,tb1.ss_net_profit , tb2.d_date_sk,tb2.d_date_id,tb2.d_date,tb2.d_month_seq,tb2.d_week_seq,tb2.d_quarter_seq,tb2.d_year,tb2.d_dow,tb2.d_moy,tb2.d_dom,tb2.d_qoy,tb2.d_fy_year,tb2.d_fy_quarter_seq,tb2.d_fy_week_seq,tb2.d_day_name ,tb2.d_quarter_name ,tb2.d_holiday ,tb2.d_weekend,tb2.d_following_holiday,tb2.d_first_dom,tb2.d_last_dom,tb2.d_same_day_ly,tb2.d_same_day_lq,tb2.d_current_day,tb2.d_current_week,tb2.d_current_month ,tb2.d_current_quarter,tb2.d_current_year
FROM tb1 , tb2
WHERE tb1.ss_sold_date_sk = tb2.d_date_sk
 ) ,
 tb4 AS (
SELECT Time_Dim.t_time_sk,Time_Dim.t_time_id,Time_Dim.t_time,Time_Dim.t_hour,Time_Dim.t_minute,Time_Dim.t_second,Time_Dim.t_am_pm,Time_Dim.t_shift,Time_Dim.t_sub_shift,Time_Dim.t_meal_time
FROM Time_Dim


) ,
tb5 AS (
SELECT  tb3.ss_sold_date_sk, tb3.ss_sold_time_sk, tb3.ss_item_sk, tb3.ss_customer_sk, tb3.ss_cdemo_sk, tb3.ss_hdemo_sk, tb3.ss_addr_sk, tb3.ss_store_sk, tb3.ss_promo_sk, tb3.ss_ticket_number, tb4.t_time_sk, tb4.t_time_id, COUNT(ss_quantity) AS ss_quantity_COUNT
FROM tb3 , tb4
WHERE tb3.ss_sold_time_sk = tb4.t_time_sk
 GROUP BY tb3.ss_sold_date_sk,tb3.ss_sold_time_sk,tb3.ss_item_sk,tb3.ss_customer_sk,tb3.ss_cdemo_sk,tb3.ss_hdemo_sk,tb3.ss_addr_sk,tb3.ss_store_sk,tb3.ss_promo_sk,tb3.ss_ticket_number,tb4.t_time_sk,tb4.t_time_id) ,
 tb6 AS (
SELECT Item.i_item_sk,Item.i_item_id,Item.i_rec_start_date,Item.i_rec_end_date,Item.i_item_desc,Item.i_current_price,Item.i_wholesale_cost,Item.i_brand_id,Item.i_brand,Item.i_class_id,Item.i_class,Item.i_category_id,Item.i_category,Item.i_manufact_id,Item.i_manufact,Item.i_size,Item.i_formulation,Item.i_color,Item.i_units,Item.i_container,Item.i_manager_id,Item.i_product_name,Item.i_current_price * 2 ,Item.i_wholesale_cost * 2 ,Item.i_brand_id * 2 ,Item.i_manufact_id * 2 ,Item.i_manager_id * 2
FROM Item where i_manager_id=8

) ,
tb7 AS (
SELECT tb5.ss_sold_date_sk,tb5.ss_sold_time_sk,tb5.ss_item_sk,tb5.ss_customer_sk,tb5.ss_cdemo_sk,tb5.ss_hdemo_sk,tb5.ss_addr_sk,tb5.ss_store_sk,tb5.ss_promo_sk,tb5.ss_ticket_number, tb6.i_item_sk,tb6.i_item_id,tb6.i_rec_start_date,tb6.i_rec_end_date,tb6.i_item_desc,tb6.i_current_price,tb6.i_wholesale_cost,tb6.i_brand_id,tb6.i_brand,tb6.i_class_id,tb6.i_class,tb6.i_category_id,tb6.i_category,tb6.i_manufact_id,tb6.i_manufact,tb6.i_size,tb6.i_formulation,tb6.i_color,tb6.i_units,tb6.i_container,tb6.i_manager_id,tb6.i_product_name,tb6.i_current_price * 2 ,tb6.i_wholesale_cost * 2 ,tb6.i_brand_id * 2 ,tb6.i_manufact_id * 2 ,tb6.i_manager_id * 2
FROM tb5 , tb6
WHERE tb5.ss_item_sk = tb6.i_item_sk
 ) ,
 tb8 AS (
SELECT Customer.c_customer_sk,Customer.c_customer_id,Customer.c_current_cdemo_sk,Customer.c_current_hdemo_sk,Customer.c_current_addr_sk,Customer.c_first_shipto_date_sk,Customer.c_first_sales_date_sk,Customer.c_salutation,Customer.c_first_name,Customer.c_last_name,Customer.c_preferred_cust_flag,Customer.c_birth_day,Customer.c_birth_month,Customer.c_birth_year,Customer.c_birth_country,Customer.c_login,Customer.c_email_address,Customer.c_birth_month * 2
FROM Customer


) ,
tb9 AS (
SELECT tb7.ss_sold_date_sk,tb7.ss_sold_time_sk,tb7.ss_item_sk,tb7.ss_customer_sk,tb7.ss_cdemo_sk,tb7.ss_hdemo_sk,tb7.ss_addr_sk,tb7.ss_store_sk,tb7.ss_promo_sk,tb7.ss_ticket_number, tb8.c_customer_sk,tb8.c_customer_id,tb8.c_current_cdemo_sk,tb8.c_current_hdemo_sk,tb8.c_current_addr_sk,tb8.c_first_shipto_date_sk,tb8.c_first_sales_date_sk,tb8.c_salutation,tb8.c_first_name,tb8.c_last_name,tb8.c_preferred_cust_flag,tb8.c_birth_day,tb8.c_birth_month,tb8.c_birth_year,tb8.c_birth_country,tb8.c_login,tb8.c_email_address,tb8.c_birth_month * 2
FROM tb7 , tb8
WHERE tb7.ss_customer_sk = tb8.c_customer_sk
 ) ,
 tb10 AS (
SELECT Customer_Demographics.cd_demo_sk,Customer_Demographics.cd_gender,Customer_Demographics.cd_marital_status,Customer_Demographics.cd_education_status,Customer_Demographics.cd_purchase_estimate,Customer_Demographics.cd_credit_rating,Customer_Demographics.cd_dep_count,Customer_Demographics.cd_dep_employed_count,Customer_Demographics.cd_dep_college_count,Customer_Demographics.cd_purchase_estimate * 2 ,Customer_Demographics.cd_dep_employed_count * 2
FROM Customer_Demographics where cd_gender='F'

) ,
tb11 AS (
SELECT tb9.ss_sold_date_sk,tb9.ss_sold_time_sk,tb9.ss_item_sk,tb9.ss_customer_sk,tb9.ss_cdemo_sk,tb9.ss_hdemo_sk,tb9.ss_addr_sk,tb9.ss_store_sk,tb9.ss_promo_sk,tb9.ss_ticket_number, tb10.cd_demo_sk,tb10.cd_gender,tb10.cd_marital_status,tb10.cd_education_status,tb10.cd_purchase_estimate,tb10.cd_credit_rating,tb10.cd_dep_count,tb10.cd_dep_employed_count,tb10.cd_dep_college_count,tb10.cd_purchase_estimate * 2 ,tb10.cd_dep_employed_count * 2
FROM tb9 , tb10
WHERE tb9.ss_cdemo_sk = tb10.cd_demo_sk
 ) ,
 tb12 AS (
SELECT Household_Demographics.hd_demo_sk,Household_Demographics.hd_income_band_sk,Household_Demographics.hd_buy_potential,Household_Demographics.hd_dep_count,Household_Demographics.hd_vehicle_count
FROM Household_Demographics


)
SELECT  tb11.ss_sold_date_sk, tb11.ss_sold_time_sk, tb11.ss_item_sk, tb11.ss_customer_sk, tb11.ss_cdemo_sk, tb11.ss_hdemo_sk, tb11.ss_addr_sk, tb11.ss_store_sk, tb11.ss_promo_sk, tb11.ss_ticket_number, tb12.hd_dep_count, AVG(cd_credit_rating) AS ss_quantity_AVG
FROM tb11 , tb12
WHERE tb11.ss_hdemo_sk = tb12.hd_demo_sk
 GROUP BY tb11.ss_sold_date_sk,tb11.ss_sold_time_sk,tb11.ss_item_sk,tb11.ss_customer_sk,tb11.ss_cdemo_sk,tb11.ss_hdemo_sk,tb11.ss_addr_sk,tb11.ss_store_sk,tb11.ss_promo_sk,tb11.ss_ticket_number,tb12.hd_dep_count
 ;