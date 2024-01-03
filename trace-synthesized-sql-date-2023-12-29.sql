
---new sql--2023-12-29 15:08:51-----
--- long_chain is ['FileScan FactTable', 'Filter', 'Project', 'HashAggregate', 'Sort', 'SortMergeJoin', 'BroadcastHashJoin', 'Filter', 'sort', 'SortMergeJoin', 'SortMergeJoin', 'Project', 'HashAggregate', 'HashAggregate', '']
WITH 
tb1 AS (
SELECT Store_Returns.sr_returned_date_sk, Store_Returns.sr_return_time_sk, Store_Returns.sr_item_sk, Store_Returns.sr_customer_sk, Store_Returns.sr_cdemo_sk, Store_Returns.sr_hdemo_sk, Store_Returns.sr_addr_sk, Store_Returns.sr_store_sk, Store_Returns.sr_reason_sk, Store_Returns.sr_ticket_number, SUM(sr_return_quantity) AS sr_return_quantity_SUM, SUM(sr_return_amt) AS sr_return_amt_SUM
FROM Store_Returns where sr_return_quantity=300
 GROUP BY Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number
) , 
 tb2 AS (
SELECT Store_Sales.ss_sold_date_sk, Store_Sales.ss_sold_time_sk, Store_Sales.ss_item_sk, Store_Sales.ss_customer_sk, Store_Sales.ss_cdemo_sk, Store_Sales.ss_hdemo_sk, Store_Sales.ss_addr_sk, Store_Sales.ss_store_sk, Store_Sales.ss_promo_sk, Store_Sales.ss_ticket_number, MAX(cast(cast( ss_quantity as BIGINT)  as string)) AS ss_quantity_MAX
FROM Store_Sales where ss_ext_discount_amt=12
 GROUP BY Store_Sales.ss_sold_date_sk,Store_Sales.ss_sold_time_sk,Store_Sales.ss_item_sk,Store_Sales.ss_customer_sk,Store_Sales.ss_cdemo_sk,Store_Sales.ss_hdemo_sk,Store_Sales.ss_addr_sk,Store_Sales.ss_store_sk,Store_Sales.ss_promo_sk,Store_Sales.ss_ticket_number
) , 
tb3 AS (
SELECT tb1.sr_returned_date_sk,tb1.sr_return_time_sk,tb1.sr_item_sk,tb1.sr_customer_sk,tb1.sr_cdemo_sk,tb1.sr_hdemo_sk,tb1.sr_addr_sk,tb1.sr_store_sk,tb1.sr_reason_sk,tb1.sr_ticket_number,tb1.sr_return_quantity_SUM,tb1.sr_return_amt_SUM , tb2.ss_sold_date_sk,tb2.ss_sold_time_sk,tb2.ss_item_sk,tb2.ss_customer_sk,tb2.ss_cdemo_sk,tb2.ss_hdemo_sk,tb2.ss_addr_sk,tb2.ss_store_sk,tb2.ss_promo_sk,tb2.ss_ticket_number,tb2.ss_quantity_MAX
FROM tb1 , tb2
WHERE tb1.sr_item_sk = tb2.ss_item_sk and tb1.sr_ticket_number = tb2.ss_ticket_number and tb1.sr_customer_sk = tb2.ss_customer_sk
 ) , 
 tb4 AS (
SELECT Catalog_Sales.cs_sold_date_sk, Catalog_Sales.cs_sold_time_sk, Catalog_Sales.cs_ship_date_sk, Catalog_Sales.cs_bill_customer_sk, Catalog_Sales.cs_bill_cdemo_sk, Catalog_Sales.cs_bill_hdemo_sk, Catalog_Sales.cs_bill_addr_sk, Catalog_Sales.cs_ship_customer_sk, Catalog_Sales.cs_ship_cdemo_sk, Catalog_Sales.cs_ship_hdemo_sk, Catalog_Sales.cs_ship_addr_sk, Catalog_Sales.cs_call_center_sk, Catalog_Sales.cs_catalog_page_sk, Catalog_Sales.cs_ship_mode_sk, Catalog_Sales.cs_warehouse_sk, Catalog_Sales.cs_item_sk, Catalog_Sales.cs_promo_sk, Catalog_Sales.cs_order_number, SUM(cs_quantity) AS cs_quantity_SUM, COUNT(cs_wholesale_cost) AS cs_wholesale_cost_COUNT
FROM Catalog_Sales where cs_net_profit=100
 GROUP BY Catalog_Sales.cs_sold_date_sk,Catalog_Sales.cs_sold_time_sk,Catalog_Sales.cs_ship_date_sk,Catalog_Sales.cs_bill_customer_sk,Catalog_Sales.cs_bill_cdemo_sk,Catalog_Sales.cs_bill_hdemo_sk,Catalog_Sales.cs_bill_addr_sk,Catalog_Sales.cs_ship_customer_sk,Catalog_Sales.cs_ship_cdemo_sk,Catalog_Sales.cs_ship_hdemo_sk,Catalog_Sales.cs_ship_addr_sk,Catalog_Sales.cs_call_center_sk,Catalog_Sales.cs_catalog_page_sk,Catalog_Sales.cs_ship_mode_sk,Catalog_Sales.cs_warehouse_sk,Catalog_Sales.cs_item_sk,Catalog_Sales.cs_promo_sk,Catalog_Sales.cs_order_number
) , 
tb5 AS (
SELECT tb3.sr_returned_date_sk,tb3.sr_return_time_sk,tb3.sr_item_sk,tb3.sr_customer_sk,tb3.sr_cdemo_sk,tb3.sr_hdemo_sk,tb3.sr_addr_sk,tb3.sr_store_sk,tb3.sr_reason_sk,tb3.sr_ticket_number,tb3.sr_return_quantity_SUM,tb3.sr_return_amt_SUM , tb4.cs_sold_date_sk,tb4.cs_sold_time_sk,tb4.cs_ship_date_sk,tb4.cs_bill_customer_sk,tb4.cs_bill_cdemo_sk,tb4.cs_bill_hdemo_sk,tb4.cs_bill_addr_sk,tb4.cs_ship_customer_sk,tb4.cs_ship_cdemo_sk,tb4.cs_ship_hdemo_sk,tb4.cs_ship_addr_sk,tb4.cs_call_center_sk,tb4.cs_catalog_page_sk,tb4.cs_ship_mode_sk,tb4.cs_warehouse_sk,tb4.cs_item_sk,tb4.cs_promo_sk,tb4.cs_order_number,tb4.cs_quantity_SUM,tb4.cs_wholesale_cost_COUNT
FROM tb3 , tb4
WHERE tb3.sr_customer_sk = tb4.cs_bill_customer_sk and tb3.sr_item_sk = tb4.cs_item_sk
 ) , 
 tb6 AS (
SELECT Customer.c_customer_sk, Customer.c_current_cdemo_sk, Customer.c_current_hdemo_sk, Customer.c_current_addr_sk, Customer.c_first_shipto_date_sk, Customer.c_first_sales_date_sk, MAX(cast(cast( c_birth_day as BIGINT)  as string)) AS c_birth_day_MAX
FROM Customer 
 
 GROUP BY Customer.c_customer_sk,Customer.c_current_cdemo_sk,Customer.c_current_hdemo_sk,Customer.c_current_addr_sk,Customer.c_first_shipto_date_sk,Customer.c_first_sales_date_sk
) , 
tb7 AS (
SELECT tb5.sr_returned_date_sk,tb5.sr_return_time_sk,tb5.sr_item_sk,tb5.sr_customer_sk,tb5.sr_cdemo_sk,tb5.sr_hdemo_sk,tb5.sr_addr_sk,tb5.sr_store_sk,tb5.sr_reason_sk,tb5.sr_ticket_number,tb5.sr_return_quantity_SUM,tb5.sr_return_amt_SUM , tb6.c_customer_sk,tb6.c_current_cdemo_sk,tb6.c_current_hdemo_sk,tb6.c_current_addr_sk,tb6.c_first_shipto_date_sk,tb6.c_first_sales_date_sk,tb6.c_birth_day_MAX
FROM tb5 , tb6
WHERE tb5.sr_customer_sk = tb6.c_customer_sk
 ) , 
 tb8 AS (
SELECT Date_Dim.d_date_sk, MAX(cast(cast( d_month_seq as BIGINT)  as string)) AS d_month_seq_MAX, MAX(cast(cast( d_week_seq as BIGINT)  as string)) AS d_week_seq_MAX, MAX(cast(cast( d_quarter_seq as BIGINT)  as string)) AS d_quarter_seq_MAX
FROM Date_Dim where d_moy=6
 GROUP BY Date_Dim.d_date_sk
) 
SELECT  tb7.sr_returned_date_sk, tb7.sr_return_time_sk, tb7.sr_item_sk, tb7.sr_customer_sk, tb7.sr_cdemo_sk, tb7.sr_hdemo_sk, tb7.sr_addr_sk, tb7.sr_store_sk, tb7.sr_reason_sk, tb7.sr_ticket_number, tb8.d_week_seq_MAX, tb7.sr_returned_date_sk, tb7.sr_return_time_sk, tb7.sr_item_sk, tb7.sr_customer_sk, tb7.sr_cdemo_sk, tb7.sr_hdemo_sk, tb7.sr_addr_sk, tb7.sr_store_sk, tb7.sr_reason_sk, tb7.sr_ticket_number, tb8.d_week_seq_MAX, SUM(sr_return_quantity_SUM) AS sr_return_quantity_SUM_SUM, SUM(sr_return_amt_SUM) AS sr_return_amt_SUM_SUM, COUNT(sr_return_quantity_SUM) AS sr_return_quantity_SUM_COUNT, SUM(sr_return_amt_SUM) AS sr_return_amt_SUM_SUM
FROM tb7 , tb8
WHERE tb7.sr_returned_date_sk = tb8.d_date_sk
 GROUP BY tb7.sr_returned_date_sk,tb7.sr_return_time_sk,tb7.sr_item_sk,tb7.sr_customer_sk,tb7.sr_cdemo_sk,tb7.sr_hdemo_sk,tb7.sr_addr_sk,tb7.sr_store_sk,tb7.sr_reason_sk,tb7.sr_ticket_number,tb8.d_week_seq_MAX,tb7.sr_returned_date_sk,tb7.sr_return_time_sk,tb7.sr_item_sk,tb7.sr_customer_sk,tb7.sr_cdemo_sk,tb7.sr_hdemo_sk,tb7.sr_addr_sk,tb7.sr_store_sk,tb7.sr_reason_sk,tb7.sr_ticket_number,tb8.d_week_seq_MAX
 LIMIT 100;
---new sql--2023-12-29 15:08:51-----
--- long_chain is ['FileScan FactTable', 'Filter', 'BroadcastHashJoin', 'Project', 'BroadcastHashJoin', 'Filter', 'sort', 'SortMergeJoin', 'BroadcastHashJoin', 'Project', '']
WITH 
tb10 AS (
SELECT Store_Returns.sr_returned_date_sk, Store_Returns.sr_return_time_sk, Store_Returns.sr_item_sk, Store_Returns.sr_customer_sk, Store_Returns.sr_cdemo_sk, Store_Returns.sr_hdemo_sk, Store_Returns.sr_addr_sk, Store_Returns.sr_store_sk, Store_Returns.sr_reason_sk, Store_Returns.sr_ticket_number, SUM(sr_return_quantity) AS sr_return_quantity_SUM, SUM(sr_return_amt) AS sr_return_amt_SUM
FROM Store_Returns where sr_return_quantity=100
 GROUP BY Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number
) , 
 tb11 AS (
SELECT Store_Sales.ss_sold_date_sk, Store_Sales.ss_sold_time_sk, Store_Sales.ss_item_sk, Store_Sales.ss_customer_sk, Store_Sales.ss_cdemo_sk, Store_Sales.ss_hdemo_sk, Store_Sales.ss_addr_sk, Store_Sales.ss_store_sk, Store_Sales.ss_promo_sk, Store_Sales.ss_ticket_number, SUM(ss_quantity) AS ss_quantity_SUM, SUM(ss_wholesale_cost) AS ss_wholesale_cost_SUM
FROM Store_Sales where ss_list_price=13
 GROUP BY Store_Sales.ss_sold_date_sk,Store_Sales.ss_sold_time_sk,Store_Sales.ss_item_sk,Store_Sales.ss_customer_sk,Store_Sales.ss_cdemo_sk,Store_Sales.ss_hdemo_sk,Store_Sales.ss_addr_sk,Store_Sales.ss_store_sk,Store_Sales.ss_promo_sk,Store_Sales.ss_ticket_number
) , 
tb12 AS (
SELECT tb10.sr_returned_date_sk,tb10.sr_return_time_sk,tb10.sr_item_sk,tb10.sr_customer_sk,tb10.sr_cdemo_sk,tb10.sr_hdemo_sk,tb10.sr_addr_sk,tb10.sr_store_sk,tb10.sr_reason_sk,tb10.sr_ticket_number,tb10.sr_return_quantity_SUM,tb10.sr_return_amt_SUM , tb11.ss_sold_date_sk,tb11.ss_sold_time_sk,tb11.ss_item_sk,tb11.ss_customer_sk,tb11.ss_cdemo_sk,tb11.ss_hdemo_sk,tb11.ss_addr_sk,tb11.ss_store_sk,tb11.ss_promo_sk,tb11.ss_ticket_number,tb11.ss_quantity_SUM,tb11.ss_wholesale_cost_SUM
FROM tb10 , tb11
WHERE tb10.sr_customer_sk = tb11.ss_customer_sk and tb10.sr_item_sk = tb11.ss_item_sk and tb10.sr_ticket_number = tb11.ss_ticket_number
 ) , 
 tb13 AS (
SELECT Catalog_Sales.cs_sold_date_sk, Catalog_Sales.cs_sold_time_sk, Catalog_Sales.cs_ship_date_sk, Catalog_Sales.cs_bill_customer_sk, Catalog_Sales.cs_bill_cdemo_sk, Catalog_Sales.cs_bill_hdemo_sk, Catalog_Sales.cs_bill_addr_sk, Catalog_Sales.cs_ship_customer_sk, Catalog_Sales.cs_ship_cdemo_sk, Catalog_Sales.cs_ship_hdemo_sk, Catalog_Sales.cs_ship_addr_sk, Catalog_Sales.cs_call_center_sk, Catalog_Sales.cs_catalog_page_sk, Catalog_Sales.cs_ship_mode_sk, Catalog_Sales.cs_warehouse_sk, Catalog_Sales.cs_item_sk, Catalog_Sales.cs_promo_sk, Catalog_Sales.cs_order_number, SUM(cs_quantity) AS cs_quantity_SUM, SUM(cs_wholesale_cost) AS cs_wholesale_cost_SUM, COUNT(cs_list_price) AS cs_list_price_COUNT
FROM Catalog_Sales where cs_net_profit=100
 GROUP BY Catalog_Sales.cs_sold_date_sk,Catalog_Sales.cs_sold_time_sk,Catalog_Sales.cs_ship_date_sk,Catalog_Sales.cs_bill_customer_sk,Catalog_Sales.cs_bill_cdemo_sk,Catalog_Sales.cs_bill_hdemo_sk,Catalog_Sales.cs_bill_addr_sk,Catalog_Sales.cs_ship_customer_sk,Catalog_Sales.cs_ship_cdemo_sk,Catalog_Sales.cs_ship_hdemo_sk,Catalog_Sales.cs_ship_addr_sk,Catalog_Sales.cs_call_center_sk,Catalog_Sales.cs_catalog_page_sk,Catalog_Sales.cs_ship_mode_sk,Catalog_Sales.cs_warehouse_sk,Catalog_Sales.cs_item_sk,Catalog_Sales.cs_promo_sk,Catalog_Sales.cs_order_number
) , 
tb14 AS (
SELECT tb12.sr_returned_date_sk,tb12.sr_return_time_sk,tb12.sr_item_sk,tb12.sr_customer_sk,tb12.sr_cdemo_sk,tb12.sr_hdemo_sk,tb12.sr_addr_sk,tb12.sr_store_sk,tb12.sr_reason_sk,tb12.sr_ticket_number,tb12.sr_return_quantity_SUM,tb12.sr_return_amt_SUM , tb13.cs_sold_date_sk,tb13.cs_sold_time_sk,tb13.cs_ship_date_sk,tb13.cs_bill_customer_sk,tb13.cs_bill_cdemo_sk,tb13.cs_bill_hdemo_sk,tb13.cs_bill_addr_sk,tb13.cs_ship_customer_sk,tb13.cs_ship_cdemo_sk,tb13.cs_ship_hdemo_sk,tb13.cs_ship_addr_sk,tb13.cs_call_center_sk,tb13.cs_catalog_page_sk,tb13.cs_ship_mode_sk,tb13.cs_warehouse_sk,tb13.cs_item_sk,tb13.cs_promo_sk,tb13.cs_order_number,tb13.cs_quantity_SUM,tb13.cs_wholesale_cost_SUM,tb13.cs_list_price_COUNT
FROM tb12 , tb13
WHERE tb12.sr_customer_sk = tb13.cs_bill_customer_sk and tb12.sr_item_sk = tb13.cs_item_sk
 ) , 
 tb15 AS (
SELECT Customer.c_customer_sk, Customer.c_current_cdemo_sk, Customer.c_current_hdemo_sk, Customer.c_current_addr_sk, Customer.c_first_shipto_date_sk, Customer.c_first_sales_date_sk, MAX(cast(cast( c_birth_day as BIGINT)  as string)) AS c_birth_day_MAX, MAX(cast(cast( c_birth_month as BIGINT)  as string)) AS c_birth_month_MAX
FROM Customer 
 
 GROUP BY Customer.c_customer_sk,Customer.c_current_cdemo_sk,Customer.c_current_hdemo_sk,Customer.c_current_addr_sk,Customer.c_first_shipto_date_sk,Customer.c_first_sales_date_sk
) , 
tb16 AS (
SELECT tb14.sr_returned_date_sk,tb14.sr_return_time_sk,tb14.sr_item_sk,tb14.sr_customer_sk,tb14.sr_cdemo_sk,tb14.sr_hdemo_sk,tb14.sr_addr_sk,tb14.sr_store_sk,tb14.sr_reason_sk,tb14.sr_ticket_number,tb14.sr_return_quantity_SUM,tb14.sr_return_amt_SUM , tb15.c_customer_sk,tb15.c_current_cdemo_sk,tb15.c_current_hdemo_sk,tb15.c_current_addr_sk,tb15.c_first_shipto_date_sk,tb15.c_first_sales_date_sk,tb15.c_birth_day_MAX,tb15.c_birth_month_MAX
FROM tb14 , tb15
WHERE tb14.sr_customer_sk = tb15.c_customer_sk
 ) , 
 tb17 AS (
SELECT Date_Dim.d_date_sk, MAX(cast(cast( d_month_seq as BIGINT)  as string)) AS d_month_seq_MAX
FROM Date_Dim where d_moy=12
 GROUP BY Date_Dim.d_date_sk
) 
SELECT tb16.sr_returned_date_sk,tb16.sr_return_time_sk,tb16.sr_item_sk,tb16.sr_customer_sk,tb16.sr_cdemo_sk,tb16.sr_hdemo_sk,tb16.sr_addr_sk,tb16.sr_store_sk,tb16.sr_reason_sk,tb16.sr_ticket_number,tb16.sr_return_quantity_SUM,tb16.sr_return_amt_SUM , tb17.d_month_seq_MAX
FROM tb16 , tb17
WHERE tb16.sr_returned_date_sk = tb17.d_date_sk
 
 LIMIT 100;
---new sql--2023-12-29 15:08:51-----
--- long_chain is ['FileScan FactTable', 'Filter', 'Project', 'Sort', 'SortMergeJoin', 'BroadcastHashJoin', 'Project', 'BroadcastHashJoin', 'Project', 'BroadcastHashJoin', 'Project', '']
WITH 
tb19 AS (
SELECT Store_Returns.sr_returned_date_sk, Store_Returns.sr_return_time_sk, Store_Returns.sr_item_sk, Store_Returns.sr_customer_sk, Store_Returns.sr_cdemo_sk, Store_Returns.sr_hdemo_sk, Store_Returns.sr_addr_sk, Store_Returns.sr_store_sk, Store_Returns.sr_reason_sk, Store_Returns.sr_ticket_number, SUM(sr_return_quantity) AS sr_return_quantity_SUM, SUM(sr_return_amt) AS sr_return_amt_SUM
FROM Store_Returns where sr_return_amt=300
 GROUP BY Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number
) , 
 tb20 AS (
SELECT Store_Sales.ss_sold_date_sk, Store_Sales.ss_sold_time_sk, Store_Sales.ss_item_sk, Store_Sales.ss_customer_sk, Store_Sales.ss_cdemo_sk, Store_Sales.ss_hdemo_sk, Store_Sales.ss_addr_sk, Store_Sales.ss_store_sk, Store_Sales.ss_promo_sk, Store_Sales.ss_ticket_number, SUM(ss_quantity) AS ss_quantity_SUM, SUM(ss_wholesale_cost) AS ss_wholesale_cost_SUM
FROM Store_Sales where ss_sales_price=13
 GROUP BY Store_Sales.ss_sold_date_sk,Store_Sales.ss_sold_time_sk,Store_Sales.ss_item_sk,Store_Sales.ss_customer_sk,Store_Sales.ss_cdemo_sk,Store_Sales.ss_hdemo_sk,Store_Sales.ss_addr_sk,Store_Sales.ss_store_sk,Store_Sales.ss_promo_sk,Store_Sales.ss_ticket_number
) , 
tb21 AS (
SELECT tb19.sr_returned_date_sk,tb19.sr_return_time_sk,tb19.sr_item_sk,tb19.sr_customer_sk,tb19.sr_cdemo_sk,tb19.sr_hdemo_sk,tb19.sr_addr_sk,tb19.sr_store_sk,tb19.sr_reason_sk,tb19.sr_ticket_number,tb19.sr_return_quantity_SUM,tb19.sr_return_amt_SUM , tb20.ss_sold_date_sk,tb20.ss_sold_time_sk,tb20.ss_item_sk,tb20.ss_customer_sk,tb20.ss_cdemo_sk,tb20.ss_hdemo_sk,tb20.ss_addr_sk,tb20.ss_store_sk,tb20.ss_promo_sk,tb20.ss_ticket_number,tb20.ss_quantity_SUM,tb20.ss_wholesale_cost_SUM
FROM tb19 , tb20
WHERE tb19.sr_item_sk = tb20.ss_item_sk and tb19.sr_customer_sk = tb20.ss_customer_sk and tb19.sr_ticket_number = tb20.ss_ticket_number
 ) , 
 tb22 AS (
SELECT Catalog_Sales.cs_sold_date_sk, Catalog_Sales.cs_sold_time_sk, Catalog_Sales.cs_ship_date_sk, Catalog_Sales.cs_bill_customer_sk, Catalog_Sales.cs_bill_cdemo_sk, Catalog_Sales.cs_bill_hdemo_sk, Catalog_Sales.cs_bill_addr_sk, Catalog_Sales.cs_ship_customer_sk, Catalog_Sales.cs_ship_cdemo_sk, Catalog_Sales.cs_ship_hdemo_sk, Catalog_Sales.cs_ship_addr_sk, Catalog_Sales.cs_call_center_sk, Catalog_Sales.cs_catalog_page_sk, Catalog_Sales.cs_ship_mode_sk, Catalog_Sales.cs_warehouse_sk, Catalog_Sales.cs_item_sk, Catalog_Sales.cs_promo_sk, Catalog_Sales.cs_order_number, MAX(cast(cast( cs_quantity as BIGINT)  as string)) AS cs_quantity_MAX
FROM Catalog_Sales where cs_quantity=300
 GROUP BY Catalog_Sales.cs_sold_date_sk,Catalog_Sales.cs_sold_time_sk,Catalog_Sales.cs_ship_date_sk,Catalog_Sales.cs_bill_customer_sk,Catalog_Sales.cs_bill_cdemo_sk,Catalog_Sales.cs_bill_hdemo_sk,Catalog_Sales.cs_bill_addr_sk,Catalog_Sales.cs_ship_customer_sk,Catalog_Sales.cs_ship_cdemo_sk,Catalog_Sales.cs_ship_hdemo_sk,Catalog_Sales.cs_ship_addr_sk,Catalog_Sales.cs_call_center_sk,Catalog_Sales.cs_catalog_page_sk,Catalog_Sales.cs_ship_mode_sk,Catalog_Sales.cs_warehouse_sk,Catalog_Sales.cs_item_sk,Catalog_Sales.cs_promo_sk,Catalog_Sales.cs_order_number
) , 
tb23 AS (
SELECT tb21.sr_returned_date_sk,tb21.sr_return_time_sk,tb21.sr_item_sk,tb21.sr_customer_sk,tb21.sr_cdemo_sk,tb21.sr_hdemo_sk,tb21.sr_addr_sk,tb21.sr_store_sk,tb21.sr_reason_sk,tb21.sr_ticket_number,tb21.sr_return_quantity_SUM,tb21.sr_return_amt_SUM , tb22.cs_sold_date_sk,tb22.cs_sold_time_sk,tb22.cs_ship_date_sk,tb22.cs_bill_customer_sk,tb22.cs_bill_cdemo_sk,tb22.cs_bill_hdemo_sk,tb22.cs_bill_addr_sk,tb22.cs_ship_customer_sk,tb22.cs_ship_cdemo_sk,tb22.cs_ship_hdemo_sk,tb22.cs_ship_addr_sk,tb22.cs_call_center_sk,tb22.cs_catalog_page_sk,tb22.cs_ship_mode_sk,tb22.cs_warehouse_sk,tb22.cs_item_sk,tb22.cs_promo_sk,tb22.cs_order_number,tb22.cs_quantity_MAX
FROM tb21 , tb22
WHERE tb21.sr_item_sk = tb22.cs_item_sk and tb21.sr_customer_sk = tb22.cs_bill_customer_sk
 ) , 
 tb24 AS (
SELECT Customer.c_customer_sk, Customer.c_current_cdemo_sk, Customer.c_current_hdemo_sk, Customer.c_current_addr_sk, Customer.c_first_shipto_date_sk, Customer.c_first_sales_date_sk, MAX(cast(cast( c_birth_day as BIGINT)  as string)) AS c_birth_day_MAX
FROM Customer 
 
 GROUP BY Customer.c_customer_sk,Customer.c_current_cdemo_sk,Customer.c_current_hdemo_sk,Customer.c_current_addr_sk,Customer.c_first_shipto_date_sk,Customer.c_first_sales_date_sk
) , 
tb25 AS (
SELECT tb23.sr_returned_date_sk,tb23.sr_return_time_sk,tb23.sr_item_sk,tb23.sr_customer_sk,tb23.sr_cdemo_sk,tb23.sr_hdemo_sk,tb23.sr_addr_sk,tb23.sr_store_sk,tb23.sr_reason_sk,tb23.sr_ticket_number,tb23.sr_return_quantity_SUM,tb23.sr_return_amt_SUM , tb24.c_customer_sk,tb24.c_current_cdemo_sk,tb24.c_current_hdemo_sk,tb24.c_current_addr_sk,tb24.c_first_shipto_date_sk,tb24.c_first_sales_date_sk,tb24.c_birth_day_MAX
FROM tb23 , tb24
WHERE tb23.sr_customer_sk = tb24.c_customer_sk
 ) , 
 tb26 AS (
SELECT Date_Dim.d_date_sk, COUNT(d_month_seq) AS d_month_seq_COUNT, SUM(d_week_seq) AS d_week_seq_SUM
FROM Date_Dim where d_year=2002
 GROUP BY Date_Dim.d_date_sk
) 
SELECT tb25.sr_returned_date_sk,tb25.sr_return_time_sk,tb25.sr_item_sk,tb25.sr_customer_sk,tb25.sr_cdemo_sk,tb25.sr_hdemo_sk,tb25.sr_addr_sk,tb25.sr_store_sk,tb25.sr_reason_sk,tb25.sr_ticket_number,tb25.sr_return_quantity_SUM,tb25.sr_return_amt_SUM , tb26.d_month_seq_COUNT,tb26.d_week_seq_SUM
FROM tb25 , tb26
WHERE tb25.sr_returned_date_sk = tb26.d_date_sk
 
 LIMIT 100;