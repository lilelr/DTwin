
---new sql--2024-01-03 14:36:28-----
--- long_chain is ['FileScan FactTable', 'Filter', 'BroadcastHashJoin', 'Project', 'BroadcastHashJoin', 'Project', '']
WITH 
tb1 AS (
SELECT Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number,Store_Returns.sr_return_quantity,Store_Returns.sr_return_amt,Store_Returns.sr_return_tax,Store_Returns.sr_return_amt_inc_tax,Store_Returns.sr_fee,Store_Returns.sr_return_ship_cost,Store_Returns.sr_refunded_cash,Store_Returns.sr_reversed_charge,Store_Returns.sr_store_credit,Store_Returns.sr_net_loss
FROM Store_Returns where sr_return_quantity=300
 
) , 
 tb2 AS (
SELECT Store_Sales.ss_sold_date_sk,Store_Sales.ss_sold_time_sk,Store_Sales.ss_item_sk,Store_Sales.ss_customer_sk,Store_Sales.ss_cdemo_sk,Store_Sales.ss_hdemo_sk,Store_Sales.ss_addr_sk,Store_Sales.ss_store_sk,Store_Sales.ss_promo_sk,Store_Sales.ss_ticket_number,Store_Sales.ss_quantity,Store_Sales.ss_wholesale_cost,Store_Sales.ss_list_price,Store_Sales.ss_sales_price,Store_Sales.ss_ext_discount_amt,Store_Sales.ss_ext_sales_price,Store_Sales.ss_ext_wholesale_cost,Store_Sales.ss_ext_list_price,Store_Sales.ss_ext_tax,Store_Sales.ss_coupon_amt,Store_Sales.ss_net_paid,Store_Sales.ss_net_paid_inc_tax,Store_Sales.ss_net_profit
FROM Store_Sales 
 
 
) , 
tb3 AS (
SELECT tb1.sr_returned_date_sk,tb1.sr_return_time_sk,tb1.sr_item_sk,tb1.sr_customer_sk,tb1.sr_cdemo_sk,tb1.sr_hdemo_sk,tb1.sr_addr_sk,tb1.sr_store_sk,tb1.sr_reason_sk,tb1.sr_ticket_number,tb1.sr_return_quantity,tb1.sr_return_amt,tb1.sr_return_tax,tb1.sr_return_amt_inc_tax,tb1.sr_fee,tb1.sr_return_ship_cost,tb1.sr_refunded_cash,tb1.sr_reversed_charge,tb1.sr_store_credit,tb1.sr_net_loss , tb2.ss_sold_date_sk,tb2.ss_sold_time_sk,tb2.ss_item_sk,tb2.ss_customer_sk,tb2.ss_cdemo_sk,tb2.ss_hdemo_sk,tb2.ss_addr_sk,tb2.ss_store_sk,tb2.ss_promo_sk,tb2.ss_ticket_number,tb2.ss_quantity,tb2.ss_wholesale_cost,tb2.ss_list_price,tb2.ss_sales_price,tb2.ss_ext_discount_amt,tb2.ss_ext_sales_price,tb2.ss_ext_wholesale_cost,tb2.ss_ext_list_price,tb2.ss_ext_tax,tb2.ss_coupon_amt,tb2.ss_net_paid,tb2.ss_net_paid_inc_tax,tb2.ss_net_profit
FROM tb1 , tb2
WHERE tb1.sr_customer_sk = tb2.ss_customer_sk
 ) , 
 tb4 AS (
SELECT Catalog_Sales.cs_sold_date_sk, Catalog_Sales.cs_sold_time_sk, Catalog_Sales.cs_ship_date_sk, Catalog_Sales.cs_bill_customer_sk, Catalog_Sales.cs_bill_cdemo_sk, Catalog_Sales.cs_bill_hdemo_sk, Catalog_Sales.cs_bill_addr_sk, Catalog_Sales.cs_ship_customer_sk, Catalog_Sales.cs_ship_cdemo_sk, Catalog_Sales.cs_ship_hdemo_sk, Catalog_Sales.cs_ship_addr_sk, Catalog_Sales.cs_call_center_sk, Catalog_Sales.cs_catalog_page_sk, Catalog_Sales.cs_ship_mode_sk, Catalog_Sales.cs_warehouse_sk, Catalog_Sales.cs_item_sk, Catalog_Sales.cs_promo_sk, Catalog_Sales.cs_order_number, SUM(cs_quantity) AS cs_quantity_SUM, SUM(cs_wholesale_cost) AS cs_wholesale_cost_SUM, MAX(cs_list_price) AS cs_list_price_MAX
FROM Catalog_Sales where cs_net_profit=300
 GROUP BY Catalog_Sales.cs_sold_date_sk,Catalog_Sales.cs_sold_time_sk,Catalog_Sales.cs_ship_date_sk,Catalog_Sales.cs_bill_customer_sk,Catalog_Sales.cs_bill_cdemo_sk,Catalog_Sales.cs_bill_hdemo_sk,Catalog_Sales.cs_bill_addr_sk,Catalog_Sales.cs_ship_customer_sk,Catalog_Sales.cs_ship_cdemo_sk,Catalog_Sales.cs_ship_hdemo_sk,Catalog_Sales.cs_ship_addr_sk,Catalog_Sales.cs_call_center_sk,Catalog_Sales.cs_catalog_page_sk,Catalog_Sales.cs_ship_mode_sk,Catalog_Sales.cs_warehouse_sk,Catalog_Sales.cs_item_sk,Catalog_Sales.cs_promo_sk,Catalog_Sales.cs_order_number
) 
SELECT tb3.sr_returned_date_sk,tb3.sr_return_time_sk,tb3.sr_item_sk,tb3.sr_customer_sk,tb3.sr_cdemo_sk,tb3.sr_hdemo_sk,tb3.sr_addr_sk,tb3.sr_store_sk,tb3.sr_reason_sk,tb3.sr_ticket_number,tb3.sr_return_quantity,tb3.sr_return_amt,tb3.sr_return_tax,tb3.sr_return_amt_inc_tax,tb3.sr_fee,tb3.sr_return_ship_cost,tb3.sr_refunded_cash,tb3.sr_reversed_charge,tb3.sr_store_credit,tb3.sr_net_loss , tb4.cs_sold_date_sk,tb4.cs_sold_time_sk,tb4.cs_ship_date_sk,tb4.cs_bill_customer_sk,tb4.cs_bill_cdemo_sk,tb4.cs_bill_hdemo_sk,tb4.cs_bill_addr_sk,tb4.cs_ship_customer_sk,tb4.cs_ship_cdemo_sk,tb4.cs_ship_hdemo_sk,tb4.cs_ship_addr_sk,tb4.cs_call_center_sk,tb4.cs_catalog_page_sk,tb4.cs_ship_mode_sk,tb4.cs_warehouse_sk,tb4.cs_item_sk,tb4.cs_promo_sk,tb4.cs_order_number,tb4.cs_quantity_SUM,tb4.cs_wholesale_cost_SUM,tb4.cs_list_price_MAX
FROM tb3 , tb4
WHERE tb3.sr_item_sk = tb4.cs_item_sk and tb3.sr_customer_sk = tb4.cs_bill_customer_sk
 
 LIMIT 100;
---new sql--2024-01-03 14:36:28-----
--- long_chain is ['FileScan FactTable', 'Filter', 'BroadcastHashJoin', 'Project', 'BroadcastHashJoin', 'Project', '']
WITH 
tb6 AS (
SELECT Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number,Store_Returns.sr_return_quantity,Store_Returns.sr_return_amt,Store_Returns.sr_return_tax,Store_Returns.sr_return_amt_inc_tax,Store_Returns.sr_fee,Store_Returns.sr_return_ship_cost,Store_Returns.sr_refunded_cash,Store_Returns.sr_reversed_charge,Store_Returns.sr_store_credit,Store_Returns.sr_net_loss
FROM Store_Returns where sr_return_quantity=300
 
) , 
 tb7 AS (
SELECT Store_Sales.ss_sold_date_sk, Store_Sales.ss_sold_time_sk, Store_Sales.ss_item_sk, Store_Sales.ss_customer_sk, Store_Sales.ss_cdemo_sk, Store_Sales.ss_hdemo_sk, Store_Sales.ss_addr_sk, Store_Sales.ss_store_sk, Store_Sales.ss_promo_sk, Store_Sales.ss_ticket_number, MAX(cast(cast( ss_quantity as BIGINT)  as string)) AS ss_quantity_MAX, MAX(cast(cast( ss_wholesale_cost as BIGINT)  as string)) AS ss_wholesale_cost_MAX, MAX(cast(cast( ss_list_price as BIGINT)  as string)) AS ss_list_price_MAX
FROM Store_Sales where ss_list_price=13
 GROUP BY Store_Sales.ss_sold_date_sk,Store_Sales.ss_sold_time_sk,Store_Sales.ss_item_sk,Store_Sales.ss_customer_sk,Store_Sales.ss_cdemo_sk,Store_Sales.ss_hdemo_sk,Store_Sales.ss_addr_sk,Store_Sales.ss_store_sk,Store_Sales.ss_promo_sk,Store_Sales.ss_ticket_number
) , 
tb8 AS (
SELECT tb6.sr_returned_date_sk,tb6.sr_return_time_sk,tb6.sr_item_sk,tb6.sr_customer_sk,tb6.sr_cdemo_sk,tb6.sr_hdemo_sk,tb6.sr_addr_sk,tb6.sr_store_sk,tb6.sr_reason_sk,tb6.sr_ticket_number,tb6.sr_return_quantity,tb6.sr_return_amt,tb6.sr_return_tax,tb6.sr_return_amt_inc_tax,tb6.sr_fee,tb6.sr_return_ship_cost,tb6.sr_refunded_cash,tb6.sr_reversed_charge,tb6.sr_store_credit,tb6.sr_net_loss , tb7.ss_sold_date_sk,tb7.ss_sold_time_sk,tb7.ss_item_sk,tb7.ss_customer_sk,tb7.ss_cdemo_sk,tb7.ss_hdemo_sk,tb7.ss_addr_sk,tb7.ss_store_sk,tb7.ss_promo_sk,tb7.ss_ticket_number,tb7.ss_quantity_MAX,tb7.ss_wholesale_cost_MAX,tb7.ss_list_price_MAX
FROM tb6 , tb7
WHERE tb6.sr_ticket_number = tb7.ss_ticket_number
 ) , 
 tb9 AS (
SELECT Catalog_Sales.cs_sold_date_sk, Catalog_Sales.cs_sold_time_sk, Catalog_Sales.cs_ship_date_sk, Catalog_Sales.cs_bill_customer_sk, Catalog_Sales.cs_bill_cdemo_sk, Catalog_Sales.cs_bill_hdemo_sk, Catalog_Sales.cs_bill_addr_sk, Catalog_Sales.cs_ship_customer_sk, Catalog_Sales.cs_ship_cdemo_sk, Catalog_Sales.cs_ship_hdemo_sk, Catalog_Sales.cs_ship_addr_sk, Catalog_Sales.cs_call_center_sk, Catalog_Sales.cs_catalog_page_sk, Catalog_Sales.cs_ship_mode_sk, Catalog_Sales.cs_warehouse_sk, Catalog_Sales.cs_item_sk, Catalog_Sales.cs_promo_sk, Catalog_Sales.cs_order_number, SUM(cs_quantity) AS cs_quantity_SUM, SUM(cs_wholesale_cost) AS cs_wholesale_cost_SUM, SUM(cs_list_price) AS cs_list_price_SUM
FROM Catalog_Sales where cs_quantity=100
 GROUP BY Catalog_Sales.cs_sold_date_sk,Catalog_Sales.cs_sold_time_sk,Catalog_Sales.cs_ship_date_sk,Catalog_Sales.cs_bill_customer_sk,Catalog_Sales.cs_bill_cdemo_sk,Catalog_Sales.cs_bill_hdemo_sk,Catalog_Sales.cs_bill_addr_sk,Catalog_Sales.cs_ship_customer_sk,Catalog_Sales.cs_ship_cdemo_sk,Catalog_Sales.cs_ship_hdemo_sk,Catalog_Sales.cs_ship_addr_sk,Catalog_Sales.cs_call_center_sk,Catalog_Sales.cs_catalog_page_sk,Catalog_Sales.cs_ship_mode_sk,Catalog_Sales.cs_warehouse_sk,Catalog_Sales.cs_item_sk,Catalog_Sales.cs_promo_sk,Catalog_Sales.cs_order_number
) 
SELECT tb8.sr_returned_date_sk,tb8.sr_return_time_sk,tb8.sr_item_sk,tb8.sr_customer_sk,tb8.sr_cdemo_sk,tb8.sr_hdemo_sk,tb8.sr_addr_sk,tb8.sr_store_sk,tb8.sr_reason_sk,tb8.sr_ticket_number,tb8.sr_return_quantity,tb8.sr_return_amt,tb8.sr_return_tax,tb8.sr_return_amt_inc_tax,tb8.sr_fee,tb8.sr_return_ship_cost,tb8.sr_refunded_cash,tb8.sr_reversed_charge,tb8.sr_store_credit,tb8.sr_net_loss , tb9.cs_sold_date_sk,tb9.cs_sold_time_sk,tb9.cs_ship_date_sk,tb9.cs_bill_customer_sk,tb9.cs_bill_cdemo_sk,tb9.cs_bill_hdemo_sk,tb9.cs_bill_addr_sk,tb9.cs_ship_customer_sk,tb9.cs_ship_cdemo_sk,tb9.cs_ship_hdemo_sk,tb9.cs_ship_addr_sk,tb9.cs_call_center_sk,tb9.cs_catalog_page_sk,tb9.cs_ship_mode_sk,tb9.cs_warehouse_sk,tb9.cs_item_sk,tb9.cs_promo_sk,tb9.cs_order_number,tb9.cs_quantity_SUM,tb9.cs_wholesale_cost_SUM,tb9.cs_list_price_SUM
FROM tb8 , tb9
WHERE tb8.sr_item_sk = tb9.cs_item_sk and tb8.sr_customer_sk = tb9.cs_bill_customer_sk
 
 LIMIT 100;
---new sql--2024-01-03 14:36:28-----
--- long_chain is ['FileScan FactTable', 'Filter', 'BroadcastHashJoin', 'Project', 'BroadcastHashJoin', 'Project', '']
WITH 
tb11 AS (
SELECT Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number,Store_Returns.sr_return_quantity,Store_Returns.sr_return_amt,Store_Returns.sr_return_tax,Store_Returns.sr_return_amt_inc_tax,Store_Returns.sr_fee,Store_Returns.sr_return_ship_cost,Store_Returns.sr_refunded_cash,Store_Returns.sr_reversed_charge,Store_Returns.sr_store_credit,Store_Returns.sr_net_loss
FROM Store_Returns where sr_return_amt=200
 
) , 
 tb12 AS (
SELECT Store_Sales.ss_sold_date_sk, Store_Sales.ss_sold_time_sk, Store_Sales.ss_item_sk, Store_Sales.ss_customer_sk, Store_Sales.ss_cdemo_sk, Store_Sales.ss_hdemo_sk, Store_Sales.ss_addr_sk, Store_Sales.ss_store_sk, Store_Sales.ss_promo_sk, Store_Sales.ss_ticket_number, MAX(cast(cast( ss_quantity as BIGINT)  as string)) AS ss_quantity_MAX, MAX(cast(cast( ss_wholesale_cost as BIGINT)  as string)) AS ss_wholesale_cost_MAX
FROM Store_Sales where ss_sales_price=13
 GROUP BY Store_Sales.ss_sold_date_sk,Store_Sales.ss_sold_time_sk,Store_Sales.ss_item_sk,Store_Sales.ss_customer_sk,Store_Sales.ss_cdemo_sk,Store_Sales.ss_hdemo_sk,Store_Sales.ss_addr_sk,Store_Sales.ss_store_sk,Store_Sales.ss_promo_sk,Store_Sales.ss_ticket_number
) , 
tb13 AS (
SELECT tb11.sr_returned_date_sk,tb11.sr_return_time_sk,tb11.sr_item_sk,tb11.sr_customer_sk,tb11.sr_cdemo_sk,tb11.sr_hdemo_sk,tb11.sr_addr_sk,tb11.sr_store_sk,tb11.sr_reason_sk,tb11.sr_ticket_number,tb11.sr_return_quantity,tb11.sr_return_amt,tb11.sr_return_tax,tb11.sr_return_amt_inc_tax,tb11.sr_fee,tb11.sr_return_ship_cost,tb11.sr_refunded_cash,tb11.sr_reversed_charge,tb11.sr_store_credit,tb11.sr_net_loss , tb12.ss_sold_date_sk,tb12.ss_sold_time_sk,tb12.ss_item_sk,tb12.ss_customer_sk,tb12.ss_cdemo_sk,tb12.ss_hdemo_sk,tb12.ss_addr_sk,tb12.ss_store_sk,tb12.ss_promo_sk,tb12.ss_ticket_number,tb12.ss_quantity_MAX,tb12.ss_wholesale_cost_MAX
FROM tb11 , tb12
WHERE tb11.sr_customer_sk = tb12.ss_customer_sk
 ) , 
 tb14 AS (
SELECT Catalog_Sales.cs_sold_date_sk, Catalog_Sales.cs_sold_time_sk, Catalog_Sales.cs_ship_date_sk, Catalog_Sales.cs_bill_customer_sk, Catalog_Sales.cs_bill_cdemo_sk, Catalog_Sales.cs_bill_hdemo_sk, Catalog_Sales.cs_bill_addr_sk, Catalog_Sales.cs_ship_customer_sk, Catalog_Sales.cs_ship_cdemo_sk, Catalog_Sales.cs_ship_hdemo_sk, Catalog_Sales.cs_ship_addr_sk, Catalog_Sales.cs_call_center_sk, Catalog_Sales.cs_catalog_page_sk, Catalog_Sales.cs_ship_mode_sk, Catalog_Sales.cs_warehouse_sk, Catalog_Sales.cs_item_sk, Catalog_Sales.cs_promo_sk, Catalog_Sales.cs_order_number, SUM(cs_quantity) AS cs_quantity_SUM, SUM(cs_wholesale_cost) AS cs_wholesale_cost_SUM
FROM Catalog_Sales where cs_net_profit=100
 GROUP BY Catalog_Sales.cs_sold_date_sk,Catalog_Sales.cs_sold_time_sk,Catalog_Sales.cs_ship_date_sk,Catalog_Sales.cs_bill_customer_sk,Catalog_Sales.cs_bill_cdemo_sk,Catalog_Sales.cs_bill_hdemo_sk,Catalog_Sales.cs_bill_addr_sk,Catalog_Sales.cs_ship_customer_sk,Catalog_Sales.cs_ship_cdemo_sk,Catalog_Sales.cs_ship_hdemo_sk,Catalog_Sales.cs_ship_addr_sk,Catalog_Sales.cs_call_center_sk,Catalog_Sales.cs_catalog_page_sk,Catalog_Sales.cs_ship_mode_sk,Catalog_Sales.cs_warehouse_sk,Catalog_Sales.cs_item_sk,Catalog_Sales.cs_promo_sk,Catalog_Sales.cs_order_number
) 
SELECT tb13.sr_returned_date_sk,tb13.sr_return_time_sk,tb13.sr_item_sk,tb13.sr_customer_sk,tb13.sr_cdemo_sk,tb13.sr_hdemo_sk,tb13.sr_addr_sk,tb13.sr_store_sk,tb13.sr_reason_sk,tb13.sr_ticket_number,tb13.sr_return_quantity,tb13.sr_return_amt,tb13.sr_return_tax,tb13.sr_return_amt_inc_tax,tb13.sr_fee,tb13.sr_return_ship_cost,tb13.sr_refunded_cash,tb13.sr_reversed_charge,tb13.sr_store_credit,tb13.sr_net_loss , tb14.cs_sold_date_sk,tb14.cs_sold_time_sk,tb14.cs_ship_date_sk,tb14.cs_bill_customer_sk,tb14.cs_bill_cdemo_sk,tb14.cs_bill_hdemo_sk,tb14.cs_bill_addr_sk,tb14.cs_ship_customer_sk,tb14.cs_ship_cdemo_sk,tb14.cs_ship_hdemo_sk,tb14.cs_ship_addr_sk,tb14.cs_call_center_sk,tb14.cs_catalog_page_sk,tb14.cs_ship_mode_sk,tb14.cs_warehouse_sk,tb14.cs_item_sk,tb14.cs_promo_sk,tb14.cs_order_number,tb14.cs_quantity_SUM,tb14.cs_wholesale_cost_SUM
FROM tb13 , tb14
WHERE tb13.sr_item_sk = tb14.cs_item_sk
 
 LIMIT 100;
---new sql--2024-01-03 14:36:28-----
--- long_chain is ['FileScan FactTable', 'Filter', 'Project', 'HashAggregate', 'BroadcastHashJoin', 'Project', 'BroadcastHashJoin', 'Project', 'Sort', 'Window', 'Sort', 'Window', 'Sort', 'Window', 'Sort', 'Window', 'Sort', 'Window', 'Sort', 'Window', '']
WITH 
tb16 AS (
SELECT Store_Returns.sr_returned_date_sk, Store_Returns.sr_return_time_sk, Store_Returns.sr_item_sk, Store_Returns.sr_customer_sk, Store_Returns.sr_cdemo_sk, Store_Returns.sr_hdemo_sk, Store_Returns.sr_addr_sk, Store_Returns.sr_store_sk, Store_Returns.sr_reason_sk, Store_Returns.sr_ticket_number, SUM(sr_return_quantity) AS sr_return_quantity_SUM, COUNT(sr_return_amt) AS sr_return_amt_COUNT, AVG(sr_return_tax) AS sr_return_tax_AVG
FROM Store_Returns where sr_return_quantity=300
 GROUP BY Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number
) , 
 tb17 AS (
SELECT Store_Sales.ss_sold_date_sk, Store_Sales.ss_sold_time_sk, Store_Sales.ss_item_sk, Store_Sales.ss_customer_sk, Store_Sales.ss_cdemo_sk, Store_Sales.ss_hdemo_sk, Store_Sales.ss_addr_sk, Store_Sales.ss_store_sk, Store_Sales.ss_promo_sk, Store_Sales.ss_ticket_number, MAX(ss_quantity) AS ss_quantity_MAX, SUM(ss_wholesale_cost) AS ss_wholesale_cost_SUM, SUM(ss_list_price) AS ss_list_price_SUM
FROM Store_Sales where ss_ext_discount_amt=10
 GROUP BY Store_Sales.ss_sold_date_sk,Store_Sales.ss_sold_time_sk,Store_Sales.ss_item_sk,Store_Sales.ss_customer_sk,Store_Sales.ss_cdemo_sk,Store_Sales.ss_hdemo_sk,Store_Sales.ss_addr_sk,Store_Sales.ss_store_sk,Store_Sales.ss_promo_sk,Store_Sales.ss_ticket_number
) , 
tb18 AS (
SELECT tb16.sr_returned_date_sk,tb16.sr_return_time_sk,tb16.sr_item_sk,tb16.sr_customer_sk,tb16.sr_cdemo_sk,tb16.sr_hdemo_sk,tb16.sr_addr_sk,tb16.sr_store_sk,tb16.sr_reason_sk,tb16.sr_ticket_number,tb16.sr_return_quantity_SUM,tb16.sr_return_amt_COUNT,tb16.sr_return_tax_AVG , tb17.ss_sold_date_sk,tb17.ss_sold_time_sk,tb17.ss_item_sk,tb17.ss_customer_sk,tb17.ss_cdemo_sk,tb17.ss_hdemo_sk,tb17.ss_addr_sk,tb17.ss_store_sk,tb17.ss_promo_sk,tb17.ss_ticket_number,tb17.ss_quantity_MAX,tb17.ss_wholesale_cost_SUM,tb17.ss_list_price_SUM
FROM tb16 , tb17
WHERE tb16.sr_ticket_number = tb17.ss_ticket_number and tb16.sr_item_sk = tb17.ss_item_sk and tb16.sr_customer_sk = tb17.ss_customer_sk
 ) , 
 tb19 AS (
SELECT Catalog_Sales.cs_sold_date_sk, Catalog_Sales.cs_sold_time_sk, Catalog_Sales.cs_ship_date_sk, Catalog_Sales.cs_bill_customer_sk, Catalog_Sales.cs_bill_cdemo_sk, Catalog_Sales.cs_bill_hdemo_sk, Catalog_Sales.cs_bill_addr_sk, Catalog_Sales.cs_ship_customer_sk, Catalog_Sales.cs_ship_cdemo_sk, Catalog_Sales.cs_ship_hdemo_sk, Catalog_Sales.cs_ship_addr_sk, Catalog_Sales.cs_call_center_sk, Catalog_Sales.cs_catalog_page_sk, Catalog_Sales.cs_ship_mode_sk, Catalog_Sales.cs_warehouse_sk, Catalog_Sales.cs_item_sk, Catalog_Sales.cs_promo_sk, Catalog_Sales.cs_order_number, MAX(cs_quantity) AS cs_quantity_MAX
FROM Catalog_Sales where cs_quantity=300
 GROUP BY Catalog_Sales.cs_sold_date_sk,Catalog_Sales.cs_sold_time_sk,Catalog_Sales.cs_ship_date_sk,Catalog_Sales.cs_bill_customer_sk,Catalog_Sales.cs_bill_cdemo_sk,Catalog_Sales.cs_bill_hdemo_sk,Catalog_Sales.cs_bill_addr_sk,Catalog_Sales.cs_ship_customer_sk,Catalog_Sales.cs_ship_cdemo_sk,Catalog_Sales.cs_ship_hdemo_sk,Catalog_Sales.cs_ship_addr_sk,Catalog_Sales.cs_call_center_sk,Catalog_Sales.cs_catalog_page_sk,Catalog_Sales.cs_ship_mode_sk,Catalog_Sales.cs_warehouse_sk,Catalog_Sales.cs_item_sk,Catalog_Sales.cs_promo_sk,Catalog_Sales.cs_order_number
) 
SELECT tb18.sr_returned_date_sk,tb18.sr_return_time_sk,tb18.sr_item_sk,tb18.sr_customer_sk,tb18.sr_cdemo_sk,tb18.sr_hdemo_sk,tb18.sr_addr_sk,tb18.sr_store_sk,tb18.sr_reason_sk,tb18.sr_ticket_number,tb18.sr_return_quantity_SUM,tb18.sr_return_amt_COUNT,tb18.sr_return_tax_AVG , tb19.cs_sold_date_sk,tb19.cs_sold_time_sk,tb19.cs_ship_date_sk,tb19.cs_bill_customer_sk,tb19.cs_bill_cdemo_sk,tb19.cs_bill_hdemo_sk,tb19.cs_bill_addr_sk,tb19.cs_ship_customer_sk,tb19.cs_ship_cdemo_sk,tb19.cs_ship_hdemo_sk,tb19.cs_ship_addr_sk,tb19.cs_call_center_sk,tb19.cs_catalog_page_sk,tb19.cs_ship_mode_sk,tb19.cs_warehouse_sk,tb19.cs_item_sk,tb19.cs_promo_sk,tb19.cs_order_number,tb19.cs_quantity_MAX,row_number() over (partition by sr_return_quantity_SUM order by sr_store_sk) as r0,row_number() over (partition by sr_return_quantity_SUM order by sr_reason_sk) as r1,row_number() over (partition by sr_return_quantity_SUM order by sr_store_sk) as r2,row_number() over (partition by sr_return_quantity_SUM order by sr_return_quantity_SUM) as r3,row_number() over (partition by sr_return_quantity_SUM order by sr_return_tax_AVG) as r4,row_number() over (partition by sr_return_quantity_SUM order by sr_item_sk) as r5
FROM tb18 , tb19
WHERE tb18.sr_customer_sk = tb19.cs_bill_customer_sk
 
 LIMIT 100;
---new sql--2024-01-03 15:06:28-----
--- long_chain is ['FileScan FactTable', 'Filter', 'Project', 'HashAggregate']

SELECT Store_Returns.sr_returned_date_sk, Store_Returns.sr_return_time_sk, Store_Returns.sr_item_sk, Store_Returns.sr_customer_sk, Store_Returns.sr_cdemo_sk, Store_Returns.sr_hdemo_sk, Store_Returns.sr_addr_sk, Store_Returns.sr_store_sk, Store_Returns.sr_reason_sk, Store_Returns.sr_ticket_number, SUM(sr_return_quantity) AS sr_return_quantity_SUM, SUM(sr_return_amt) AS sr_return_amt_SUM
FROM Store_Returns where sr_return_amt=100
 GROUP BY Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number

 LIMIT 100;