
---new sql--2024-01-02 16:36:15-----
--- long_chain is ['FileScan FactTable', 'Filter', 'Project', 'HashAggregate', 'Window', '']

SELECT Store_Returns.sr_returned_date_sk, Store_Returns.sr_return_time_sk, Store_Returns.sr_item_sk, Store_Returns.sr_customer_sk, Store_Returns.sr_cdemo_sk, Store_Returns.sr_hdemo_sk, Store_Returns.sr_addr_sk, Store_Returns.sr_store_sk, Store_Returns.sr_reason_sk, Store_Returns.sr_ticket_number, COUNT(sr_return_quantity) AS sr_return_quantity_COUNT, SUM(sr_return_amt) AS sr_return_amt_SUM, AVG(sr_return_tax) AS sr_return_tax_AVG,row_number() over (partition by sr_return_quantity_COUNT order by sr_reason_sk) as r0
FROM Store_Returns where sr_return_quantity=300
 GROUP BY Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number

---new sql--2024-01-02 16:36:15-----
--- long_chain is ['FileScan FactTable', 'Filter', 'Project', 'HashAggregate', 'Window', '']

SELECT Store_Returns.sr_returned_date_sk, Store_Returns.sr_return_time_sk, Store_Returns.sr_item_sk, Store_Returns.sr_customer_sk, Store_Returns.sr_cdemo_sk, Store_Returns.sr_hdemo_sk, Store_Returns.sr_addr_sk, Store_Returns.sr_store_sk, Store_Returns.sr_reason_sk, Store_Returns.sr_ticket_number, Store_Returns.sr_returned_date_sk, Store_Returns.sr_return_time_sk, Store_Returns.sr_item_sk, Store_Returns.sr_customer_sk, Store_Returns.sr_cdemo_sk, Store_Returns.sr_hdemo_sk, Store_Returns.sr_addr_sk, Store_Returns.sr_store_sk, Store_Returns.sr_reason_sk, Store_Returns.sr_ticket_number, COUNT(sr_return_quantity) AS sr_return_quantity_COUNT, SUM(sr_return_amt) AS sr_return_amt_SUM, AVG(sr_return_tax) AS sr_return_tax_AVG, SUM(sr_return_quantity_COUNT) AS sr_return_quantity_COUNT_SUM,row_number() over (partition by sr_return_quantity_COUNT order by sr_returned_date_sk) as r0,row_number() over (partition by sr_return_quantity_COUNT order by sr_store_sk) as r1
FROM Store_Returns where sr_return_quantity=100
 GROUP BY Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number,Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number

---new sql--2024-01-02 16:36:15-----
--- long_chain is ['FileScan FactTable', 'Filter', 'Project', 'HashAggregate', 'Window', '']

SELECT Store_Returns.sr_returned_date_sk, Store_Returns.sr_return_time_sk, Store_Returns.sr_item_sk, Store_Returns.sr_customer_sk, Store_Returns.sr_cdemo_sk, Store_Returns.sr_hdemo_sk, Store_Returns.sr_addr_sk, Store_Returns.sr_store_sk, Store_Returns.sr_reason_sk, Store_Returns.sr_ticket_number, Store_Returns.sr_returned_date_sk, Store_Returns.sr_return_time_sk, Store_Returns.sr_item_sk, Store_Returns.sr_customer_sk, Store_Returns.sr_cdemo_sk, Store_Returns.sr_hdemo_sk, Store_Returns.sr_addr_sk, Store_Returns.sr_store_sk, Store_Returns.sr_reason_sk, Store_Returns.sr_ticket_number, Store_Returns.sr_returned_date_sk, Store_Returns.sr_return_time_sk, Store_Returns.sr_item_sk, Store_Returns.sr_customer_sk, Store_Returns.sr_cdemo_sk, Store_Returns.sr_hdemo_sk, Store_Returns.sr_addr_sk, Store_Returns.sr_store_sk, Store_Returns.sr_reason_sk, Store_Returns.sr_ticket_number, COUNT(sr_return_quantity) AS sr_return_quantity_COUNT, SUM(sr_return_amt) AS sr_return_amt_SUM, AVG(sr_return_tax) AS sr_return_tax_AVG, SUM(sr_return_quantity_COUNT) AS sr_return_quantity_COUNT_SUM, COUNT(sr_return_quantity_COUNT_SUM) AS sr_return_quantity_COUNT_SUM_COUNT,row_number() over (partition by sr_return_quantity_COUNT order by sr_return_quantity_COUNT_SUM_COUNT) as r0,row_number() over (partition by sr_return_quantity_COUNT order by sr_store_sk) as r1,row_number() over (partition by sr_return_quantity_COUNT order by sr_customer_sk) as r2
FROM Store_Returns where sr_return_amt=100
 GROUP BY Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number,Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number,Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number

---new sql--2024-01-02 16:43:12-----
--- long_chain is ['FileScan FactTable', 'Filter', 'Project', 'HashAggregate', 'Window', '']

SELECT Store_Returns.sr_returned_date_sk, Store_Returns.sr_return_time_sk, Store_Returns.sr_item_sk, Store_Returns.sr_customer_sk, Store_Returns.sr_cdemo_sk, Store_Returns.sr_hdemo_sk, Store_Returns.sr_addr_sk, Store_Returns.sr_store_sk, Store_Returns.sr_reason_sk, Store_Returns.sr_ticket_number, SUM(sr_return_quantity) AS sr_return_quantity_SUM, COUNT(sr_return_amt) AS sr_return_amt_COUNT,row_number() over (partition by sr_returned_date_sk order by sr_ticket_number) as r0
FROM Store_Returns where sr_return_amt=200
 GROUP BY Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number

---new sql--2024-01-02 16:43:12-----
--- long_chain is ['FileScan FactTable', 'Filter', 'Project', 'HashAggregate', 'Window', '']

SELECT Store_Returns.sr_returned_date_sk, Store_Returns.sr_return_time_sk, Store_Returns.sr_item_sk, Store_Returns.sr_customer_sk, Store_Returns.sr_cdemo_sk, Store_Returns.sr_hdemo_sk, Store_Returns.sr_addr_sk, Store_Returns.sr_store_sk, Store_Returns.sr_reason_sk, Store_Returns.sr_ticket_number, Store_Returns.sr_returned_date_sk, Store_Returns.sr_return_time_sk, Store_Returns.sr_item_sk, Store_Returns.sr_customer_sk, Store_Returns.sr_cdemo_sk, Store_Returns.sr_hdemo_sk, Store_Returns.sr_addr_sk, Store_Returns.sr_store_sk, Store_Returns.sr_reason_sk, Store_Returns.sr_ticket_number, SUM(sr_return_quantity) AS sr_return_quantity_SUM, COUNT(sr_return_amt) AS sr_return_amt_COUNT, SUM(sr_return_quantity_SUM) AS sr_return_quantity_SUM_SUM,row_number() over (partition by sr_returned_date_sk order by sr_customer_sk) as r0,row_number() over (partition by sr_returned_date_sk order by sr_hdemo_sk) as r1
FROM Store_Returns where sr_return_amt=300
 GROUP BY Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number,Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number

---new sql--2024-01-02 16:43:12-----
--- long_chain is ['FileScan FactTable', 'Filter', 'Project', 'HashAggregate', 'Window', '']

SELECT Store_Returns.sr_returned_date_sk, Store_Returns.sr_return_time_sk, Store_Returns.sr_item_sk, Store_Returns.sr_customer_sk, Store_Returns.sr_cdemo_sk, Store_Returns.sr_hdemo_sk, Store_Returns.sr_addr_sk, Store_Returns.sr_store_sk, Store_Returns.sr_reason_sk, Store_Returns.sr_ticket_number, Store_Returns.sr_returned_date_sk, Store_Returns.sr_return_time_sk, Store_Returns.sr_item_sk, Store_Returns.sr_customer_sk, Store_Returns.sr_cdemo_sk, Store_Returns.sr_hdemo_sk, Store_Returns.sr_addr_sk, Store_Returns.sr_store_sk, Store_Returns.sr_reason_sk, Store_Returns.sr_ticket_number, Store_Returns.sr_returned_date_sk, Store_Returns.sr_return_time_sk, Store_Returns.sr_item_sk, Store_Returns.sr_customer_sk, Store_Returns.sr_cdemo_sk, Store_Returns.sr_hdemo_sk, Store_Returns.sr_addr_sk, Store_Returns.sr_store_sk, Store_Returns.sr_reason_sk, Store_Returns.sr_ticket_number, SUM(sr_return_quantity) AS sr_return_quantity_SUM, COUNT(sr_return_amt) AS sr_return_amt_COUNT, SUM(sr_return_quantity_SUM) AS sr_return_quantity_SUM_SUM, SUM(sr_return_quantity_SUM_SUM) AS sr_return_quantity_SUM_SUM_SUM,row_number() over (partition by sr_returned_date_sk order by sr_hdemo_sk) as r0,row_number() over (partition by sr_returned_date_sk order by sr_hdemo_sk) as r1,row_number() over (partition by sr_returned_date_sk order by sr_addr_sk) as r2
FROM Store_Returns where sr_return_quantity=100
 GROUP BY Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number,Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number,Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number
 ;

---new sql--2024-01-02 16:59:50-----
--- long_chain is ['FileScan FactTable', 'Filter', 'Project', 'HashAggregate', 'Window', '']

SELECT Store_Returns.sr_returned_date_sk, Store_Returns.sr_return_time_sk, Store_Returns.sr_item_sk, Store_Returns.sr_customer_sk, Store_Returns.sr_cdemo_sk, Store_Returns.sr_hdemo_sk, Store_Returns.sr_addr_sk, Store_Returns.sr_store_sk, Store_Returns.sr_reason_sk, Store_Returns.sr_ticket_number, SUM(sr_return_quantity) AS sr_return_quantity_SUM, COUNT(sr_return_amt) AS sr_return_amt_COUNT, SUM(sr_return_tax) AS sr_return_tax_SUM,row_number() over (partition by sr_returned_date_sk order by sr_return_tax_SUM) as r0
FROM Store_Returns where sr_return_amt=200
 GROUP BY Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number

;
---new sql--2024-01-02 17:04:30-----
--- long_chain is ['FileScan FactTable', 'Filter', 'Project', 'HashAggregate', 'Window', '']

SELECT Store_Returns.sr_returned_date_sk, Store_Returns.sr_return_time_sk, Store_Returns.sr_item_sk, Store_Returns.sr_customer_sk, Store_Returns.sr_cdemo_sk, Store_Returns.sr_hdemo_sk, Store_Returns.sr_addr_sk, Store_Returns.sr_store_sk, Store_Returns.sr_reason_sk, Store_Returns.sr_ticket_number, SUM(sr_return_quantity) AS sr_return_quantity_SUM,row_number() over (partition by sr_returned_date_sk order by sr_addr_sk) as r0
FROM Store_Returns where sr_return_amt=300
 GROUP BY Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number

;
---new sql--2024-01-02 17:04:30-----
--- long_chain is ['FileScan FactTable', 'Filter', 'Project', 'HashAggregate', 'Window', '']

SELECT Store_Returns.sr_returned_date_sk, Store_Returns.sr_return_time_sk, Store_Returns.sr_item_sk, Store_Returns.sr_customer_sk, Store_Returns.sr_cdemo_sk, Store_Returns.sr_hdemo_sk, Store_Returns.sr_addr_sk, Store_Returns.sr_store_sk, Store_Returns.sr_reason_sk, Store_Returns.sr_ticket_number, Store_Returns.sr_returned_date_sk, Store_Returns.sr_return_time_sk, Store_Returns.sr_item_sk, Store_Returns.sr_customer_sk, Store_Returns.sr_cdemo_sk, Store_Returns.sr_hdemo_sk, Store_Returns.sr_addr_sk, Store_Returns.sr_store_sk, Store_Returns.sr_reason_sk, Store_Returns.sr_ticket_number, SUM(sr_return_quantity) AS sr_return_quantity_SUM, COUNT(sr_return_quantity_SUM) AS sr_return_quantity_SUM_COUNT,row_number() over (partition by sr_returned_date_sk order by sr_reason_sk) as r0,row_number() over (partition by sr_returned_date_sk order by sr_store_sk) as r1
FROM Store_Returns where sr_return_amt=200
 GROUP BY Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number,Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number

;
---new sql--2024-01-02 17:04:30-----
--- long_chain is ['FileScan FactTable', 'Filter', 'Project', 'HashAggregate', 'Window', '']

SELECT Store_Returns.sr_returned_date_sk, Store_Returns.sr_return_time_sk, Store_Returns.sr_item_sk, Store_Returns.sr_customer_sk, Store_Returns.sr_cdemo_sk, Store_Returns.sr_hdemo_sk, Store_Returns.sr_addr_sk, Store_Returns.sr_store_sk, Store_Returns.sr_reason_sk, Store_Returns.sr_ticket_number, Store_Returns.sr_returned_date_sk, Store_Returns.sr_return_time_sk, Store_Returns.sr_item_sk, Store_Returns.sr_customer_sk, Store_Returns.sr_cdemo_sk, Store_Returns.sr_hdemo_sk, Store_Returns.sr_addr_sk, Store_Returns.sr_store_sk, Store_Returns.sr_reason_sk, Store_Returns.sr_ticket_number, Store_Returns.sr_returned_date_sk, Store_Returns.sr_return_time_sk, Store_Returns.sr_item_sk, Store_Returns.sr_customer_sk, Store_Returns.sr_cdemo_sk, Store_Returns.sr_hdemo_sk, Store_Returns.sr_addr_sk, Store_Returns.sr_store_sk, Store_Returns.sr_reason_sk, Store_Returns.sr_ticket_number, SUM(sr_return_quantity) AS sr_return_quantity_SUM, COUNT(sr_return_quantity_SUM) AS sr_return_quantity_SUM_COUNT, SUM(sr_return_quantity_SUM_COUNT) AS sr_return_quantity_SUM_COUNT_SUM,row_number() over (partition by sr_returned_date_sk order by sr_cdemo_sk) as r0,row_number() over (partition by sr_returned_date_sk order by sr_store_sk) as r1,row_number() over (partition by sr_returned_date_sk order by sr_store_sk) as r2
FROM Store_Returns where sr_return_quantity=300
 GROUP BY Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number,Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number,Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number

;
---new sql--2024-01-02 17:06:21-----
--- long_chain is ['FileScan FactTable', 'Filter', 'Project', 'HashAggregate', 'Window', '']

SELECT Store_Returns.sr_returned_date_sk, Store_Returns.sr_return_time_sk, Store_Returns.sr_item_sk, Store_Returns.sr_customer_sk, Store_Returns.sr_cdemo_sk, Store_Returns.sr_hdemo_sk, Store_Returns.sr_addr_sk, Store_Returns.sr_store_sk, Store_Returns.sr_reason_sk, Store_Returns.sr_ticket_number, AVG(sr_return_quantity) AS sr_return_quantity_AVG, SUM(sr_return_amt) AS sr_return_amt_SUM, AVG(sr_return_tax) AS sr_return_tax_AVG,row_number() over (partition by sr_returned_date_sk order by sr_returned_date_sk) as r0
FROM Store_Returns where sr_return_amt=300
 GROUP BY Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number

;
---new sql--2024-01-02 17:06:21-----
--- long_chain is ['FileScan FactTable', 'Filter', 'Project', 'HashAggregate', 'Window', '']

SELECT Store_Returns.sr_returned_date_sk, Store_Returns.sr_return_time_sk, Store_Returns.sr_item_sk, Store_Returns.sr_customer_sk, Store_Returns.sr_cdemo_sk, Store_Returns.sr_hdemo_sk, Store_Returns.sr_addr_sk, Store_Returns.sr_store_sk, Store_Returns.sr_reason_sk, Store_Returns.sr_ticket_number, Store_Returns.sr_returned_date_sk, Store_Returns.sr_return_time_sk, Store_Returns.sr_item_sk, Store_Returns.sr_customer_sk, Store_Returns.sr_cdemo_sk, Store_Returns.sr_hdemo_sk, Store_Returns.sr_addr_sk, Store_Returns.sr_store_sk, Store_Returns.sr_reason_sk, Store_Returns.sr_ticket_number, AVG(sr_return_quantity) AS sr_return_quantity_AVG, SUM(sr_return_amt) AS sr_return_amt_SUM, AVG(sr_return_tax) AS sr_return_tax_AVG,row_number() over (partition by sr_returned_date_sk order by sr_store_sk) as r0,row_number() over (partition by sr_returned_date_sk order by sr_return_time_sk) as r1
FROM Store_Returns where sr_return_quantity=100
 GROUP BY Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number,Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number

;
---new sql--2024-01-02 17:06:21-----
--- long_chain is ['FileScan FactTable', 'Filter', 'Project', 'HashAggregate', 'Window', '']

SELECT Store_Returns.sr_returned_date_sk, Store_Returns.sr_return_time_sk, Store_Returns.sr_item_sk, Store_Returns.sr_customer_sk, Store_Returns.sr_cdemo_sk, Store_Returns.sr_hdemo_sk, Store_Returns.sr_addr_sk, Store_Returns.sr_store_sk, Store_Returns.sr_reason_sk, Store_Returns.sr_ticket_number, Store_Returns.sr_returned_date_sk, Store_Returns.sr_return_time_sk, Store_Returns.sr_item_sk, Store_Returns.sr_customer_sk, Store_Returns.sr_cdemo_sk, Store_Returns.sr_hdemo_sk, Store_Returns.sr_addr_sk, Store_Returns.sr_store_sk, Store_Returns.sr_reason_sk, Store_Returns.sr_ticket_number, Store_Returns.sr_returned_date_sk, Store_Returns.sr_return_time_sk, Store_Returns.sr_item_sk, Store_Returns.sr_customer_sk, Store_Returns.sr_cdemo_sk, Store_Returns.sr_hdemo_sk, Store_Returns.sr_addr_sk, Store_Returns.sr_store_sk, Store_Returns.sr_reason_sk, Store_Returns.sr_ticket_number, AVG(sr_return_quantity) AS sr_return_quantity_AVG, SUM(sr_return_amt) AS sr_return_amt_SUM, AVG(sr_return_tax) AS sr_return_tax_AVG,row_number() over (partition by sr_returned_date_sk order by sr_cdemo_sk) as r0,row_number() over (partition by sr_returned_date_sk order by sr_cdemo_sk) as r1,row_number() over (partition by sr_returned_date_sk order by sr_addr_sk) as r2
FROM Store_Returns where sr_return_quantity=300
 GROUP BY Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number,Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number,Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number

;
---new sql--2024-01-02 17:12:13-----
--- long_chain is ['FileScan FactTable', 'Filter', 'BroadcastHashJoin', 'Project', 'BroadcastHashJoin', 'Filter', 'sort', 'SortMergeJoin', 'BroadcastHashJoin', 'Project', 'Sort', 'Window', 'Sort', 'Window', 'Sort', 'Window', 'Sort', 'Window', 'Sort', 'Window', 'Sort', 'Window', '']
WITH 
tb1 AS (
SELECT Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number,Store_Returns.sr_return_quantity,Store_Returns.sr_return_amt,Store_Returns.sr_return_tax,Store_Returns.sr_return_amt_inc_tax,Store_Returns.sr_fee,Store_Returns.sr_return_ship_cost,Store_Returns.sr_refunded_cash,Store_Returns.sr_reversed_charge,Store_Returns.sr_store_credit,Store_Returns.sr_net_loss
FROM Store_Returns where sr_return_quantity=200
 
) , 
 tb2 AS (
SELECT Store_Sales.ss_sold_date_sk,Store_Sales.ss_sold_time_sk,Store_Sales.ss_item_sk,Store_Sales.ss_customer_sk,Store_Sales.ss_cdemo_sk,Store_Sales.ss_hdemo_sk,Store_Sales.ss_addr_sk,Store_Sales.ss_store_sk,Store_Sales.ss_promo_sk,Store_Sales.ss_ticket_number,Store_Sales.ss_quantity,Store_Sales.ss_wholesale_cost,Store_Sales.ss_list_price,Store_Sales.ss_sales_price,Store_Sales.ss_ext_discount_amt,Store_Sales.ss_ext_sales_price,Store_Sales.ss_ext_wholesale_cost,Store_Sales.ss_ext_list_price,Store_Sales.ss_ext_tax,Store_Sales.ss_coupon_amt,Store_Sales.ss_net_paid,Store_Sales.ss_net_paid_inc_tax,Store_Sales.ss_net_profit
FROM Store_Sales 
 
 
) , 
tb3 AS (
SELECT tb1.sr_returned_date_sk,tb1.sr_return_time_sk,tb1.sr_item_sk,tb1.sr_customer_sk,tb1.sr_cdemo_sk,tb1.sr_hdemo_sk,tb1.sr_addr_sk,tb1.sr_store_sk,tb1.sr_reason_sk,tb1.sr_ticket_number,tb1.sr_return_quantity,tb1.sr_return_amt,tb1.sr_return_tax,tb1.sr_return_amt_inc_tax,tb1.sr_fee,tb1.sr_return_ship_cost,tb1.sr_refunded_cash,tb1.sr_reversed_charge,tb1.sr_store_credit,tb1.sr_net_loss , tb2.ss_sold_date_sk,tb2.ss_sold_time_sk,tb2.ss_item_sk,tb2.ss_customer_sk,tb2.ss_cdemo_sk,tb2.ss_hdemo_sk,tb2.ss_addr_sk,tb2.ss_store_sk,tb2.ss_promo_sk,tb2.ss_ticket_number,tb2.ss_quantity,tb2.ss_wholesale_cost,tb2.ss_list_price,tb2.ss_sales_price,tb2.ss_ext_discount_amt,tb2.ss_ext_sales_price,tb2.ss_ext_wholesale_cost,tb2.ss_ext_list_price,tb2.ss_ext_tax,tb2.ss_coupon_amt,tb2.ss_net_paid,tb2.ss_net_paid_inc_tax,tb2.ss_net_profit
FROM tb1 , tb2
WHERE tb1.sr_ticket_number = tb2.ss_ticket_number
 ) , 
 tb4 AS (
SELECT Catalog_Sales.cs_sold_date_sk,Catalog_Sales.cs_sold_time_sk,Catalog_Sales.cs_ship_date_sk,Catalog_Sales.cs_bill_customer_sk,Catalog_Sales.cs_bill_cdemo_sk,Catalog_Sales.cs_bill_hdemo_sk,Catalog_Sales.cs_bill_addr_sk,Catalog_Sales.cs_ship_customer_sk,Catalog_Sales.cs_ship_cdemo_sk,Catalog_Sales.cs_ship_hdemo_sk,Catalog_Sales.cs_ship_addr_sk,Catalog_Sales.cs_call_center_sk,Catalog_Sales.cs_catalog_page_sk,Catalog_Sales.cs_ship_mode_sk,Catalog_Sales.cs_warehouse_sk,Catalog_Sales.cs_item_sk,Catalog_Sales.cs_promo_sk,Catalog_Sales.cs_order_number,Catalog_Sales.cs_quantity,Catalog_Sales.cs_wholesale_cost,Catalog_Sales.cs_list_price,Catalog_Sales.cs_sales_price,Catalog_Sales.cs_ext_discount_amt,Catalog_Sales.cs_ext_sales_price,Catalog_Sales.cs_ext_wholesale_cost,Catalog_Sales.cs_ext_list_price,Catalog_Sales.cs_ext_tax,Catalog_Sales.cs_coupon_amt,Catalog_Sales.cs_ext_ship_cost,Catalog_Sales.cs_net_paid,Catalog_Sales.cs_net_paid_inc_tax,Catalog_Sales.cs_net_paid_inc_ship,Catalog_Sales.cs_net_paid_inc_ship_tax,Catalog_Sales.cs_net_profit
FROM Catalog_Sales 
 
 
) , 
tb5 AS (
SELECT tb3.sr_returned_date_sk,tb3.sr_return_time_sk,tb3.sr_item_sk,tb3.sr_customer_sk,tb3.sr_cdemo_sk,tb3.sr_hdemo_sk,tb3.sr_addr_sk,tb3.sr_store_sk,tb3.sr_reason_sk,tb3.sr_ticket_number,tb3.sr_return_quantity,tb3.sr_return_amt,tb3.sr_return_tax,tb3.sr_return_amt_inc_tax,tb3.sr_fee,tb3.sr_return_ship_cost,tb3.sr_refunded_cash,tb3.sr_reversed_charge,tb3.sr_store_credit,tb3.sr_net_loss , tb4.cs_sold_date_sk,tb4.cs_sold_time_sk,tb4.cs_ship_date_sk,tb4.cs_bill_customer_sk,tb4.cs_bill_cdemo_sk,tb4.cs_bill_hdemo_sk,tb4.cs_bill_addr_sk,tb4.cs_ship_customer_sk,tb4.cs_ship_cdemo_sk,tb4.cs_ship_hdemo_sk,tb4.cs_ship_addr_sk,tb4.cs_call_center_sk,tb4.cs_catalog_page_sk,tb4.cs_ship_mode_sk,tb4.cs_warehouse_sk,tb4.cs_item_sk,tb4.cs_promo_sk,tb4.cs_order_number,tb4.cs_quantity,tb4.cs_wholesale_cost,tb4.cs_list_price,tb4.cs_sales_price,tb4.cs_ext_discount_amt,tb4.cs_ext_sales_price,tb4.cs_ext_wholesale_cost,tb4.cs_ext_list_price,tb4.cs_ext_tax,tb4.cs_coupon_amt,tb4.cs_ext_ship_cost,tb4.cs_net_paid,tb4.cs_net_paid_inc_tax,tb4.cs_net_paid_inc_ship,tb4.cs_net_paid_inc_ship_tax,tb4.cs_net_profit
FROM tb3 , tb4
WHERE tb3.sr_item_sk = tb4.cs_item_sk and tb3.sr_customer_sk = tb4.cs_bill_customer_sk
 ) , 
 tb6 AS (
SELECT Customer.c_customer_sk, Customer.c_current_cdemo_sk, Customer.c_current_hdemo_sk, Customer.c_current_addr_sk, Customer.c_first_shipto_date_sk, Customer.c_first_sales_date_sk, MAX(cast(cast( c_birth_day as BIGINT)  as string)) AS c_birth_day_MAX, MAX(cast(cast( c_birth_month as BIGINT)  as string)) AS c_birth_month_MAX, MAX(cast(cast( c_birth_year as BIGINT)  as string)) AS c_birth_year_MAX
FROM Customer 
 
 GROUP BY Customer.c_customer_sk,Customer.c_current_cdemo_sk,Customer.c_current_hdemo_sk,Customer.c_current_addr_sk,Customer.c_first_shipto_date_sk,Customer.c_first_sales_date_sk
) , 
tb7 AS (
SELECT tb5.sr_returned_date_sk,tb5.sr_return_time_sk,tb5.sr_item_sk,tb5.sr_customer_sk,tb5.sr_cdemo_sk,tb5.sr_hdemo_sk,tb5.sr_addr_sk,tb5.sr_store_sk,tb5.sr_reason_sk,tb5.sr_ticket_number,tb5.sr_return_quantity,tb5.sr_return_amt,tb5.sr_return_tax,tb5.sr_return_amt_inc_tax,tb5.sr_fee,tb5.sr_return_ship_cost,tb5.sr_refunded_cash,tb5.sr_reversed_charge,tb5.sr_store_credit,tb5.sr_net_loss , tb6.c_customer_sk,tb6.c_current_cdemo_sk,tb6.c_current_hdemo_sk,tb6.c_current_addr_sk,tb6.c_first_shipto_date_sk,tb6.c_first_sales_date_sk,tb6.c_birth_day_MAX,tb6.c_birth_month_MAX,tb6.c_birth_year_MAX
FROM tb5 , tb6
WHERE tb5.sr_customer_sk = tb6.c_customer_sk
 ) , 
 tb8 AS (
SELECT Date_Dim.d_date_sk,Date_Dim.d_date_id,Date_Dim.d_date,Date_Dim.d_month_seq,Date_Dim.d_week_seq,Date_Dim.d_quarter_seq,Date_Dim.d_year,Date_Dim.d_dow,Date_Dim.d_moy,Date_Dim.d_dom,Date_Dim.d_qoy,Date_Dim.d_fy_year,Date_Dim.d_fy_quarter_seq,Date_Dim.d_fy_week_seq,Date_Dim.d_day_name ,Date_Dim.d_quarter_name ,Date_Dim.d_holiday ,Date_Dim.d_weekend,Date_Dim.d_following_holiday,Date_Dim.d_first_dom,Date_Dim.d_last_dom,Date_Dim.d_same_day_ly,Date_Dim.d_same_day_lq,Date_Dim.d_current_day,Date_Dim.d_current_week,Date_Dim.d_current_month ,Date_Dim.d_current_quarter,Date_Dim.d_current_year
FROM Date_Dim 
 
 
) 
SELECT tb7.sr_returned_date_sk,tb7.sr_return_time_sk,tb7.sr_item_sk,tb7.sr_customer_sk,tb7.sr_cdemo_sk,tb7.sr_hdemo_sk,tb7.sr_addr_sk,tb7.sr_store_sk,tb7.sr_reason_sk,tb7.sr_ticket_number,tb7.sr_return_quantity,tb7.sr_return_amt,tb7.sr_return_tax,tb7.sr_return_amt_inc_tax,tb7.sr_fee,tb7.sr_return_ship_cost,tb7.sr_refunded_cash,tb7.sr_reversed_charge,tb7.sr_store_credit,tb7.sr_net_loss , tb8.d_date_id,tb8.d_month_seq,tb8.d_week_seq,tb8.d_quarter_seq,tb8.d_year,tb8.d_dow,tb8.d_moy,tb8.d_dom,tb8.d_qoy,tb8.d_fy_year,tb8.d_fy_quarter_seq,tb8.d_fy_week_seq,tb8.d_day_name ,tb8.d_quarter_name ,tb8.d_holiday ,tb8.d_weekend,tb8.d_following_holiday,tb8.d_first_dom,tb8.d_last_dom,tb8.d_same_day_ly,tb8.d_same_day_lq,tb8.d_current_day,tb8.d_current_week,tb8.d_current_month ,tb8.d_current_quarter,tb8.d_current_year,row_number() over (partition by sr_return_quantity order by sr_fee) as r0,row_number() over (partition by sr_return_quantity order by sr_fee) as r1,row_number() over (partition by sr_return_quantity order by sr_return_tax) as r2,row_number() over (partition by sr_return_quantity order by sr_ticket_number) as r3,row_number() over (partition by sr_return_quantity order by sr_refunded_cash) as r4,row_number() over (partition by sr_return_quantity order by sr_ticket_number) as r5
FROM tb7 , tb8
WHERE tb7.sr_returned_date_sk = tb8.d_date_sk
 
 LIMIT 100;
---new sql--2024-01-02 17:12:13-----
--- long_chain is ['FileScan FactTable', 'Filter', 'BroadcastHashJoin', 'Project', 'BroadcastHashJoin', 'Project', 'BroadcastHashJoin', 'Filter', 'sort', 'SortMergeJoin', 'Project', 'HashAggregate', 'HashAggregate', '']
WITH 
tb10 AS (
SELECT Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number,Store_Returns.sr_return_quantity,Store_Returns.sr_return_amt,Store_Returns.sr_return_tax,Store_Returns.sr_return_amt_inc_tax,Store_Returns.sr_fee,Store_Returns.sr_return_ship_cost,Store_Returns.sr_refunded_cash,Store_Returns.sr_reversed_charge,Store_Returns.sr_store_credit,Store_Returns.sr_net_loss
FROM Store_Returns where sr_return_quantity=200
 
) , 
 tb11 AS (
SELECT Store_Sales.ss_sold_date_sk, Store_Sales.ss_sold_time_sk, Store_Sales.ss_item_sk, Store_Sales.ss_customer_sk, Store_Sales.ss_cdemo_sk, Store_Sales.ss_hdemo_sk, Store_Sales.ss_addr_sk, Store_Sales.ss_store_sk, Store_Sales.ss_promo_sk, Store_Sales.ss_ticket_number, COUNT(ss_quantity) AS ss_quantity_COUNT
FROM Store_Sales where ss_list_price=13
 GROUP BY Store_Sales.ss_sold_date_sk,Store_Sales.ss_sold_time_sk,Store_Sales.ss_item_sk,Store_Sales.ss_customer_sk,Store_Sales.ss_cdemo_sk,Store_Sales.ss_hdemo_sk,Store_Sales.ss_addr_sk,Store_Sales.ss_store_sk,Store_Sales.ss_promo_sk,Store_Sales.ss_ticket_number
) , 
tb12 AS (
SELECT tb10.sr_returned_date_sk,tb10.sr_return_time_sk,tb10.sr_item_sk,tb10.sr_customer_sk,tb10.sr_cdemo_sk,tb10.sr_hdemo_sk,tb10.sr_addr_sk,tb10.sr_store_sk,tb10.sr_reason_sk,tb10.sr_ticket_number,tb10.sr_return_quantity,tb10.sr_return_amt,tb10.sr_return_tax,tb10.sr_return_amt_inc_tax,tb10.sr_fee,tb10.sr_return_ship_cost,tb10.sr_refunded_cash,tb10.sr_reversed_charge,tb10.sr_store_credit,tb10.sr_net_loss , tb11.ss_sold_date_sk,tb11.ss_sold_time_sk,tb11.ss_item_sk,tb11.ss_customer_sk,tb11.ss_cdemo_sk,tb11.ss_hdemo_sk,tb11.ss_addr_sk,tb11.ss_store_sk,tb11.ss_promo_sk,tb11.ss_ticket_number,tb11.ss_quantity_COUNT
FROM tb10 , tb11
WHERE tb10.sr_ticket_number = tb11.ss_ticket_number
 ) , 
 tb13 AS (
SELECT Catalog_Sales.cs_sold_date_sk,Catalog_Sales.cs_sold_time_sk,Catalog_Sales.cs_ship_date_sk,Catalog_Sales.cs_bill_customer_sk,Catalog_Sales.cs_bill_cdemo_sk,Catalog_Sales.cs_bill_hdemo_sk,Catalog_Sales.cs_bill_addr_sk,Catalog_Sales.cs_ship_customer_sk,Catalog_Sales.cs_ship_cdemo_sk,Catalog_Sales.cs_ship_hdemo_sk,Catalog_Sales.cs_ship_addr_sk,Catalog_Sales.cs_call_center_sk,Catalog_Sales.cs_catalog_page_sk,Catalog_Sales.cs_ship_mode_sk,Catalog_Sales.cs_warehouse_sk,Catalog_Sales.cs_item_sk,Catalog_Sales.cs_promo_sk,Catalog_Sales.cs_order_number,Catalog_Sales.cs_quantity,Catalog_Sales.cs_wholesale_cost,Catalog_Sales.cs_list_price,Catalog_Sales.cs_sales_price,Catalog_Sales.cs_ext_discount_amt,Catalog_Sales.cs_ext_sales_price,Catalog_Sales.cs_ext_wholesale_cost,Catalog_Sales.cs_ext_list_price,Catalog_Sales.cs_ext_tax,Catalog_Sales.cs_coupon_amt,Catalog_Sales.cs_ext_ship_cost,Catalog_Sales.cs_net_paid,Catalog_Sales.cs_net_paid_inc_tax,Catalog_Sales.cs_net_paid_inc_ship,Catalog_Sales.cs_net_paid_inc_ship_tax,Catalog_Sales.cs_net_profit
FROM Catalog_Sales 
 
 
) , 
tb14 AS (
SELECT tb12.sr_returned_date_sk,tb12.sr_return_time_sk,tb12.sr_item_sk,tb12.sr_customer_sk,tb12.sr_cdemo_sk,tb12.sr_hdemo_sk,tb12.sr_addr_sk,tb12.sr_store_sk,tb12.sr_reason_sk,tb12.sr_ticket_number,tb12.sr_return_quantity,tb12.sr_return_amt,tb12.sr_return_tax,tb12.sr_return_amt_inc_tax,tb12.sr_fee,tb12.sr_return_ship_cost,tb12.sr_refunded_cash,tb12.sr_reversed_charge,tb12.sr_store_credit,tb12.sr_net_loss , tb13.cs_sold_date_sk,tb13.cs_sold_time_sk,tb13.cs_ship_date_sk,tb13.cs_bill_customer_sk,tb13.cs_bill_cdemo_sk,tb13.cs_bill_hdemo_sk,tb13.cs_bill_addr_sk,tb13.cs_ship_customer_sk,tb13.cs_ship_cdemo_sk,tb13.cs_ship_hdemo_sk,tb13.cs_ship_addr_sk,tb13.cs_call_center_sk,tb13.cs_catalog_page_sk,tb13.cs_ship_mode_sk,tb13.cs_warehouse_sk,tb13.cs_item_sk,tb13.cs_promo_sk,tb13.cs_order_number,tb13.cs_quantity,tb13.cs_wholesale_cost,tb13.cs_list_price,tb13.cs_sales_price,tb13.cs_ext_discount_amt,tb13.cs_ext_sales_price,tb13.cs_ext_wholesale_cost,tb13.cs_ext_list_price,tb13.cs_ext_tax,tb13.cs_coupon_amt,tb13.cs_ext_ship_cost,tb13.cs_net_paid,tb13.cs_net_paid_inc_tax,tb13.cs_net_paid_inc_ship,tb13.cs_net_paid_inc_ship_tax,tb13.cs_net_profit
FROM tb12 , tb13
WHERE tb12.sr_customer_sk = tb13.cs_bill_customer_sk
 ) , 
 tb15 AS (
SELECT Customer.c_customer_sk, Customer.c_current_cdemo_sk, Customer.c_current_hdemo_sk, Customer.c_current_addr_sk, Customer.c_first_shipto_date_sk, Customer.c_first_sales_date_sk, MAX(cast(cast( c_birth_day as BIGINT)  as string)) AS c_birth_day_MAX, MAX(cast(cast( c_birth_month as BIGINT)  as string)) AS c_birth_month_MAX
FROM Customer 
 
 GROUP BY Customer.c_customer_sk,Customer.c_current_cdemo_sk,Customer.c_current_hdemo_sk,Customer.c_current_addr_sk,Customer.c_first_shipto_date_sk,Customer.c_first_sales_date_sk
) , 
tb16 AS (
SELECT tb14.sr_returned_date_sk,tb14.sr_return_time_sk,tb14.sr_item_sk,tb14.sr_customer_sk,tb14.sr_cdemo_sk,tb14.sr_hdemo_sk,tb14.sr_addr_sk,tb14.sr_store_sk,tb14.sr_reason_sk,tb14.sr_ticket_number,tb14.sr_return_quantity,tb14.sr_return_amt,tb14.sr_return_tax,tb14.sr_return_amt_inc_tax,tb14.sr_fee,tb14.sr_return_ship_cost,tb14.sr_refunded_cash,tb14.sr_reversed_charge,tb14.sr_store_credit,tb14.sr_net_loss , tb15.c_customer_sk,tb15.c_current_cdemo_sk,tb15.c_current_hdemo_sk,tb15.c_current_addr_sk,tb15.c_first_shipto_date_sk,tb15.c_first_sales_date_sk,tb15.c_birth_day_MAX,tb15.c_birth_month_MAX
FROM tb14 , tb15
WHERE tb14.sr_customer_sk = tb15.c_customer_sk
 ) , 
 tb17 AS (
SELECT Date_Dim.d_date_sk, SUM(d_month_seq) AS d_month_seq_SUM, COUNT(d_week_seq) AS d_week_seq_COUNT
FROM Date_Dim where d_year=2002
 GROUP BY Date_Dim.d_date_sk
) 
SELECT  tb16.sr_returned_date_sk, tb16.sr_return_time_sk, tb16.sr_item_sk, tb16.sr_customer_sk, tb16.sr_cdemo_sk, tb16.sr_hdemo_sk, tb16.sr_addr_sk, tb16.sr_store_sk, tb16.sr_reason_sk, tb16.sr_ticket_number, tb16.sr_returned_date_sk, tb16.sr_return_time_sk, tb16.sr_item_sk, tb16.sr_customer_sk, tb16.sr_cdemo_sk, tb16.sr_hdemo_sk, tb16.sr_addr_sk, tb16.sr_store_sk, tb16.sr_reason_sk, tb16.sr_ticket_number, tb17.d_month_seq_SUM, MAX(sr_return_quantity) AS sr_return_quantity_MAX, SUM(sr_return_amt) AS sr_return_amt_SUM, SUM(sr_return_quantity) AS sr_return_quantity_SUM, MAX(sr_return_amt) AS sr_return_amt_MAX
FROM tb16 , tb17
WHERE tb16.sr_returned_date_sk = tb17.d_date_sk
 GROUP BY tb16.sr_returned_date_sk,tb16.sr_return_time_sk,tb16.sr_item_sk,tb16.sr_customer_sk,tb16.sr_cdemo_sk,tb16.sr_hdemo_sk,tb16.sr_addr_sk,tb16.sr_store_sk,tb16.sr_reason_sk,tb16.sr_ticket_number,tb16.sr_returned_date_sk,tb16.sr_return_time_sk,tb16.sr_item_sk,tb16.sr_customer_sk,tb16.sr_cdemo_sk,tb16.sr_hdemo_sk,tb16.sr_addr_sk,tb16.sr_store_sk,tb16.sr_reason_sk,tb16.sr_ticket_number,tb17.d_month_seq_SUM
 LIMIT 100;
---new sql--2024-01-02 17:12:13-----
--- long_chain is ['FileScan FactTable', 'Filter', 'BroadcastHashJoin', 'Project', 'BroadcastHashJoin', 'Project', 'BroadcastHashJoin', 'Project', 'BroadcastHashJoin', 'Project', 'Sort', 'Window', 'Sort', 'Window', 'Sort', 'Window', 'Sort', 'Window', 'Sort', 'Window', 'Sort', 'Window', '']
WITH 
tb19 AS (
SELECT Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number,Store_Returns.sr_return_quantity,Store_Returns.sr_return_amt,Store_Returns.sr_return_tax,Store_Returns.sr_return_amt_inc_tax,Store_Returns.sr_fee,Store_Returns.sr_return_ship_cost,Store_Returns.sr_refunded_cash,Store_Returns.sr_reversed_charge,Store_Returns.sr_store_credit,Store_Returns.sr_net_loss
FROM Store_Returns where sr_return_quantity=300
 
) , 
 tb20 AS (
SELECT Store_Sales.ss_sold_date_sk, Store_Sales.ss_sold_time_sk, Store_Sales.ss_item_sk, Store_Sales.ss_customer_sk, Store_Sales.ss_cdemo_sk, Store_Sales.ss_hdemo_sk, Store_Sales.ss_addr_sk, Store_Sales.ss_store_sk, Store_Sales.ss_promo_sk, Store_Sales.ss_ticket_number, AVG(ss_quantity) AS ss_quantity_AVG, SUM(ss_wholesale_cost) AS ss_wholesale_cost_SUM
FROM Store_Sales where ss_list_price=10
 GROUP BY Store_Sales.ss_sold_date_sk,Store_Sales.ss_sold_time_sk,Store_Sales.ss_item_sk,Store_Sales.ss_customer_sk,Store_Sales.ss_cdemo_sk,Store_Sales.ss_hdemo_sk,Store_Sales.ss_addr_sk,Store_Sales.ss_store_sk,Store_Sales.ss_promo_sk,Store_Sales.ss_ticket_number
) , 
tb21 AS (
SELECT tb19.sr_returned_date_sk,tb19.sr_return_time_sk,tb19.sr_item_sk,tb19.sr_customer_sk,tb19.sr_cdemo_sk,tb19.sr_hdemo_sk,tb19.sr_addr_sk,tb19.sr_store_sk,tb19.sr_reason_sk,tb19.sr_ticket_number,tb19.sr_return_quantity,tb19.sr_return_amt,tb19.sr_return_tax,tb19.sr_return_amt_inc_tax,tb19.sr_fee,tb19.sr_return_ship_cost,tb19.sr_refunded_cash,tb19.sr_reversed_charge,tb19.sr_store_credit,tb19.sr_net_loss , tb20.ss_sold_date_sk,tb20.ss_sold_time_sk,tb20.ss_item_sk,tb20.ss_customer_sk,tb20.ss_cdemo_sk,tb20.ss_hdemo_sk,tb20.ss_addr_sk,tb20.ss_store_sk,tb20.ss_promo_sk,tb20.ss_ticket_number,tb20.ss_quantity_AVG,tb20.ss_wholesale_cost_SUM
FROM tb19 , tb20
WHERE tb19.sr_item_sk = tb20.ss_item_sk and tb19.sr_ticket_number = tb20.ss_ticket_number and tb19.sr_customer_sk = tb20.ss_customer_sk
 ) , 
 tb22 AS (
SELECT Catalog_Sales.cs_sold_date_sk,Catalog_Sales.cs_sold_time_sk,Catalog_Sales.cs_ship_date_sk,Catalog_Sales.cs_bill_customer_sk,Catalog_Sales.cs_bill_cdemo_sk,Catalog_Sales.cs_bill_hdemo_sk,Catalog_Sales.cs_bill_addr_sk,Catalog_Sales.cs_ship_customer_sk,Catalog_Sales.cs_ship_cdemo_sk,Catalog_Sales.cs_ship_hdemo_sk,Catalog_Sales.cs_ship_addr_sk,Catalog_Sales.cs_call_center_sk,Catalog_Sales.cs_catalog_page_sk,Catalog_Sales.cs_ship_mode_sk,Catalog_Sales.cs_warehouse_sk,Catalog_Sales.cs_item_sk,Catalog_Sales.cs_promo_sk,Catalog_Sales.cs_order_number,Catalog_Sales.cs_quantity,Catalog_Sales.cs_wholesale_cost,Catalog_Sales.cs_list_price,Catalog_Sales.cs_sales_price,Catalog_Sales.cs_ext_discount_amt,Catalog_Sales.cs_ext_sales_price,Catalog_Sales.cs_ext_wholesale_cost,Catalog_Sales.cs_ext_list_price,Catalog_Sales.cs_ext_tax,Catalog_Sales.cs_coupon_amt,Catalog_Sales.cs_ext_ship_cost,Catalog_Sales.cs_net_paid,Catalog_Sales.cs_net_paid_inc_tax,Catalog_Sales.cs_net_paid_inc_ship,Catalog_Sales.cs_net_paid_inc_ship_tax,Catalog_Sales.cs_net_profit
FROM Catalog_Sales 
 
 
) , 
tb23 AS (
SELECT tb21.sr_returned_date_sk,tb21.sr_return_time_sk,tb21.sr_item_sk,tb21.sr_customer_sk,tb21.sr_cdemo_sk,tb21.sr_hdemo_sk,tb21.sr_addr_sk,tb21.sr_store_sk,tb21.sr_reason_sk,tb21.sr_ticket_number,tb21.sr_return_quantity,tb21.sr_return_amt,tb21.sr_return_tax,tb21.sr_return_amt_inc_tax,tb21.sr_fee,tb21.sr_return_ship_cost,tb21.sr_refunded_cash,tb21.sr_reversed_charge,tb21.sr_store_credit,tb21.sr_net_loss , tb22.cs_sold_date_sk,tb22.cs_sold_time_sk,tb22.cs_ship_date_sk,tb22.cs_bill_customer_sk,tb22.cs_bill_cdemo_sk,tb22.cs_bill_hdemo_sk,tb22.cs_bill_addr_sk,tb22.cs_ship_customer_sk,tb22.cs_ship_cdemo_sk,tb22.cs_ship_hdemo_sk,tb22.cs_ship_addr_sk,tb22.cs_call_center_sk,tb22.cs_catalog_page_sk,tb22.cs_ship_mode_sk,tb22.cs_warehouse_sk,tb22.cs_item_sk,tb22.cs_promo_sk,tb22.cs_order_number,tb22.cs_quantity,tb22.cs_wholesale_cost,tb22.cs_list_price,tb22.cs_sales_price,tb22.cs_ext_discount_amt,tb22.cs_ext_sales_price,tb22.cs_ext_wholesale_cost,tb22.cs_ext_list_price,tb22.cs_ext_tax,tb22.cs_coupon_amt,tb22.cs_ext_ship_cost,tb22.cs_net_paid,tb22.cs_net_paid_inc_tax,tb22.cs_net_paid_inc_ship,tb22.cs_net_paid_inc_ship_tax,tb22.cs_net_profit
FROM tb21 , tb22
WHERE tb21.sr_customer_sk = tb22.cs_bill_customer_sk
 ) , 
 tb24 AS (
SELECT Customer.c_customer_sk, Customer.c_current_cdemo_sk, Customer.c_current_hdemo_sk, Customer.c_current_addr_sk, Customer.c_first_shipto_date_sk, Customer.c_first_sales_date_sk, SUM(c_birth_day) AS c_birth_day_SUM, SUM(c_birth_month) AS c_birth_month_SUM, SUM(c_birth_year) AS c_birth_year_SUM
FROM Customer 
 
 GROUP BY Customer.c_customer_sk,Customer.c_current_cdemo_sk,Customer.c_current_hdemo_sk,Customer.c_current_addr_sk,Customer.c_first_shipto_date_sk,Customer.c_first_sales_date_sk
) , 
tb25 AS (
SELECT tb23.sr_returned_date_sk,tb23.sr_return_time_sk,tb23.sr_item_sk,tb23.sr_customer_sk,tb23.sr_cdemo_sk,tb23.sr_hdemo_sk,tb23.sr_addr_sk,tb23.sr_store_sk,tb23.sr_reason_sk,tb23.sr_ticket_number,tb23.sr_return_quantity,tb23.sr_return_amt,tb23.sr_return_tax,tb23.sr_return_amt_inc_tax,tb23.sr_fee,tb23.sr_return_ship_cost,tb23.sr_refunded_cash,tb23.sr_reversed_charge,tb23.sr_store_credit,tb23.sr_net_loss , tb24.c_customer_sk,tb24.c_current_cdemo_sk,tb24.c_current_hdemo_sk,tb24.c_current_addr_sk,tb24.c_first_shipto_date_sk,tb24.c_first_sales_date_sk,tb24.c_birth_day_SUM,tb24.c_birth_month_SUM,tb24.c_birth_year_SUM
FROM tb23 , tb24
WHERE tb23.sr_customer_sk = tb24.c_customer_sk
 ) , 
 tb26 AS (
SELECT Date_Dim.d_date_sk, SUM(d_month_seq) AS d_month_seq_SUM, SUM(d_week_seq) AS d_week_seq_SUM
FROM Date_Dim where d_year=2001
 GROUP BY Date_Dim.d_date_sk
) 
SELECT tb25.sr_returned_date_sk,tb25.sr_return_time_sk,tb25.sr_item_sk,tb25.sr_customer_sk,tb25.sr_cdemo_sk,tb25.sr_hdemo_sk,tb25.sr_addr_sk,tb25.sr_store_sk,tb25.sr_reason_sk,tb25.sr_ticket_number,tb25.sr_return_quantity,tb25.sr_return_amt,tb25.sr_return_tax,tb25.sr_return_amt_inc_tax,tb25.sr_fee,tb25.sr_return_ship_cost,tb25.sr_refunded_cash,tb25.sr_reversed_charge,tb25.sr_store_credit,tb25.sr_net_loss , tb26.d_month_seq_SUM,tb26.d_week_seq_SUM,row_number() over (partition by sr_return_quantity order by sr_addr_sk) as r0,row_number() over (partition by sr_return_quantity order by sr_item_sk) as r1,row_number() over (partition by sr_return_quantity order by sr_return_tax) as r2,row_number() over (partition by sr_return_quantity order by sr_return_ship_cost) as r3,row_number() over (partition by sr_return_quantity order by sr_addr_sk) as r4,row_number() over (partition by sr_return_quantity order by sr_item_sk) as r5
FROM tb25 , tb26
WHERE tb25.sr_returned_date_sk = tb26.d_date_sk
 
 LIMIT 100;
---new sql--2024-01-02 17:12:41-----
--- long_chain is ['FileScan FactTable', 'Filter', 'Project', 'HashAggregate']

SELECT Store_Returns.sr_returned_date_sk, Store_Returns.sr_return_time_sk, Store_Returns.sr_item_sk, Store_Returns.sr_customer_sk, Store_Returns.sr_cdemo_sk, Store_Returns.sr_hdemo_sk, Store_Returns.sr_addr_sk, Store_Returns.sr_store_sk, Store_Returns.sr_reason_sk, Store_Returns.sr_ticket_number, SUM(sr_return_quantity) AS sr_return_quantity_SUM, MAX(sr_return_amt) AS sr_return_amt_MAX, AVG(sr_return_tax) AS sr_return_tax_AVG
FROM Store_Returns where sr_return_quantity=100
 GROUP BY Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number

;
---new sql--2024-01-02 17:12:41-----
--- long_chain is ['FileScan FactTable', 'Filter', 'Project']

SELECT Store_Returns.sr_returned_date_sk, Store_Returns.sr_return_time_sk, Store_Returns.sr_item_sk, Store_Returns.sr_customer_sk, Store_Returns.sr_cdemo_sk, Store_Returns.sr_hdemo_sk, Store_Returns.sr_addr_sk, Store_Returns.sr_store_sk, Store_Returns.sr_reason_sk, Store_Returns.sr_ticket_number, SUM(sr_return_quantity) AS sr_return_quantity_SUM, MAX(sr_return_amt) AS sr_return_amt_MAX, AVG(sr_return_tax) AS sr_return_tax_AVG
FROM Store_Returns where sr_return_amt=300
 GROUP BY Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number

;
---new sql--2024-01-02 17:12:41-----
--- long_chain is ['FileScan FactTable', 'Filter', 'Project', 'HashAggregate']

SELECT Store_Returns.sr_returned_date_sk, Store_Returns.sr_return_time_sk, Store_Returns.sr_item_sk, Store_Returns.sr_customer_sk, Store_Returns.sr_cdemo_sk, Store_Returns.sr_hdemo_sk, Store_Returns.sr_addr_sk, Store_Returns.sr_store_sk, Store_Returns.sr_reason_sk, Store_Returns.sr_ticket_number, Store_Returns.sr_returned_date_sk, Store_Returns.sr_return_time_sk, Store_Returns.sr_item_sk, Store_Returns.sr_customer_sk, Store_Returns.sr_cdemo_sk, Store_Returns.sr_hdemo_sk, Store_Returns.sr_addr_sk, Store_Returns.sr_store_sk, Store_Returns.sr_reason_sk, Store_Returns.sr_ticket_number, SUM(sr_return_quantity) AS sr_return_quantity_SUM, MAX(sr_return_amt) AS sr_return_amt_MAX, AVG(sr_return_tax) AS sr_return_tax_AVG
FROM Store_Returns where sr_return_amt=300
 GROUP BY Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number,Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number

;
---new sql--2024-01-02 17:12:53-----
--- long_chain is ['FileScan FactTable', 'Filter', 'Project', 'HashAggregate']

SELECT Store_Returns.sr_returned_date_sk, Store_Returns.sr_return_time_sk, Store_Returns.sr_item_sk, Store_Returns.sr_customer_sk, Store_Returns.sr_cdemo_sk, Store_Returns.sr_hdemo_sk, Store_Returns.sr_addr_sk, Store_Returns.sr_store_sk, Store_Returns.sr_reason_sk, Store_Returns.sr_ticket_number, AVG(sr_return_quantity) AS sr_return_quantity_AVG, MAX(sr_return_amt) AS sr_return_amt_MAX
FROM Store_Returns where sr_return_quantity=100
 GROUP BY Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number

;
---new sql--2024-01-02 17:12:53-----
--- long_chain is ['FileScan FactTable', 'Filter', 'Project', 'HashAggregate', 'Window']

SELECT Store_Returns.sr_returned_date_sk, Store_Returns.sr_return_time_sk, Store_Returns.sr_item_sk, Store_Returns.sr_customer_sk, Store_Returns.sr_cdemo_sk, Store_Returns.sr_hdemo_sk, Store_Returns.sr_addr_sk, Store_Returns.sr_store_sk, Store_Returns.sr_reason_sk, Store_Returns.sr_ticket_number, Store_Returns.sr_returned_date_sk, Store_Returns.sr_return_time_sk, Store_Returns.sr_item_sk, Store_Returns.sr_customer_sk, Store_Returns.sr_cdemo_sk, Store_Returns.sr_hdemo_sk, Store_Returns.sr_addr_sk, Store_Returns.sr_store_sk, Store_Returns.sr_reason_sk, Store_Returns.sr_ticket_number, AVG(sr_return_quantity) AS sr_return_quantity_AVG, MAX(sr_return_amt) AS sr_return_amt_MAX,row_number() over (partition by sr_returned_date_sk order by sr_customer_sk) as r0
FROM Store_Returns where sr_return_quantity=200
 GROUP BY Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number,Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number

;
---new sql--2024-01-02 17:12:53-----
--- long_chain is ['FileScan FactTable', 'Filter', 'Project', 'HashAggregate', 'Window']

SELECT Store_Returns.sr_returned_date_sk, Store_Returns.sr_return_time_sk, Store_Returns.sr_item_sk, Store_Returns.sr_customer_sk, Store_Returns.sr_cdemo_sk, Store_Returns.sr_hdemo_sk, Store_Returns.sr_addr_sk, Store_Returns.sr_store_sk, Store_Returns.sr_reason_sk, Store_Returns.sr_ticket_number, Store_Returns.sr_returned_date_sk, Store_Returns.sr_return_time_sk, Store_Returns.sr_item_sk, Store_Returns.sr_customer_sk, Store_Returns.sr_cdemo_sk, Store_Returns.sr_hdemo_sk, Store_Returns.sr_addr_sk, Store_Returns.sr_store_sk, Store_Returns.sr_reason_sk, Store_Returns.sr_ticket_number, Store_Returns.sr_returned_date_sk, Store_Returns.sr_return_time_sk, Store_Returns.sr_item_sk, Store_Returns.sr_customer_sk, Store_Returns.sr_cdemo_sk, Store_Returns.sr_hdemo_sk, Store_Returns.sr_addr_sk, Store_Returns.sr_store_sk, Store_Returns.sr_reason_sk, Store_Returns.sr_ticket_number, AVG(sr_return_quantity) AS sr_return_quantity_AVG, MAX(sr_return_amt) AS sr_return_amt_MAX,row_number() over (partition by sr_returned_date_sk order by sr_return_time_sk) as r0,row_number() over (partition by sr_returned_date_sk order by sr_store_sk) as r1
FROM Store_Returns where sr_return_amt=100
 GROUP BY Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number,Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number,Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number

;
---new sql--2024-01-02 17:13:27-----
--- long_chain is ['FileScan FactTable', 'Filter', 'Project']

SELECT Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number,Store_Returns.sr_return_quantity,Store_Returns.sr_return_amt,Store_Returns.sr_return_tax,Store_Returns.sr_return_amt_inc_tax,Store_Returns.sr_fee,Store_Returns.sr_return_ship_cost,Store_Returns.sr_refunded_cash,Store_Returns.sr_reversed_charge,Store_Returns.sr_store_credit,Store_Returns.sr_net_loss
FROM Store_Returns where sr_return_amt=100
 

;
---new sql--2024-01-02 17:13:27-----
--- long_chain is ['FileScan FactTable', 'Filter', 'Project']

SELECT Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number,Store_Returns.sr_return_quantity,Store_Returns.sr_return_amt,Store_Returns.sr_return_tax,Store_Returns.sr_return_amt_inc_tax,Store_Returns.sr_fee,Store_Returns.sr_return_ship_cost,Store_Returns.sr_refunded_cash,Store_Returns.sr_reversed_charge,Store_Returns.sr_store_credit,Store_Returns.sr_net_loss
FROM Store_Returns where sr_return_amt=200
 

;
---new sql--2024-01-02 17:13:27-----
--- long_chain is ['FileScan FactTable', 'Filter', 'Project', 'HashAggregate']

SELECT Store_Returns.sr_returned_date_sk, Store_Returns.sr_return_time_sk, Store_Returns.sr_item_sk, Store_Returns.sr_customer_sk, Store_Returns.sr_cdemo_sk, Store_Returns.sr_hdemo_sk, Store_Returns.sr_addr_sk, Store_Returns.sr_store_sk, Store_Returns.sr_reason_sk, Store_Returns.sr_ticket_number, COUNT(sr_return_quantity) AS sr_return_quantity_COUNT
FROM Store_Returns where sr_return_quantity=100
 GROUP BY Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number

;
---new sql--2024-01-02 17:13:41-----
--- long_chain is ['FileScan FactTable', 'Filter', 'Project', 'Window']

SELECT Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number,Store_Returns.sr_return_quantity,Store_Returns.sr_return_amt,Store_Returns.sr_return_tax,Store_Returns.sr_return_amt_inc_tax,Store_Returns.sr_fee,Store_Returns.sr_return_ship_cost,Store_Returns.sr_refunded_cash,Store_Returns.sr_reversed_charge,Store_Returns.sr_store_credit,Store_Returns.sr_net_loss,row_number() over (partition by sr_returned_date_sk order by sr_store_credit) as r0
FROM Store_Returns where sr_return_quantity=100
 

;
---new sql--2024-01-02 17:13:41-----
--- long_chain is ['FileScan FactTable', 'Filter', 'Project']

SELECT Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number,Store_Returns.sr_return_quantity,Store_Returns.sr_return_amt,Store_Returns.sr_return_tax,Store_Returns.sr_return_amt_inc_tax,Store_Returns.sr_fee,Store_Returns.sr_return_ship_cost,Store_Returns.sr_refunded_cash,Store_Returns.sr_reversed_charge,Store_Returns.sr_store_credit,Store_Returns.sr_net_loss,row_number() over (partition by sr_returned_date_sk order by sr_return_amt) as r0
FROM Store_Returns where sr_return_amt=300
 

;
---new sql--2024-01-02 17:13:41-----
--- long_chain is ['FileScan FactTable', 'Filter', 'Project', 'HashAggregate']

SELECT Store_Returns.sr_returned_date_sk, Store_Returns.sr_return_time_sk, Store_Returns.sr_item_sk, Store_Returns.sr_customer_sk, Store_Returns.sr_cdemo_sk, Store_Returns.sr_hdemo_sk, Store_Returns.sr_addr_sk, Store_Returns.sr_store_sk, Store_Returns.sr_reason_sk, Store_Returns.sr_ticket_number, SUM(sr_return_quantity) AS sr_return_quantity_SUM, COUNT(sr_return_amt) AS sr_return_amt_COUNT, MAX(sr_return_tax) AS sr_return_tax_MAX,row_number() over (partition by sr_returned_date_sk order by sr_ticket_number) as r0
FROM Store_Returns where sr_return_amt=200
 GROUP BY Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number

;
---new sql--2024-01-02 17:15:50-----
--- long_chain is ['FileScan FactTable', 'Filter', 'Project']

SELECT Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number,Store_Returns.sr_return_quantity,Store_Returns.sr_return_amt,Store_Returns.sr_return_tax,Store_Returns.sr_return_amt_inc_tax,Store_Returns.sr_fee,Store_Returns.sr_return_ship_cost,Store_Returns.sr_refunded_cash,Store_Returns.sr_reversed_charge,Store_Returns.sr_store_credit,Store_Returns.sr_net_loss
FROM Store_Returns where sr_return_quantity=200
 

;
---new sql--2024-01-02 17:15:50-----
--- long_chain is ['FileScan FactTable', 'Filter', 'Project', 'Window']

SELECT Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number,Store_Returns.sr_return_quantity,Store_Returns.sr_return_amt,Store_Returns.sr_return_tax,Store_Returns.sr_return_amt_inc_tax,Store_Returns.sr_fee,Store_Returns.sr_return_ship_cost,Store_Returns.sr_refunded_cash,Store_Returns.sr_reversed_charge,Store_Returns.sr_store_credit,Store_Returns.sr_net_loss,row_number() over (partition by sr_returned_date_sk order by sr_return_ship_cost) as r0
FROM Store_Returns where sr_return_amt=100
 

;
---new sql--2024-01-02 17:15:50-----
--- long_chain is ['FileScan FactTable', 'Filter', 'Project']

SELECT Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number,Store_Returns.sr_return_quantity,Store_Returns.sr_return_amt,Store_Returns.sr_return_tax,Store_Returns.sr_return_amt_inc_tax,Store_Returns.sr_fee,Store_Returns.sr_return_ship_cost,Store_Returns.sr_refunded_cash,Store_Returns.sr_reversed_charge,Store_Returns.sr_store_credit,Store_Returns.sr_net_loss,row_number() over (partition by sr_returned_date_sk order by sr_store_sk) as r0
FROM Store_Returns where sr_return_quantity=200
 

;
---new sql--2024-01-02 17:16:24-----
--- long_chain is ['FileScan FactTable', 'Filter', 'Project']

SELECT Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number,Store_Returns.sr_return_quantity,Store_Returns.sr_return_amt,Store_Returns.sr_return_tax,Store_Returns.sr_return_amt_inc_tax,Store_Returns.sr_fee,Store_Returns.sr_return_ship_cost,Store_Returns.sr_refunded_cash,Store_Returns.sr_reversed_charge,Store_Returns.sr_store_credit,Store_Returns.sr_net_loss
FROM Store_Returns where sr_return_quantity=300
 

;
---new sql--2024-01-02 17:16:24-----
--- long_chain is ['FileScan FactTable', 'Filter', 'Project', 'Window']

SELECT Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number,Store_Returns.sr_return_quantity,Store_Returns.sr_return_amt,Store_Returns.sr_return_tax,Store_Returns.sr_return_amt_inc_tax,Store_Returns.sr_fee,Store_Returns.sr_return_ship_cost,Store_Returns.sr_refunded_cash,Store_Returns.sr_reversed_charge,Store_Returns.sr_store_credit,Store_Returns.sr_net_loss,row_number() over (partition by sr_returned_date_sk order by sr_return_amt_inc_tax) as r0
FROM Store_Returns where sr_return_quantity=200
 

;
---new sql--2024-01-02 17:16:24-----
--- long_chain is ['FileScan FactTable', 'Filter', 'Project']

SELECT Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number,Store_Returns.sr_return_quantity,Store_Returns.sr_return_amt,Store_Returns.sr_return_tax,Store_Returns.sr_return_amt_inc_tax,Store_Returns.sr_fee,Store_Returns.sr_return_ship_cost,Store_Returns.sr_refunded_cash,Store_Returns.sr_reversed_charge,Store_Returns.sr_store_credit,Store_Returns.sr_net_loss,row_number() over (partition by sr_returned_date_sk order by sr_return_time_sk) as r0
FROM Store_Returns where sr_return_quantity=100
 

;
---new sql--2024-01-02 17:17:15-----
--- long_chain is ['FileScan FactTable', 'Filter', 'Project']

SELECT Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number,Store_Returns.sr_return_quantity,Store_Returns.sr_return_amt,Store_Returns.sr_return_tax,Store_Returns.sr_return_amt_inc_tax,Store_Returns.sr_fee,Store_Returns.sr_return_ship_cost,Store_Returns.sr_refunded_cash,Store_Returns.sr_reversed_charge,Store_Returns.sr_store_credit,Store_Returns.sr_net_loss
FROM Store_Returns where sr_return_amt=100
 

;
---new sql--2024-01-02 17:17:15-----
--- long_chain is ['FileScan FactTable', 'Filter', 'Project', 'Window']

SELECT Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number,Store_Returns.sr_return_quantity,Store_Returns.sr_return_amt,Store_Returns.sr_return_tax,Store_Returns.sr_return_amt_inc_tax,Store_Returns.sr_fee,Store_Returns.sr_return_ship_cost,Store_Returns.sr_refunded_cash,Store_Returns.sr_reversed_charge,Store_Returns.sr_store_credit,Store_Returns.sr_net_loss,row_number() over (partition by sr_returned_date_sk order by sr_store_credit) as r0
FROM Store_Returns where sr_return_amt=300
 

;
---new sql--2024-01-02 17:17:15-----
--- long_chain is ['FileScan FactTable', 'Filter', 'Project']

SELECT Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number,Store_Returns.sr_return_quantity,Store_Returns.sr_return_amt,Store_Returns.sr_return_tax,Store_Returns.sr_return_amt_inc_tax,Store_Returns.sr_fee,Store_Returns.sr_return_ship_cost,Store_Returns.sr_refunded_cash,Store_Returns.sr_reversed_charge,Store_Returns.sr_store_credit,Store_Returns.sr_net_loss,row_number() over (partition by sr_returned_date_sk order by sr_returned_date_sk) as r0
FROM Store_Returns where sr_return_quantity=100
 

;
---new sql--2024-01-02 17:18:48-----
--- long_chain is ['FileScan FactTable', 'Filter', 'Project']

SELECT Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number,Store_Returns.sr_return_quantity,Store_Returns.sr_return_amt,Store_Returns.sr_return_tax,Store_Returns.sr_return_amt_inc_tax,Store_Returns.sr_fee,Store_Returns.sr_return_ship_cost,Store_Returns.sr_refunded_cash,Store_Returns.sr_reversed_charge,Store_Returns.sr_store_credit,Store_Returns.sr_net_loss
FROM Store_Returns where sr_return_quantity=300
 

;
---new sql--2024-01-02 17:18:49-----
--- long_chain is ['FileScan FactTable', 'Filter', 'Project']

SELECT Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number,Store_Returns.sr_return_quantity,Store_Returns.sr_return_amt,Store_Returns.sr_return_tax,Store_Returns.sr_return_amt_inc_tax,Store_Returns.sr_fee,Store_Returns.sr_return_ship_cost,Store_Returns.sr_refunded_cash,Store_Returns.sr_reversed_charge,Store_Returns.sr_store_credit,Store_Returns.sr_net_loss
FROM Store_Returns where sr_return_quantity=100
 limit 100

;
---new sql--2024-01-02 17:18:49-----
--- long_chain is ['FileScan FactTable', 'Filter', 'Project']

SELECT Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number,Store_Returns.sr_return_quantity,Store_Returns.sr_return_amt,Store_Returns.sr_return_tax,Store_Returns.sr_return_amt_inc_tax,Store_Returns.sr_fee,Store_Returns.sr_return_ship_cost,Store_Returns.sr_refunded_cash,Store_Returns.sr_reversed_charge,Store_Returns.sr_store_credit,Store_Returns.sr_net_loss
FROM Store_Returns where sr_return_amt=200
 

;
---new sql--2024-01-02 17:29:39-----
--- long_chain is ['FileScan FactTable', 'Filter', 'Project', 'HashAggregate', 'BroadcastHashJoin', 'Filter', 'sort', 'SortMergeJoin', 'BroadcastHashJoin', 'Project', 'BroadcastHashJoin', 'Project', '']
WITH 
tb1 AS (
SELECT Store_Returns.sr_returned_date_sk, Store_Returns.sr_return_time_sk, Store_Returns.sr_item_sk, Store_Returns.sr_customer_sk, Store_Returns.sr_cdemo_sk, Store_Returns.sr_hdemo_sk, Store_Returns.sr_addr_sk, Store_Returns.sr_store_sk, Store_Returns.sr_reason_sk, Store_Returns.sr_ticket_number, SUM(sr_return_quantity) AS sr_return_quantity_SUM, MAX(sr_return_amt) AS sr_return_amt_MAX, SUM(sr_return_tax) AS sr_return_tax_SUM
FROM Store_Returns where sr_return_amt=300
 GROUP BY Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number
) , 
 tb2 AS (
SELECT Store_Sales.ss_sold_date_sk, Store_Sales.ss_sold_time_sk, Store_Sales.ss_item_sk, Store_Sales.ss_customer_sk, Store_Sales.ss_cdemo_sk, Store_Sales.ss_hdemo_sk, Store_Sales.ss_addr_sk, Store_Sales.ss_store_sk, Store_Sales.ss_promo_sk, Store_Sales.ss_ticket_number, MAX(ss_quantity) AS ss_quantity_MAX
FROM Store_Sales where ss_list_price=12
 GROUP BY Store_Sales.ss_sold_date_sk,Store_Sales.ss_sold_time_sk,Store_Sales.ss_item_sk,Store_Sales.ss_customer_sk,Store_Sales.ss_cdemo_sk,Store_Sales.ss_hdemo_sk,Store_Sales.ss_addr_sk,Store_Sales.ss_store_sk,Store_Sales.ss_promo_sk,Store_Sales.ss_ticket_number
) , 
tb3 AS (
SELECT tb1.sr_returned_date_sk,tb1.sr_return_time_sk,tb1.sr_item_sk,tb1.sr_customer_sk,tb1.sr_cdemo_sk,tb1.sr_hdemo_sk,tb1.sr_addr_sk,tb1.sr_store_sk,tb1.sr_reason_sk,tb1.sr_ticket_number,tb1.sr_return_quantity_SUM,tb1.sr_return_amt_MAX,tb1.sr_return_tax_SUM , tb2.ss_sold_date_sk,tb2.ss_sold_time_sk,tb2.ss_item_sk,tb2.ss_customer_sk,tb2.ss_cdemo_sk,tb2.ss_hdemo_sk,tb2.ss_addr_sk,tb2.ss_store_sk,tb2.ss_promo_sk,tb2.ss_ticket_number,tb2.ss_quantity_MAX
FROM tb1 , tb2
WHERE tb1.sr_customer_sk = tb2.ss_customer_sk and tb1.sr_item_sk = tb2.ss_item_sk and tb1.sr_ticket_number = tb2.ss_ticket_number
 ) , 
 tb4 AS (
SELECT Catalog_Sales.cs_sold_date_sk, Catalog_Sales.cs_sold_time_sk, Catalog_Sales.cs_ship_date_sk, Catalog_Sales.cs_bill_customer_sk, Catalog_Sales.cs_bill_cdemo_sk, Catalog_Sales.cs_bill_hdemo_sk, Catalog_Sales.cs_bill_addr_sk, Catalog_Sales.cs_ship_customer_sk, Catalog_Sales.cs_ship_cdemo_sk, Catalog_Sales.cs_ship_hdemo_sk, Catalog_Sales.cs_ship_addr_sk, Catalog_Sales.cs_call_center_sk, Catalog_Sales.cs_catalog_page_sk, Catalog_Sales.cs_ship_mode_sk, Catalog_Sales.cs_warehouse_sk, Catalog_Sales.cs_item_sk, Catalog_Sales.cs_promo_sk, Catalog_Sales.cs_order_number, MAX(cs_quantity) AS cs_quantity_MAX, AVG(cs_wholesale_cost) AS cs_wholesale_cost_AVG
FROM Catalog_Sales where cs_net_profit=200
 GROUP BY Catalog_Sales.cs_sold_date_sk,Catalog_Sales.cs_sold_time_sk,Catalog_Sales.cs_ship_date_sk,Catalog_Sales.cs_bill_customer_sk,Catalog_Sales.cs_bill_cdemo_sk,Catalog_Sales.cs_bill_hdemo_sk,Catalog_Sales.cs_bill_addr_sk,Catalog_Sales.cs_ship_customer_sk,Catalog_Sales.cs_ship_cdemo_sk,Catalog_Sales.cs_ship_hdemo_sk,Catalog_Sales.cs_ship_addr_sk,Catalog_Sales.cs_call_center_sk,Catalog_Sales.cs_catalog_page_sk,Catalog_Sales.cs_ship_mode_sk,Catalog_Sales.cs_warehouse_sk,Catalog_Sales.cs_item_sk,Catalog_Sales.cs_promo_sk,Catalog_Sales.cs_order_number
) , 
tb5 AS (
SELECT tb3.sr_returned_date_sk,tb3.sr_return_time_sk,tb3.sr_item_sk,tb3.sr_customer_sk,tb3.sr_cdemo_sk,tb3.sr_hdemo_sk,tb3.sr_addr_sk,tb3.sr_store_sk,tb3.sr_reason_sk,tb3.sr_ticket_number,tb3.sr_return_quantity_SUM,tb3.sr_return_amt_MAX,tb3.sr_return_tax_SUM , tb4.cs_sold_date_sk,tb4.cs_sold_time_sk,tb4.cs_ship_date_sk,tb4.cs_bill_customer_sk,tb4.cs_bill_cdemo_sk,tb4.cs_bill_hdemo_sk,tb4.cs_bill_addr_sk,tb4.cs_ship_customer_sk,tb4.cs_ship_cdemo_sk,tb4.cs_ship_hdemo_sk,tb4.cs_ship_addr_sk,tb4.cs_call_center_sk,tb4.cs_catalog_page_sk,tb4.cs_ship_mode_sk,tb4.cs_warehouse_sk,tb4.cs_item_sk,tb4.cs_promo_sk,tb4.cs_order_number,tb4.cs_quantity_MAX,tb4.cs_wholesale_cost_AVG
FROM tb3 , tb4
WHERE tb3.sr_customer_sk = tb4.cs_bill_customer_sk and tb3.sr_item_sk = tb4.cs_item_sk
 ) , 
 tb6 AS (
SELECT Customer.c_customer_sk, Customer.c_current_cdemo_sk, Customer.c_current_hdemo_sk, Customer.c_current_addr_sk, Customer.c_first_shipto_date_sk, Customer.c_first_sales_date_sk, MAX(cast(cast( c_birth_day as BIGINT)  as string)) AS c_birth_day_MAX, MAX(cast(cast( c_birth_month as BIGINT)  as string)) AS c_birth_month_MAX
FROM Customer 
 
 GROUP BY Customer.c_customer_sk,Customer.c_current_cdemo_sk,Customer.c_current_hdemo_sk,Customer.c_current_addr_sk,Customer.c_first_shipto_date_sk,Customer.c_first_sales_date_sk
) , 
tb7 AS (
SELECT tb5.sr_returned_date_sk,tb5.sr_return_time_sk,tb5.sr_item_sk,tb5.sr_customer_sk,tb5.sr_cdemo_sk,tb5.sr_hdemo_sk,tb5.sr_addr_sk,tb5.sr_store_sk,tb5.sr_reason_sk,tb5.sr_ticket_number,tb5.sr_return_quantity_SUM,tb5.sr_return_amt_MAX,tb5.sr_return_tax_SUM , tb6.c_customer_sk,tb6.c_current_cdemo_sk,tb6.c_current_hdemo_sk,tb6.c_current_addr_sk,tb6.c_first_shipto_date_sk,tb6.c_first_sales_date_sk,tb6.c_birth_day_MAX,tb6.c_birth_month_MAX
FROM tb5 , tb6
WHERE tb5.sr_customer_sk = tb6.c_customer_sk
 ) , 
 tb8 AS (
SELECT Date_Dim.d_date_sk, MAX(cast(cast( d_month_seq as BIGINT)  as string)) AS d_month_seq_MAX, MAX(cast(cast( d_week_seq as BIGINT)  as string)) AS d_week_seq_MAX
FROM Date_Dim where d_moy=7
 GROUP BY Date_Dim.d_date_sk
) 
SELECT tb7.sr_returned_date_sk,tb7.sr_return_time_sk,tb7.sr_item_sk,tb7.sr_customer_sk,tb7.sr_cdemo_sk,tb7.sr_hdemo_sk,tb7.sr_addr_sk,tb7.sr_store_sk,tb7.sr_reason_sk,tb7.sr_ticket_number,tb7.sr_return_quantity_SUM,tb7.sr_return_amt_MAX,tb7.sr_return_tax_SUM , tb8.d_month_seq_MAX,tb8.d_week_seq_MAX
FROM tb7 , tb8
WHERE tb7.sr_returned_date_sk = tb8.d_date_sk
 
 LIMIT 100;
---new sql--2024-01-02 17:29:39-----
--- long_chain is ['FileScan FactTable', 'Filter', 'Project', 'HashAggregate', 'BroadcastHashJoin', 'Project', 'BroadcastHashJoin', 'Project', 'BroadcastHashJoin', 'Project', 'BroadcastHashJoin', 'Project', 'Sort', 'Window', 'Sort', 'Window', 'Sort', 'Window', 'Sort', 'Window', 'Sort', 'Window', 'Sort', 'Window', '']
WITH 
tb10 AS (
SELECT Store_Returns.sr_returned_date_sk, Store_Returns.sr_return_time_sk, Store_Returns.sr_item_sk, Store_Returns.sr_customer_sk, Store_Returns.sr_cdemo_sk, Store_Returns.sr_hdemo_sk, Store_Returns.sr_addr_sk, Store_Returns.sr_store_sk, Store_Returns.sr_reason_sk, Store_Returns.sr_ticket_number, Store_Returns.sr_returned_date_sk, Store_Returns.sr_return_time_sk, Store_Returns.sr_item_sk, Store_Returns.sr_customer_sk, Store_Returns.sr_cdemo_sk, Store_Returns.sr_hdemo_sk, Store_Returns.sr_addr_sk, Store_Returns.sr_store_sk, Store_Returns.sr_reason_sk, Store_Returns.sr_ticket_number, SUM(sr_return_quantity) AS sr_return_quantity_SUM, MAX(sr_return_amt) AS sr_return_amt_MAX, SUM(sr_return_tax) AS sr_return_tax_SUM
FROM Store_Returns where sr_return_quantity=200
 GROUP BY Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number,Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number
) , 
 tb11 AS (
SELECT Store_Sales.ss_sold_date_sk,Store_Sales.ss_sold_time_sk,Store_Sales.ss_item_sk,Store_Sales.ss_customer_sk,Store_Sales.ss_cdemo_sk,Store_Sales.ss_hdemo_sk,Store_Sales.ss_addr_sk,Store_Sales.ss_store_sk,Store_Sales.ss_promo_sk,Store_Sales.ss_ticket_number,Store_Sales.ss_quantity,Store_Sales.ss_wholesale_cost,Store_Sales.ss_list_price,Store_Sales.ss_sales_price,Store_Sales.ss_ext_discount_amt,Store_Sales.ss_ext_sales_price,Store_Sales.ss_ext_wholesale_cost,Store_Sales.ss_ext_list_price,Store_Sales.ss_ext_tax,Store_Sales.ss_coupon_amt,Store_Sales.ss_net_paid,Store_Sales.ss_net_paid_inc_tax,Store_Sales.ss_net_profit
FROM Store_Sales 
 
 
) , 
tb12 AS (
SELECT tb10.sr_returned_date_sk,tb10.sr_return_time_sk,tb10.sr_item_sk,tb10.sr_customer_sk,tb10.sr_cdemo_sk,tb10.sr_hdemo_sk,tb10.sr_addr_sk,tb10.sr_store_sk,tb10.sr_reason_sk,tb10.sr_ticket_number , tb11.ss_sold_date_sk,tb11.ss_sold_time_sk,tb11.ss_item_sk,tb11.ss_customer_sk,tb11.ss_cdemo_sk,tb11.ss_hdemo_sk,tb11.ss_addr_sk,tb11.ss_store_sk,tb11.ss_promo_sk,tb11.ss_ticket_number,tb11.ss_quantity,tb11.ss_wholesale_cost,tb11.ss_list_price,tb11.ss_sales_price,tb11.ss_ext_discount_amt,tb11.ss_ext_sales_price,tb11.ss_ext_wholesale_cost,tb11.ss_ext_list_price,tb11.ss_ext_tax,tb11.ss_coupon_amt,tb11.ss_net_paid,tb11.ss_net_paid_inc_tax,tb11.ss_net_profit
FROM tb10 , tb11
WHERE tb10.sr_item_sk = tb11.ss_item_sk and tb10.sr_customer_sk = tb11.ss_customer_sk and tb10.sr_ticket_number = tb11.ss_ticket_number
 ) , 
 tb13 AS (
SELECT Catalog_Sales.cs_sold_date_sk, Catalog_Sales.cs_sold_time_sk, Catalog_Sales.cs_ship_date_sk, Catalog_Sales.cs_bill_customer_sk, Catalog_Sales.cs_bill_cdemo_sk, Catalog_Sales.cs_bill_hdemo_sk, Catalog_Sales.cs_bill_addr_sk, Catalog_Sales.cs_ship_customer_sk, Catalog_Sales.cs_ship_cdemo_sk, Catalog_Sales.cs_ship_hdemo_sk, Catalog_Sales.cs_ship_addr_sk, Catalog_Sales.cs_call_center_sk, Catalog_Sales.cs_catalog_page_sk, Catalog_Sales.cs_ship_mode_sk, Catalog_Sales.cs_warehouse_sk, Catalog_Sales.cs_item_sk, Catalog_Sales.cs_promo_sk, Catalog_Sales.cs_order_number, MAX(cast(cast( cs_quantity as BIGINT)  as string)) AS cs_quantity_MAX
FROM Catalog_Sales where cs_net_profit=200
 GROUP BY Catalog_Sales.cs_sold_date_sk,Catalog_Sales.cs_sold_time_sk,Catalog_Sales.cs_ship_date_sk,Catalog_Sales.cs_bill_customer_sk,Catalog_Sales.cs_bill_cdemo_sk,Catalog_Sales.cs_bill_hdemo_sk,Catalog_Sales.cs_bill_addr_sk,Catalog_Sales.cs_ship_customer_sk,Catalog_Sales.cs_ship_cdemo_sk,Catalog_Sales.cs_ship_hdemo_sk,Catalog_Sales.cs_ship_addr_sk,Catalog_Sales.cs_call_center_sk,Catalog_Sales.cs_catalog_page_sk,Catalog_Sales.cs_ship_mode_sk,Catalog_Sales.cs_warehouse_sk,Catalog_Sales.cs_item_sk,Catalog_Sales.cs_promo_sk,Catalog_Sales.cs_order_number
) , 
tb14 AS (
SELECT tb12.sr_returned_date_sk,tb12.sr_return_time_sk,tb12.sr_item_sk,tb12.sr_customer_sk,tb12.sr_cdemo_sk,tb12.sr_hdemo_sk,tb12.sr_addr_sk,tb12.sr_store_sk,tb12.sr_reason_sk,tb12.sr_ticket_number , tb13.cs_sold_date_sk,tb13.cs_sold_time_sk,tb13.cs_ship_date_sk,tb13.cs_bill_customer_sk,tb13.cs_bill_cdemo_sk,tb13.cs_bill_hdemo_sk,tb13.cs_bill_addr_sk,tb13.cs_ship_customer_sk,tb13.cs_ship_cdemo_sk,tb13.cs_ship_hdemo_sk,tb13.cs_ship_addr_sk,tb13.cs_call_center_sk,tb13.cs_catalog_page_sk,tb13.cs_ship_mode_sk,tb13.cs_warehouse_sk,tb13.cs_item_sk,tb13.cs_promo_sk,tb13.cs_order_number,tb13.cs_quantity_MAX
FROM tb12 , tb13
WHERE tb12.sr_item_sk = tb13.cs_item_sk
 ) , 
 tb15 AS (
SELECT Customer.c_customer_sk, Customer.c_current_cdemo_sk, Customer.c_current_hdemo_sk, Customer.c_current_addr_sk, Customer.c_first_shipto_date_sk, Customer.c_first_sales_date_sk, MAX(c_birth_day) AS c_birth_day_MAX, SUM(c_birth_month) AS c_birth_month_SUM
FROM Customer 
 
 GROUP BY Customer.c_customer_sk,Customer.c_current_cdemo_sk,Customer.c_current_hdemo_sk,Customer.c_current_addr_sk,Customer.c_first_shipto_date_sk,Customer.c_first_sales_date_sk
) , 
tb16 AS (
SELECT tb14.sr_returned_date_sk,tb14.sr_return_time_sk,tb14.sr_item_sk,tb14.sr_customer_sk,tb14.sr_cdemo_sk,tb14.sr_hdemo_sk,tb14.sr_addr_sk,tb14.sr_store_sk,tb14.sr_reason_sk,tb14.sr_ticket_number , tb15.c_customer_sk,tb15.c_current_cdemo_sk,tb15.c_current_hdemo_sk,tb15.c_current_addr_sk,tb15.c_first_shipto_date_sk,tb15.c_first_sales_date_sk,tb15.c_birth_day_MAX,tb15.c_birth_month_SUM
FROM tb14 , tb15
WHERE tb14.sr_customer_sk = tb15.c_customer_sk
 ) , 
 tb17 AS (
SELECT Date_Dim.d_date_sk, SUM(d_month_seq) AS d_month_seq_SUM, SUM(d_week_seq) AS d_week_seq_SUM, SUM(d_quarter_seq) AS d_quarter_seq_SUM
FROM Date_Dim where d_quarter_name ='2001Q2'
 GROUP BY Date_Dim.d_date_sk
) 
SELECT tb16.sr_returned_date_sk,tb16.sr_return_time_sk,tb16.sr_item_sk,tb16.sr_customer_sk,tb16.sr_cdemo_sk,tb16.sr_hdemo_sk,tb16.sr_addr_sk,tb16.sr_store_sk,tb16.sr_reason_sk,tb16.sr_ticket_number , tb17.d_month_seq_SUM,tb17.d_week_seq_SUM,tb17.d_quarter_seq_SUM,row_number() over (partition by sr_returned_date_sk order by sr_returned_date_sk) as r0,row_number() over (partition by sr_returned_date_sk order by sr_addr_sk) as r1,row_number() over (partition by sr_returned_date_sk order by sr_hdemo_sk) as r2,row_number() over (partition by sr_returned_date_sk order by sr_hdemo_sk) as r3,row_number() over (partition by sr_returned_date_sk order by sr_hdemo_sk) as r4,row_number() over (partition by sr_returned_date_sk order by sr_store_sk) as r5
FROM tb16 , tb17
WHERE tb16.sr_returned_date_sk = tb17.d_date_sk
 
 LIMIT 100;
---new sql--2024-01-02 17:29:39-----
--- long_chain is ['FileScan FactTable', 'Filter', 'BroadcastHashJoin', 'Project', 'BroadcastHashJoin', 'Project', 'BroadcastHashJoin', 'Project', 'BroadcastHashJoin', 'Project', 'Sort', 'Window', 'Sort', 'Window', 'Sort', 'Window', 'Sort', 'Window', 'Sort', 'Window', 'Sort', 'Window', '']
WITH 
tb19 AS (
SELECT Store_Returns.sr_returned_date_sk, Store_Returns.sr_return_time_sk, Store_Returns.sr_item_sk, Store_Returns.sr_customer_sk, Store_Returns.sr_cdemo_sk, Store_Returns.sr_hdemo_sk, Store_Returns.sr_addr_sk, Store_Returns.sr_store_sk, Store_Returns.sr_reason_sk, Store_Returns.sr_ticket_number, Store_Returns.sr_returned_date_sk, Store_Returns.sr_return_time_sk, Store_Returns.sr_item_sk, Store_Returns.sr_customer_sk, Store_Returns.sr_cdemo_sk, Store_Returns.sr_hdemo_sk, Store_Returns.sr_addr_sk, Store_Returns.sr_store_sk, Store_Returns.sr_reason_sk, Store_Returns.sr_ticket_number, SUM(sr_return_quantity) AS sr_return_quantity_SUM, MAX(sr_return_amt) AS sr_return_amt_MAX, SUM(sr_return_tax) AS sr_return_tax_SUM
FROM Store_Returns where sr_return_quantity=300
 GROUP BY Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number,Store_Returns.sr_returned_date_sk,Store_Returns.sr_return_time_sk,Store_Returns.sr_item_sk,Store_Returns.sr_customer_sk,Store_Returns.sr_cdemo_sk,Store_Returns.sr_hdemo_sk,Store_Returns.sr_addr_sk,Store_Returns.sr_store_sk,Store_Returns.sr_reason_sk,Store_Returns.sr_ticket_number
) , 
 tb20 AS (
SELECT Store_Sales.ss_sold_date_sk,Store_Sales.ss_sold_time_sk,Store_Sales.ss_item_sk,Store_Sales.ss_customer_sk,Store_Sales.ss_cdemo_sk,Store_Sales.ss_hdemo_sk,Store_Sales.ss_addr_sk,Store_Sales.ss_store_sk,Store_Sales.ss_promo_sk,Store_Sales.ss_ticket_number,Store_Sales.ss_quantity,Store_Sales.ss_wholesale_cost,Store_Sales.ss_list_price,Store_Sales.ss_sales_price,Store_Sales.ss_ext_discount_amt,Store_Sales.ss_ext_sales_price,Store_Sales.ss_ext_wholesale_cost,Store_Sales.ss_ext_list_price,Store_Sales.ss_ext_tax,Store_Sales.ss_coupon_amt,Store_Sales.ss_net_paid,Store_Sales.ss_net_paid_inc_tax,Store_Sales.ss_net_profit
FROM Store_Sales 
 
 
) , 
tb21 AS (
SELECT tb19.sr_returned_date_sk,tb19.sr_return_time_sk,tb19.sr_item_sk,tb19.sr_customer_sk,tb19.sr_cdemo_sk,tb19.sr_hdemo_sk,tb19.sr_addr_sk,tb19.sr_store_sk,tb19.sr_reason_sk,tb19.sr_ticket_number , tb20.ss_sold_date_sk,tb20.ss_sold_time_sk,tb20.ss_item_sk,tb20.ss_customer_sk,tb20.ss_cdemo_sk,tb20.ss_hdemo_sk,tb20.ss_addr_sk,tb20.ss_store_sk,tb20.ss_promo_sk,tb20.ss_ticket_number,tb20.ss_quantity,tb20.ss_wholesale_cost,tb20.ss_list_price,tb20.ss_sales_price,tb20.ss_ext_discount_amt,tb20.ss_ext_sales_price,tb20.ss_ext_wholesale_cost,tb20.ss_ext_list_price,tb20.ss_ext_tax,tb20.ss_coupon_amt,tb20.ss_net_paid,tb20.ss_net_paid_inc_tax,tb20.ss_net_profit
FROM tb19 , tb20
WHERE tb19.sr_customer_sk = tb20.ss_customer_sk
 ) , 
 tb22 AS (
SELECT Catalog_Sales.cs_sold_date_sk,Catalog_Sales.cs_sold_time_sk,Catalog_Sales.cs_ship_date_sk,Catalog_Sales.cs_bill_customer_sk,Catalog_Sales.cs_bill_cdemo_sk,Catalog_Sales.cs_bill_hdemo_sk,Catalog_Sales.cs_bill_addr_sk,Catalog_Sales.cs_ship_customer_sk,Catalog_Sales.cs_ship_cdemo_sk,Catalog_Sales.cs_ship_hdemo_sk,Catalog_Sales.cs_ship_addr_sk,Catalog_Sales.cs_call_center_sk,Catalog_Sales.cs_catalog_page_sk,Catalog_Sales.cs_ship_mode_sk,Catalog_Sales.cs_warehouse_sk,Catalog_Sales.cs_item_sk,Catalog_Sales.cs_promo_sk,Catalog_Sales.cs_order_number,Catalog_Sales.cs_quantity,Catalog_Sales.cs_wholesale_cost,Catalog_Sales.cs_list_price,Catalog_Sales.cs_sales_price,Catalog_Sales.cs_ext_discount_amt,Catalog_Sales.cs_ext_sales_price,Catalog_Sales.cs_ext_wholesale_cost,Catalog_Sales.cs_ext_list_price,Catalog_Sales.cs_ext_tax,Catalog_Sales.cs_coupon_amt,Catalog_Sales.cs_ext_ship_cost,Catalog_Sales.cs_net_paid,Catalog_Sales.cs_net_paid_inc_tax,Catalog_Sales.cs_net_paid_inc_ship,Catalog_Sales.cs_net_paid_inc_ship_tax,Catalog_Sales.cs_net_profit
FROM Catalog_Sales 
 
 
) , 
tb23 AS (
SELECT tb21.sr_returned_date_sk,tb21.sr_return_time_sk,tb21.sr_item_sk,tb21.sr_customer_sk,tb21.sr_cdemo_sk,tb21.sr_hdemo_sk,tb21.sr_addr_sk,tb21.sr_store_sk,tb21.sr_reason_sk,tb21.sr_ticket_number , tb22.cs_sold_date_sk,tb22.cs_sold_time_sk,tb22.cs_ship_date_sk,tb22.cs_bill_customer_sk,tb22.cs_bill_cdemo_sk,tb22.cs_bill_hdemo_sk,tb22.cs_bill_addr_sk,tb22.cs_ship_customer_sk,tb22.cs_ship_cdemo_sk,tb22.cs_ship_hdemo_sk,tb22.cs_ship_addr_sk,tb22.cs_call_center_sk,tb22.cs_catalog_page_sk,tb22.cs_ship_mode_sk,tb22.cs_warehouse_sk,tb22.cs_item_sk,tb22.cs_promo_sk,tb22.cs_order_number,tb22.cs_quantity,tb22.cs_wholesale_cost,tb22.cs_list_price,tb22.cs_sales_price,tb22.cs_ext_discount_amt,tb22.cs_ext_sales_price,tb22.cs_ext_wholesale_cost,tb22.cs_ext_list_price,tb22.cs_ext_tax,tb22.cs_coupon_amt,tb22.cs_ext_ship_cost,tb22.cs_net_paid,tb22.cs_net_paid_inc_tax,tb22.cs_net_paid_inc_ship,tb22.cs_net_paid_inc_ship_tax,tb22.cs_net_profit
FROM tb21 , tb22
WHERE tb21.sr_customer_sk = tb22.cs_bill_customer_sk and tb21.sr_item_sk = tb22.cs_item_sk
 ) , 
 tb24 AS (
SELECT Customer.c_customer_sk, Customer.c_current_cdemo_sk, Customer.c_current_hdemo_sk, Customer.c_current_addr_sk, Customer.c_first_shipto_date_sk, Customer.c_first_sales_date_sk, MAX(cast(cast( c_birth_day as BIGINT)  as string)) AS c_birth_day_MAX, MAX(cast(cast( c_birth_month as BIGINT)  as string)) AS c_birth_month_MAX, MAX(cast(cast( c_birth_year as BIGINT)  as string)) AS c_birth_year_MAX
FROM Customer 
 
 GROUP BY Customer.c_customer_sk,Customer.c_current_cdemo_sk,Customer.c_current_hdemo_sk,Customer.c_current_addr_sk,Customer.c_first_shipto_date_sk,Customer.c_first_sales_date_sk
) , 
tb25 AS (
SELECT tb23.sr_returned_date_sk,tb23.sr_return_time_sk,tb23.sr_item_sk,tb23.sr_customer_sk,tb23.sr_cdemo_sk,tb23.sr_hdemo_sk,tb23.sr_addr_sk,tb23.sr_store_sk,tb23.sr_reason_sk,tb23.sr_ticket_number , tb24.c_customer_sk,tb24.c_current_cdemo_sk,tb24.c_current_hdemo_sk,tb24.c_current_addr_sk,tb24.c_first_shipto_date_sk,tb24.c_first_sales_date_sk,tb24.c_birth_day_MAX,tb24.c_birth_month_MAX,tb24.c_birth_year_MAX
FROM tb23 , tb24
WHERE tb23.sr_customer_sk = tb24.c_customer_sk
 ) , 
 tb26 AS (
SELECT Date_Dim.d_date_sk, MAX(d_month_seq) AS d_month_seq_MAX
FROM Date_Dim where d_quarter_name ='2001Q2'
 GROUP BY Date_Dim.d_date_sk
) 
SELECT tb25.sr_returned_date_sk,tb25.sr_return_time_sk,tb25.sr_item_sk,tb25.sr_customer_sk,tb25.sr_cdemo_sk,tb25.sr_hdemo_sk,tb25.sr_addr_sk,tb25.sr_store_sk,tb25.sr_reason_sk,tb25.sr_ticket_number , tb26.d_month_seq_MAX,row_number() over (partition by sr_returned_date_sk order by sr_ticket_number) as r0,row_number() over (partition by sr_returned_date_sk order by sr_cdemo_sk) as r1,row_number() over (partition by sr_returned_date_sk order by sr_customer_sk) as r2,row_number() over (partition by sr_returned_date_sk order by sr_reason_sk) as r3,row_number() over (partition by sr_returned_date_sk order by sr_item_sk) as r4,row_number() over (partition by sr_returned_date_sk order by sr_customer_sk) as r5
FROM tb25 , tb26
WHERE tb25.sr_returned_date_sk = tb26.d_date_sk
 
 LIMIT 100;