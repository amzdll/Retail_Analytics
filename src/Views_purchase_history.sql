create or replace view view_purchase_history as
select customer_id,
       transaction_id,
       transaction_datetime,
       group_id,
       sum(sku_amount * sku_purchase_price) as Group_Cost,
       sum(sku_summ)                        as Group_Summ,
       sum(sku_summ_paid)                   as Group_Summ_Paid
from Common_Data
group by 1, 2, 3, 4
order by 1, 2, 4;