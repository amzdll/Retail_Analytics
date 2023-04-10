create or replace view view_periods as
with Group_Without_Discount
         as (select customer_id,
                    group_id,
                    min(sku_discount / sku_summ) as Group_Min_Discount
             from Common_Data
             where sku_discount = 0
             group by customer_id, group_id
             order by 1, 2),
     Group_With_Discount
         as (select customer_id,
                    group_id,
                    min(sku_discount / sku_summ) as Group_Min_Discount
             from Common_Data
             where sku_discount > 0
             group by customer_id, group_id
             order by 1, 2),
     Group_Min_Discount
         as (select coalesce(Group_With_Discount.customer_id, Group_Without_Discount.customer_id) as customer_id,
                    coalesce(Group_With_Discount.group_id, Group_Without_Discount.group_id)       as group_id,
                    coalesce(Group_With_Discount.Group_Min_Discount, 0)                           as Group_Min_Discount
             from Group_With_Discount
             full join Group_Without_Discount
                       on Group_Without_Discount.customer_id = Group_With_Discount.customer_id and
                          Group_Without_Discount.group_id = Group_With_Discount.group_id),
     Common_Fields
         as (select customer_id,
                    group_id,
                    min(transaction_datetime) as First_Group_Purchase_Date,
                    max(transaction_datetime) as Last_Group_Purchase_Date,
                    count(group_id)           as Group_Purchase,
                    (extract(day from (max(transaction_datetime) - min(transaction_datetime))) + 1) /
                    count(group_id)           as Group_Frequency
             from view_purchase_history
             group by 1, 2
             order by 1, 2)
select Common_Fields.customer_id,
       Common_Fields.group_id,
       First_Group_Purchase_Date,
       Last_Group_Purchase_Date,
       Group_Purchase,
       Group_Frequency,
       Group_Min_Discount
from Common_Fields
inner join Group_Min_Discount using (customer_id, group_id)
order by 1, 2;