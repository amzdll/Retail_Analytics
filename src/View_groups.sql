create or replace function Group_Margin_function(in calculation_method text default 'period',
                                                 in count_of_calculation int default null)
    returns table
            (
                result_customer_id bigint,
                result_group_id    bigint,
                Group_Margin       decimal
            )
as
$$
begin
    if calculation_method = 'quantity' then
        return query select customer_id,
                            group_id,
                            sum(sum)
                     from (select customer_id,
                                  group_id,
                                  group_summ_paid - group_cost as sum
                           from view_purchase_history
                           where transaction_datetime <= (select analysis_formation from date_of_analysis_formation)
                           order by transaction_datetime desc
                           limit count_of_calculation) transaction_data
                     group by 1, 2;
    elseif calculation_method = 'period' then
        return query select customer_id,
                            group_id,
                            sum(sum)
                     from (select customer_id,
                                  group_id,
                                  group_summ_paid - group_cost as sum
                           from view_purchase_history
                           where (transaction_datetime::date >=
                                  (select analysis_formation from date_of_analysis_formation)::date -
                                  count_of_calculation or count_of_calculation is null)
                             and transaction_datetime <=
                                 (select analysis_formation from date_of_analysis_formation)) transaction_data
                     group by 1, 2;
    end if;
end;
$$ language plpgsql;


CREATE OR REPLACE VIEW view_groups AS
with Group_Affinity_Index
         as
         (select p.customer_id,
                 p.group_id,
                 (group_purchase::decimal / count(transaction_datetime))
                     as Group_Affinity_Index
          from view_periods p
          inner join view_purchase_history ph on p.customer_id = ph.customer_id and
                                                 ph.transaction_datetime between p.first_group_purchase_date and p.last_group_purchase_date
          group by 1, 2, group_purchase),
     Group_Churn_Rate as
         (select p.customer_id,
                 p.group_id,
                 days::decimal / p.group_frequency
                     as Group_Churn_Rate
          from (select customer_id,
                       group_id,
                       extract(days from (select * from date_of_analysis_formation) - max(transaction_datetime))
                           as days
                from view_purchase_history
                group by customer_id, group_id) count
          inner join view_periods p using (customer_id, group_id)),
     Group_Stability_Index as
         (select customer_id, group_id, avg(deviation) as Group_Stability_Index
          from (select p.customer_id,
                       p.group_id,
                       case
                           when interval_between_purchases.interval < p.group_frequency
                               then ((interval_between_purchases.interval - p.group_frequency) * (-1)) /
                                    p.group_frequency
                           else (interval_between_purchases.interval - p.group_frequency) / p.group_frequency
                           end
                           as deviation
                from (select ph.customer_id,
                             ph.group_id,
                             ph.transaction_datetime,
                             min(extract(days from ph_2.transaction_datetime - ph.transaction_datetime))
                                 as interval
                      from view_purchase_history ph
                      inner join view_purchase_history ph_2 on ph.customer_id = ph_2.customer_id and
                                                               ph.group_id = ph_2.group_id and
                                                               ph.transaction_datetime < ph_2.transaction_datetime
                      group by 1, 2, 3
                      order by 1, 2, 3) interval_between_purchases
                inner join view_periods p using (customer_id, group_id)) relative_deviation
          group by customer_id, group_id),
     Group_Discount_Share as
         (select p.customer_id,
                 p.group_id,
                 count_of_transactions_with_discount::decimal / group_purchase::decimal as Group_Discount_Share
          from view_periods p
          inner join (select customer_id,
                             group_id,
                             count(transaction_id) as count_of_transactions_with_discount
                      from Common_Data
                      where sku_discount > 0
                      group by customer_id, group_id
                      order by 1, 2) discounts using (customer_id, group_id)),
     Group_Minimum_Discount as
         (select customer_id,
                 group_id,
                 group_min_discount as Group_Minimum_Discount
          from view_periods),
     Group_Average_Discount as
         (select customer_id,
                 group_id,
                 sum(group_summ_paid::decimal) /
                 sum(group_summ::decimal) as Group_Average_Discount
          from view_purchase_history
          group by customer_id, group_id
          order by customer_id, group_id)

select gai.customer_id,
       gai.group_id,
       gai.Group_Affinity_Index,
       gcr.Group_Churn_Rate,
       gsi.Group_Stability_Index,
       mf.Group_Margin,
       gds.Group_Discount_Share,
       gms.Group_Minimum_Discount,
       gad.Group_Average_Discount
from Group_Affinity_Index gai
inner join Group_Churn_Rate gcr using (customer_id, group_id)
inner join Group_Stability_Index gsi using (customer_id, group_id)
inner join Group_Discount_Share gds using (customer_id, group_id)
inner join Group_Minimum_Discount gms using (customer_id, group_id)
inner join Group_Average_Discount gad using (customer_id, group_id)
inner join group_margin_function() as mf
           on gai.customer_id = mf.result_customer_id and gai.group_id = mf.result_group_id;