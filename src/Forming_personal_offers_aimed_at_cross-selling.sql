create or replace function fnc_offers_focused_on_cross_selling(
    in number_of_groups int,
    in maximum_churn_index numeric,
    in maximum_consumption_stability_index numeric,
    in maximum_sku_share numeric,
    in allowable_margin_share numeric
)
    returns table
            (
                Customer_ID          int,
                SKU_Name             text,
                Offer_Discount_Depth numeric
            )
as
$$
with source as (select distinct view_groups.customer_id,
                                sku_name,
                                view_groups.group_churn_rate,
                                view_groups.group_stability_index,
                                max(sku_retail_price - sku_purchase_price)
                                over (partition by view_groups.group_id,view_groups.customer_id, sku_id)    as sku_max_margine,
                                count(transaction_store_id) over (partition by sku_id)::float /
                                count(transaction_store_id) over (partition by view_groups.group_id)::float as sku_share_group,
                                case
                                    when ((sku_retail_price - sku_purchase_price)::float *
                                          (allowable_margin_share / 100) / sku_retail_price) <=
                                         ceil(view_groups.group_minimum_discount * 100 / 5) * 5
                                        then ceil(view_groups.group_minimum_discount * 100 / 5) * 5
                                    end                                                                     as Offer_Discount_Depth,
                                dense_rank()
                                over (partition by view_groups.customer_id order by view_groups.group_id)   as number_group
                from view_groups
                inner join sku using (group_id)
                inner join stores s2 using (sku_id))
select distinct on (customer_id) Customer_ID, SKU_Name, Offer_Discount_Depth
from source
where number_of_groups >= number_group
  and maximum_churn_index >= group_churn_rate
  and maximum_consumption_stability_index >= group_stability_index
  and maximum_sku_share >= sku_share_group * 100
  and Offer_Discount_Depth is not null
order by 1;
$$
    language sql;

select *
from fnc_offers_focused_on_cross_selling(
    1, 2, 0.7, 80, 100);
