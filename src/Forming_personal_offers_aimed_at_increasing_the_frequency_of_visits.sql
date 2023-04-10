create or replace function fnc_determination_reward(
    maximum_churn_index numeric,
    maximum_share_of_transactions_with_a_discount numeric,
    allowable_share_of_margin numeric
)
    returns table
            (
                customer_id          bigint,
                group_name           varchar,
                offer_discount_depth integer
            )
    language plpgsql
as
$$
begin
    return query
        (with Unfiltred_Determination_reward
                  as (select distinct common_data.customer_id,
                                      common_data.group_name,
                                      group_affinity_index,
                                      (ceil((group_minimum_discount::decimal(5, 2) * 100) / 5) * 5)::int as discount
                      from common_data
                      inner join view_groups using (customer_id, group_id)
                      where group_minimum_discount::decimal(5, 2) > 0
                        and group_churn_rate < maximum_churn_index
                        and group_discount_share < maximum_share_of_transactions_with_a_discount / 100.0
                        and ceil((group_minimum_discount::decimal(5, 2) * 100) / 5) * 5 <
                            ((sku_retail_price - sku_purchase_price) * allowable_share_of_margin /
                             sku_retail_price))

         select Unfiltred_Determination_reward.customer_id,
                Unfiltred_Determination_reward.group_name,
                Unfiltred_Determination_reward.discount
         from Unfiltred_Determination_reward
         where (Unfiltred_Determination_reward.customer_id,
                group_affinity_index) in (select Unfiltred_Determination_reward.customer_id,
                                                 max(group_affinity_index)
                                          from Unfiltred_Determination_reward
                                          group by Unfiltred_Determination_reward.customer_id));
end;
$$;

create or replace function fnc_determines_offers_aimed_increasing_frequency_of_visits(
    in first_date_of_the_period timestamp,
    in last_date_of_the_period timestamp,
    in added_number_of_transactions int,
    in maximum_churn_index numeric,
    in maximum_share_of_transactions_with_a_discount numeric,
    in allowable_margin_share numeric
)
    returns table
            (
                Customer_ID                 bigint,
                Start_Date                  timestamp,
                End_Date                    timestamp,
                Required_Transactions_Count int,
                Group_Name                  varchar,
                Offer_Discount_Depth        int
            )
as
$$
begin
    SET datestyle = "ISO, DMY";
    return query select distinct common_data.customer_id,
                                 first_date_of_the_period,
                                 last_date_of_the_period,
                                 (round(extract(epoch from last_date_of_the_period::timestamp -
                                                           first_date_of_the_period::timestamp)) /
                                  customer_frequency / 86400)::int + added_number_of_transactions,
                                 fnc_determination_reward.group_name,
                                 fnc_determination_reward.offer_discount_depth

                 from common_data
                 inner join view_customers using (customer_id)
                 inner join fnc_determination_reward(maximum_churn_index::numeric,
                                                     maximum_share_of_transactions_with_a_discount::numeric,
                                                     allowable_margin_share::numeric)
                            using (customer_id, group_name);
end
$$ language plpgsql;

SET datestyle = "ISO, DMY";

select *
from fnc_determines_offers_aimed_increasing_frequency_of_visits(first_date_of_the_period := '16.08.2022 00:00:00',
                                                                last_date_of_the_period := '18.08.2022 00:00:00',
                                                                added_number_of_transactions := 6,
                                                                maximum_churn_index := 4,
                                                                maximum_share_of_transactions_with_a_discount := 50,
                                                                allowable_margin_share := 25);