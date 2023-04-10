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


create or replace function fnc_determination_target_value_average_check_period(
    first_date_of_the_period timestamp,
    last_date_of_the_period timestamp,
    coefficient_of_average_check_increase numeric
)
    returns table
            (
                customer_id            bigint,
                required_check_measure numeric
            )
    language plpgsql
as
$$
begin
    SET datestyle = "ISO, DMY";
    return query select common_data.customer_id,
                        sum(transaction_summ) / count(transaction_id) *
                        coefficient_of_average_check_increase as Required_Check_Measure
                 from common_data
                 where transaction_datetime between first_date_of_the_period and last_date_of_the_period
                 group by common_data.customer_id, coefficient_of_average_check_increase
                 order by common_data.customer_id;
end;
$$;


create or replace function fnc_determination_target_value_average_check_quantity(
    number_of_transactions integer,
    coefficient_of_average_check_increase numeric
)
    returns table
            (
                customer_id            bigint,
                required_check_measure numeric
            )
    language plpgsql
as
$$
begin
    return query select source_for_average_check.customer_id,
                        sum(transaction_summ) / max(count_of_transaction) *
                        coefficient_of_average_check_increase
                 from (select common_data.customer_id,
                              transaction_summ,
                              row_number() over (partition by common_data.customer_id) as count_of_transaction
                       from common_data) source_for_average_check
                 where count_of_transaction < number_of_transactions
                 group by source_for_average_check.customer_id, coefficient_of_average_check_increase;
end;
$$;


create or replace function fnc_determination_offers_growth_of_the_average_check(
    average_check_calculation_method integer default 1,
    first_date_of_the_period timestamp default null,
    last_date_of_the_period timestamp default null,
    number_of_transactions integer default null,
    coefficient_of_average_check_increase numeric default 1,
    maximum_churn_index numeric default 1,
    maximum_share_of_transactions_with_a_discount numeric default 100,
    allowable_share_of_margin numeric default 100)
    returns table
            (
                customer_id            bigint,
                group_name             character varying,
                required_check_measure numeric,
                offer_discount_depth   integer
            )
    language plpgsql
as
$$
begin
    SET datestyle = "ISO, DMY";
    if average_check_calculation_method = 1 then
        if first_date_of_the_period is null or
           (first_date_of_the_period) < (select min(transaction_datetime) from view_purchase_history) or
           first_date_of_the_period > last_date_of_the_period
        then
            first_date_of_the_period = (select min(transaction_datetime) from view_purchase_history);
        end if;
        if last_date_of_the_period is null or
           last_date_of_the_period > (select * from date_of_analysis_formation) or
           last_date_of_the_period < first_date_of_the_period
        then
            last_date_of_the_period = (select * from date_of_analysis_formation);
        end if;
        return query select fnc_Determination_reward.customer_id,
                            fnc_Determination_reward.group_name,
                            fnc_Determination_target_value_average_check_period.Required_Check_Measure,
                            fnc_Determination_reward.Offer_Discount_Depth
                     from fnc_Determination_target_value_average_check_period(first_date_of_the_period,
                                                                              last_date_of_the_period,
                                                                              coefficient_of_average_check_increase
                         )

                     inner join fnc_Determination_reward(maximum_churn_index,
                                                         maximum_share_of_transactions_with_a_discount,
                                                         allowable_share_of_margin) using (customer_id);
    elseif average_check_calculation_method = 2 then
        if number_of_transactions is null or number_of_transactions < 0 then
            number_of_transactions = (select max(count)
                                      from (select count(transaction_id) as count
                                            from common_data
                                            group by common_data.customer_id) max_count_of_transactions);
        end if;
        return query select fnc_Determination_reward.customer_id,
                            fnc_Determination_reward.group_name,
                            fnc_Determination_target_value_average_check_quantity.Required_Check_Measure,
                            fnc_Determination_reward.Offer_Discount_Depth
                     from fnc_Determination_target_value_average_check_quantity(number_of_transactions,
                                                                                coefficient_of_average_check_increase)
                     inner join fnc_Determination_reward(maximum_churn_index,
                                                         maximum_share_of_transactions_with_a_discount,
                                                         allowable_share_of_margin) using (customer_id);
    end if;
end;
$$;


select *
from fnc_determination_offers_growth_of_the_average_check(
        average_check_calculation_method := 2,
        number_of_transactions := 80,
        first_date_of_the_period := '02.10.2022 00:00:00',
        last_date_of_the_period := '13.03.2023 00:00:00',
        coefficient_of_average_check_increase := 1.30,
        maximum_churn_index := 4,
        maximum_share_of_transactions_with_a_discount := 80,
        allowable_share_of_margin := 50);