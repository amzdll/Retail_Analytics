create or replace view View_Customers as
(
with Customer_Average_Check
         as (select customer_id,
                    avg(transaction_summ)                                                       as Customer_Average_Check,
                    (row_number() over (order by avg(transaction_summ) desc))::double precision as percent_segment,
                    count(customer_id)
             from common_data
             group by customer_id
             order by 2 desc),
     Customer_Average_Check_Segment
         as (select customer_id,
                    percent_segment,
                    case
                        when percent_segment * 100.0 /
                             (select max(percent_segment) from Customer_Average_Check) <= 10 then 'High'
                        when percent_segment * 100.0 /
                             (select max(percent_segment) from Customer_Average_Check) > 10 and
                             percent_segment * 100.0 /
                             (select max(percent_segment) from Customer_Average_Check) <= 35 then 'Medium'
                        else 'Low'
                        end as Customer_Average_Check_Segment
             from Customer_Average_Check),
     Customer_Frequency
         as (select customer_id,
                    extract(epoch from max(transaction_datetime) - min(transaction_datetime))::double precision /
                    ((24 * 60 * 60) * count(transaction_datetime))                  as Customer_Frequency,
                    (row_number()
                     over (order by extract(days from max(transaction_datetime) - min(transaction_datetime)) /
                                    count(transaction_datetime)))::double precision as percent_segment
             from Common_Data
             group by customer_id
             order by 1),
     Customer_Frequency_Segment
         as (select customer_id,
                    percent_segment,
                    case
                        when percent_segment * 100.0 /
                             (select max(percent_segment) from Customer_Frequency) <= 10 then 'Often'
                        when percent_segment * 100.0 /
                             (select max(percent_segment) from Customer_Frequency) > 10 and
                             percent_segment * 100.0 /
                             (select max(percent_segment) from Customer_Frequency) <= 35 then 'Occasionally'
                        else 'Rarely'
                        end as Customer_Frequency_Segment
             from Customer_Frequency),
     Customer_Inactive_Period
         as (select customer_id,
                    extract(epoch from (select max(analysis_formation)
                                        from date_of_analysis_formation) -
                                       max(transaction_datetime))::double precision /
                    (24.0 * 60.0 * 60.0) as Customer_Inactive_Period
             from Common_Data
             where transaction_datetime <= (select max(analysis_formation)
                                            from date_of_analysis_formation)
             group by customer_id),
     Customer_Churn_Rate
         as (select Customer_Inactive_Period.customer_id,
                    Customer_Inactive_Period / Customer_Frequency as Customer_Churn_Rate
             from Customer_Inactive_Period
             inner join Customer_Frequency using (customer_id)),
     Customer_Churn_Segment
         as (select customer_id,
                    case
                        when Customer_Churn_Rate between 0 and 2 then 'Low'
                        when Customer_Churn_Rate between 2 and 5 then 'Medium'
                        else 'High'
                        end as Customer_Churn_Segment
             from Customer_Churn_Rate),
     Customer_Segment
         as (select customer_id,
                    churn_rate + frequency_rate + average_rate as Customer_Segment
             from (select Customer_Average_Check_Segment.customer_id,
                          case
                              when Customer_Churn_Segment = 'Low' then 1
                              when Customer_Churn_Segment = 'Medium' then 2
                              when Customer_Churn_Segment = 'High' then 3
                              end as churn_rate,
                          case
                              when Customer_Frequency_Segment = 'Rarely' then 0
                              when Customer_Frequency_Segment = 'Occasionally' then 3
                              when Customer_Frequency_Segment = 'Often' then 6
                              end as frequency_rate,
                          case
                              when Customer_Average_Check_Segment = 'Low' then 0
                              when Customer_Average_Check_Segment = 'Medium' then 9
                              when Customer_Average_Check_Segment = 'High' then 18
                              end as average_rate
                   from Customer_Average_Check_Segment
                   inner join Customer_Frequency_Segment using (customer_id)
                   inner join Customer_Churn_Segment using (customer_id)
                   order by customer_id) Segments),
     Recent_Stores
         as (select customer_id,
                    transaction_store_id,
                    count(transaction_store_id) as count_of_stores
             from Common_Data
             where transaction_datetime in
                   (select transaction_datetime
                    from Common_Data as AT
                    where AT.customer_id = Common_Data.customer_id
                    order by transaction_datetime desc
                    limit 3)
             group by customer_id, transaction_store_id
             order by 1, 2, 3),
     Permanent_Customers
         as (select customer_id,
                    transaction_store_id as Customer_Primary_Store
             from Recent_Stores
             where customer_id in
                   (select customer_id
                    from Recent_Stores
                    group by customer_id
                    having (count(customer_id)) = 1)),
     Unpermament_Customers
         as (select customer_id, transaction_store_id as Customer_Primary_Store
             from (select Common_Data.customer_id,
                          Common_Data.transaction_store_id,
                          max(Common_Data.transaction_datetime),
                          row_number()
                          over (partition by common_data.customer_id order by max(common_data.transaction_datetime) desc) as filter
                   from (select customer_id,
                                transaction_store_id,
                                count(transaction_store_id) as count_of_transactions
                         from common_data
                         group by customer_id, transaction_store_id
                         order by 1, 2) Preferred_Stores
                   inner join common_data using (customer_id, transaction_store_id)
                   group by common_data.customer_id, common_data.transaction_store_id
                   order by 1, 3 desc) source
             where filter = 1),
     Customer_Primary_Store
         as ((select *
              from Unpermament_Customers
              except
              select *
              from Permanent_Customers)
             union
             select *
             from Permanent_Customers)
select Customer_Average_Check.customer_id,
       Customer_Average_Check.Customer_Average_Check,
       Customer_Average_Check_Segment.Customer_Average_Check_Segment,
       Customer_Frequency.Customer_Frequency,
       Customer_Frequency_Segment.Customer_Frequency_Segment,
       Customer_Inactive_Period.Customer_Inactive_Period,
       Customer_Churn_Rate.Customer_Churn_Rate,
       Customer_Churn_Segment.Customer_Churn_Segment,
       Customer_Segment.Customer_Segment,
       Customer_Primary_Store.Customer_Primary_Store
from Customer_Average_Check
inner join Customer_Average_Check_Segment using (customer_id)
inner join Customer_Frequency using (customer_id)
inner join Customer_Frequency_Segment using (customer_id)
inner join Customer_Inactive_Period using (customer_id)
inner join Customer_Churn_Rate using (customer_id)
inner join Customer_Churn_Segment using (customer_id)
inner join Customer_Segment using (customer_id)
inner join Customer_Primary_Store using (customer_id)
order by 1);
