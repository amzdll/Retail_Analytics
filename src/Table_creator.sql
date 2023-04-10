create table Personal_data
(
    Customer_ID            bigint primary key unique,
    Customer_Name          varchar(255) not null,
    Customer_Surname       varchar(255) not null,
    Customer_Primary_Email varchar(255) not null,
    Customer_Primary_Phone varchar(255) not null,

    constraint ch_Customer_Name check (Customer_Name ~ INITCAP(Customer_Name)),
    constraint ch_Customer_Surname check (Customer_Surname ~ INITCAP(Customer_Surname)),
    constraint ch_Customer_Primary_Email check (Customer_Primary_Email ~*
                                                '^[A-Za-z0-9._+%-]+@[A-Za-z0-9.-]+[.][A-Za-z]+$'),
    constraint ch_Customer_Primary_Phone check (Customer_Primary_Phone ~* '^\+7\d{10}$')
);

create table Cards
(
    Customer_Card_ID bigint primary key unique,
    Customer_ID      bigint not null,

    constraint fk_Customer_ID_Personal_data foreign key (Customer_ID) references Personal_data (Customer_ID)
);


create table groups_sku
(
    Group_ID   bigint primary key,
    Group_Name varchar(255) not null
);

create table SKU
(
    SKU_ID   bigint primary key,
    SKU_Name varchar(255) not null,
    Group_ID bigint,

    constraint fk_Group_ID_groups_sku foreign key (Group_ID) references groups_sku (Group_ID)
);

create table Stores
(
    Transaction_Store_ID bigint,
    SKU_ID               bigint,
    SKU_Purchase_Price   decimal not null,
    SKU_Retail_Price     decimal not null,

    constraint fk_SKU_ID_groups_sku foreign key (SKU_ID) references SKU (SKU_ID)
);

create table Transactions
(
    Transaction_ID       bigint primary key unique,
    Customer_Card_ID     bigint    not null,
    Transaction_Summ     decimal   not null,
    Transaction_DateTime timestamp not null,
    Transaction_Store_ID bigint    not null,

    constraint fk_Customer_Card_ID_Personal_Information foreign key (Customer_Card_ID) references Cards (Customer_Card_ID)
);

create table Checks
(
    Transaction_ID bigint  not null,
    SKU_ID         bigint  not null,
    SKU_Amount     decimal not null,
    SKU_Summ       decimal not null,
    SKU_Summ_Paid  decimal not null,
    SKU_Discount   decimal not null,

    constraint fk_SKU_ID_SKU foreign key (SKU_ID) references sku (SKU_ID)
);

create table Date_of_analysis_formation
(
    Analysis_Formation timestamp
);

-- Import data from tsv

create or replace procedure import_tsv(in table_name text, in filepath text, in separator char) as
$$
begin
    execute format('COPY %s FROM %L DELIMITER E''%s''', $1, $2, $3);
end;
$$ language plpgsql;

set datestyle = "ISO, DMY";
set import_path.txt to 'some_path/SQL3_RetailAnalitycs_v1.0-0/datasets/';

call import_tsv('personal_data', (current_setting('import_path.txt') || 'Personal_Data_Mini.tsv'), '\t');
call import_tsv('cards', (current_setting('import_path.txt') || 'Cards_Mini.tsv'), '\t');
call import_tsv('groups_sku', (current_setting('import_path.txt') || 'Groups_SKU_Mini.tsv'), '\t');
call import_tsv('sku', (current_setting('import_path.txt') || 'SKU_Mini.tsv'), '\t');
call import_tsv('stores', (current_setting('import_path.txt') || 'Stores_Mini.tsv'), '\t');
call import_tsv('checks', (current_setting('import_path.txt') || 'Checks_Mini.tsv'), '\t');
call import_tsv('transactions', (current_setting('import_path.txt') || 'Transactions_Mini.tsv'), '\t');
call import_tsv('date_of_analysis_formation', (current_setting('import_path.txt') || 'Date_Of_Analysis_Formation.tsv'), '\t');

-- Export data to tsv

CREATE OR REPLACE PROCEDURE export(IN tablename text, IN path text, IN Separator CHAR) AS
$$
BEGIN
    EXECUTE format('COPY %s TO %L DELIMITER E''%s'';', $1, $2, $3);
END;
$$ LANGUAGE plpgsql;

set export_path.txt to 'some_path/src/for_export/';

tsv
call export('personal_data',(current_setting('export_path.txt') || 'Personal_Data.tsv'), '\t');
call export('cards', (current_setting('export_path.txt') || 'Cards.tsv'), '\t');
call export('groups_sku', (current_setting('export_path.txt') || 'Groups_SKU.tsv'), '\t');
call export('sku', (current_setting('export_path.txt') || 'SKU.tsv'), '\t');
call export('stores', (current_setting('export_path.txt') || 'Stores.tsv'), '\t');
call export('checks', (current_setting('export_path.txt') || 'Checks.tsv'), '\t');
call export('transactions', (current_setting('export_path.txt') || 'Transactions.tsv'), '\t');
call export('date_of_analysis_formation',(current_setting('export_path.txt') || 'Date_Of_Analysis_Formation.tsv'), '\t');

--csv
call export('personal_data',(current_setting('export_path.txt') || 'Personal_Data.csv'), '\t');
call export('cards', (current_setting('export_path.txt') || 'Cards.csv'), '\t');
call export('groups_sku', (current_setting('export_path.txt') || 'Groups_SKU.csv'), '\t');
call export('sku', (current_setting('export_path.txt') || 'SKU.csv'), '\t');
call export('stores', (current_setting('export_path.txt') || 'Stores.csv'), '\t');
call export('checks', (current_setting('export_path.txt') || 'Checks.csv'), '\t');
call export('transactions', (current_setting('export_path.txt') || 'Transactions.csv'), '\t');
call export('date_of_analysis_formation',(current_setting('export_path.txt') || 'Date_Of_Analysis_Formation.csv'), '\t');

create or replace view Common_Data as
(
select customer_id,
       transaction_id,
       transaction_datetime,
       transaction_store_id,
       transaction_summ,
       group_id,
       group_name,
       sku_summ_paid,
       sku_summ,
       sku_discount,
       sku_id,
       sku_retail_price,
       sku_purchase_price,
       sku_amount,
       analysis_formation
from personal_data
         inner join cards using (customer_id)
         inner join transactions using (customer_card_id)
         inner join checks using (transaction_id)
         inner join sku using (sku_id)
         inner join groups_sku using (group_id)
         inner join stores using (sku_id, transaction_store_id)
         inner join date_of_analysis_formation af
                    on af.analysis_formation = af.analysis_formation
order by 1 );