create role administrator;
grant all privileges on all tables in schema public to administrator;

create role visitor;
grant select on all tables in schema public to visitor;