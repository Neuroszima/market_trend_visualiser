DROP FUNCTION IF EXISTS "public".generate_financial_view_1day;
DROP FUNCTION IF EXISTS "public".generate_financial_view_1min;
DROP FUNCTION IF EXISTS "public".generate_forex_view;
DROP FUNCTION IF EXISTS "public".check_is_stock;

DROP VIEW IF EXISTS "public".tracked_indexes;
DROP VIEW IF EXISTS "public".non_standard_functions;
DROP VIEW IF EXISTS "public".non_standard_views;
DROP VIEW IF EXISTS "public".stocks_explained;
DROP VIEW IF EXISTS "public".forex_pairs_explained;
DROP VIEW IF EXISTS "public".markets_explained;

DROP TABLE IF EXISTS "public".currencies CASCADE;
DROP TABLE IF EXISTS "public".markets CASCADE;
DROP TABLE IF EXISTS "public".investment_types CASCADE;
DROP TABLE IF EXISTS "public".forex_currency_groups CASCADE;
DROP TABLE IF EXISTS "public".timezones CASCADE;
DROP TABLE IF EXISTS "public".stocks CASCADE;
DROP TABLE IF EXISTS "public".time_tracking_info CASCADE;
DROP TABLE IF EXISTS "public".forex_pairs CASCADE;
DROP TABLE IF EXISTS "public".plans CASCADE;
DROP TABLE IF EXISTS "public".countries CASCADE;

-- not wiping "public" schema or any database that gets set up can have benefits of having unique
-- functions/views/triggers, that will not get wiped when cleaning DB from contents this project has prepared
-- HOWEVER - cascade-wiping other schemas will remove all the views
-- made by functions associated with time_series

DROP SCHEMA IF EXISTS "1day_time_series" CASCADE;
DROP SCHEMA IF EXISTS "1min_time_series" CASCADE;
DROP SCHEMA IF EXISTS "forex_time_series" CASCADE;

DROP USER IF EXISTS db_user;