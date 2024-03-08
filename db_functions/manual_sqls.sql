CREATE table "public".time_tracking_info (
    "ID" integer constraint time_tracking_pkey primary key,
    is_tracked_1min boolean not null default false,
    is_tracked_1day boolean not null default false,
    stock integer not null,
    worker_name varchar(35) not null,
    constraint stock_id foreign key (stock) references "public".stocks("ID")
);

CREATE VIEW "public".tracked_indexes AS
    SELECT
    intermediate.name as "equity_name",
    intermediate.symbol,
    m.name as "market_name",
    intermediate.is_tracked_1day,
    intermediate.is_tracked_1min,
    intermediate.worker_name
    FROM (
        SELECT s.name, s.exchange, s.symbol, t_trk.is_tracked_1min, t_trk.is_tracked_1day ,t_trk.worker_name
        FROM "public".time_tracking_info t_trk
        LEFT JOIN "public".stocks s
        on t_trk.stock = s."ID"
    ) intermediate
    LEFT JOIN "public".markets m
    on intermediate.exchange = m."ID";

-- funny idea - check for closest day/minute if day has not been found...? Alongside with helpful time type-casts
select date_trunc('month', (select datetime from "forex_time_series"."USD_EUR_1min"));
select TIMESTAMP '2020-03-20' + INTERVAL '1 day';
select date_trunc('day', (select datetime from "forex_time_series"."USD_EUR_1min")) + INTERVAL '1 days';

--ID | symbol |                 name                  | currency | exchange | country | type | plan |
-- currencies: ID |     name     | symbol |
-- markets: ID |  name  | access | timezone | country | code |
-- countries: ID |  name   |
-- investment_types: ID |     name     |
-- plans: ID | global  | plan

SELECT main_tab."ID", main_tab.symbol, main_tab.name, curs.symbol as currency_symbol, mkts.code as exchange_mic_code,
    cntrs.name as country_name, i_types.name as investment_type, p.plan as access_plan
FROM public.stocks main_tab
LEFT JOIN public.currencies curs ON main_tab.currency = curs."ID"
LEFT JOIN public.markets mkts ON main_tab.exchange = mkts."ID"
LEFT JOIN public.countries cntrs ON main_tab.country = cntrs."ID"
LEFT JOIN public.investment_types i_types ON main_tab.type = i_types."ID"
LEFT JOIN public.plans p ON main_tab.plan = p."ID" LIMIT 5;
