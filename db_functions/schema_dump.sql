--
-- PostgreSQL database dump
--

-- Dumped from database version 15.3
-- Dumped by pg_dump version 15.3

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

CREATE USER db_user;

ALTER USER db_user WITH CREATEDB CREATEROLE LOGIN;

--
-- Name: 1day_time_series; Type: SCHEMA; Schema: -; Owner: db_user
--

CREATE SCHEMA "1day_time_series";


ALTER SCHEMA "1day_time_series" OWNER TO db_user;

--
-- Name: 1min_time_series; Type: SCHEMA; Schema: -; Owner: db_user
--

CREATE SCHEMA "1min_time_series";


ALTER SCHEMA "1min_time_series" OWNER TO db_user;

--
-- Name: forex_time_series; Type: SCHEMA; Schema: -; Owner: db_user
--

CREATE SCHEMA "forex_time_series";


ALTER SCHEMA "forex_time_series" OWNER TO db_user;

--
-- Name: adminpack; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS adminpack WITH SCHEMA pg_catalog;


--
-- Name: EXTENSION adminpack; Type: COMMENT; Schema: -; Owner:
--

COMMENT ON EXTENSION adminpack IS 'administrative functions for PostgreSQL';


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: countries; Type: TABLE; Schema: public; Owner: db_user
--

CREATE TABLE IF NOT EXISTS public.countries (
    "ID" integer NOT NULL,
    name character varying(60)
);


ALTER TABLE public.countries OWNER TO db_user;

--
-- Name: currencies; Type: TABLE; Schema: public; Owner: db_user
--

CREATE TABLE IF NOT EXISTS public.currencies (
    "ID" integer NOT NULL,
    name character varying(35),
    symbol character varying(10)
);


ALTER TABLE public.currencies OWNER TO db_user;

--
-- Name: forex_currency_groups; Type: TABLE; Schema: public; Owner: db_user
--

CREATE TABLE IF NOT EXISTS public.forex_currency_groups (
    "ID" integer NOT NULL,
    name character varying(25)
);


ALTER TABLE public.forex_currency_groups OWNER TO db_user;

--
-- Name: forex_pairs; Type: TABLE; Schema: public; Owner: db_user
--

CREATE TABLE IF NOT EXISTS public.forex_pairs (
    "ID" integer NOT NULL,
    currency_group integer,
    symbol character varying(10),
    currency_base integer,
    currency_quote integer
);


ALTER TABLE public.forex_pairs OWNER TO db_user;

--
-- Name: investment_types; Type: TABLE; Schema: public; Owner: db_user
--

CREATE TABLE IF NOT EXISTS public.investment_types (
    "ID" integer NOT NULL,
    name character varying(40)
);


ALTER TABLE public.investment_types OWNER TO db_user;

--
-- Name: markets; Type: TABLE; Schema: public; Owner: db_user
--

CREATE TABLE IF NOT EXISTS public.markets (
    "ID" integer NOT NULL,
    name character varying(50) NOT NULL,
    access integer,
    timezone integer,
    country integer,
    code character varying(20) NOT NULL
);


ALTER TABLE public.markets OWNER TO db_user;

--
-- Name: plans; Type: TABLE; Schema: public; Owner: db_user
--

CREATE TABLE IF NOT EXISTS public.plans (
    "ID" integer NOT NULL,
    global character varying(30),
    plan character varying(30)
);


ALTER TABLE public.plans OWNER TO db_user;

--
-- Name: stocks; Type: TABLE; Schema: public; Owner: db_user
--

CREATE TABLE IF NOT EXISTS public.stocks (
    "ID" integer NOT NULL,
    symbol character varying(20),
    name character varying(170),
    currency integer NOT NULL,
    exchange integer NOT NULL,
    country integer NOT NULL,
    type integer NOT NULL,
    plan integer NOT NULL
);


ALTER TABLE public.stocks OWNER TO db_user;

--
-- Name: time_tracking_info; Type: TABLE; Schema: public; Owner: db_user
--

CREATE TABLE IF NOT EXISTS public.time_tracking_info (
    "ID" integer NOT NULL,
    is_tracked_1min boolean DEFAULT false NOT NULL,
    is_tracked_1day boolean DEFAULT false NOT NULL,
    stock integer NOT NULL,
    worker_name character varying(35) NOT NULL
);


ALTER TABLE public.time_tracking_info OWNER TO db_user;

--
-- Name: timezones; Type: TABLE; Schema: public; Owner: db_user
--

CREATE TABLE IF NOT EXISTS public.timezones (
    "ID" integer NOT NULL,
    name character varying(100)
);


ALTER TABLE public.timezones OWNER TO db_user;

--
-- Name: tracked_indexes; Type: VIEW; Schema: public; Owner: db_user
--

CREATE VIEW public.tracked_indexes AS
 SELECT intermediate.name AS equity_name,
    intermediate.symbol,
    m.name AS market_name,
    intermediate.is_tracked_1day,
    intermediate.is_tracked_1min,
    intermediate.worker_name
   FROM (( SELECT s.name,
            s.exchange,
            s.symbol,
            t_trk.is_tracked_1min,
            t_trk.is_tracked_1day,
            t_trk.worker_name
           FROM (public.time_tracking_info t_trk
             LEFT JOIN public.stocks s ON ((t_trk.stock = s."ID")))) intermediate
     LEFT JOIN public.markets m ON ((intermediate.exchange = m."ID")));


ALTER TABLE public.tracked_indexes OWNER TO db_user;


--
-- Name: non_standard_functions; Type: VIEW; Schema: public; Owner: db_user
--

CREATE VIEW public.non_standard_functions AS
SELECT specific_name, routine_name, routine_schema, routine_type
FROM information_schema.routines
WHERE routine_type = 'FUNCTION'
  AND routine_schema NOT IN ('pg_catalog', 'information_schema');


ALTER TABLE public.non_standard_functions OWNER TO db_user;


--
-- Name: non_standard_views; Type: VIEW; Schema: public; Owner: db_user
--

CREATE VIEW public.non_standard_views AS
SELECT table_schema, table_name
FROM information_schema.views
WHERE table_schema NOT IN ('pg_catalog', 'information_schema');


ALTER TABLE public.non_standard_views OWNER TO db_user;


--
-- Name: forex_pairs_explained; Type: VIEW; Schema: public; Owner: db_user
--

CREATE VIEW public.forex_pairs_explained AS
SELECT main_tab."ID", f_grps.name as currency_group_name, main_tab.symbol,
    base_cur.symbol as base_currency_symbol, quote_cur.symbol as quote_currency_symbol,
    base_cur.name as base_currency_name, quote_cur.name as quote_currency_name
FROM public.forex_pairs main_tab
LEFT JOIN public.forex_currency_groups f_grps ON main_tab.currency_group = f_grps."ID"
LEFT JOIN public.currencies quote_cur ON main_tab.currency_quote = quote_cur."ID"
LEFT JOIN public.currencies base_cur ON main_tab.currency_base = base_cur."ID";


ALTER TABLE public.forex_pairs_explained OWNER TO db_user;


--
-- Name: forex_pairs_explained; Type: VIEW; Schema: public; Owner: db_user
--

CREATE VIEW public.markets_explained AS
SELECT main_tab."ID", main_tab.name, main_tab.code as mic_code, tz.name as timezone_name,
    p.plan as access_plan, cntrs.name as country_name
FROM public.markets main_tab
LEFT JOIN public.timezones tz ON main_tab.timezone = tz."ID"
LEFT JOIN public.countries cntrs ON main_tab.country = cntrs."ID"
LEFT JOIN public.plans p ON main_tab.access = p."ID";


ALTER TABLE public.markets_explained OWNER TO db_user;


--
-- Name: stocks_explained; Type: VIEW; Schema: public; Owner: db_user
--

CREATE VIEW public.stocks_explained AS
SELECT main_tab."ID", main_tab.symbol, main_tab.name, curs.symbol as currency_symbol, mkts.code as exchange_mic_code,
    cntrs.name as country_name, i_types.name as investment_type, p.plan as access_plan
FROM public.stocks main_tab
LEFT JOIN public.currencies curs ON main_tab.currency = curs."ID"
LEFT JOIN public.markets mkts ON main_tab.exchange = mkts."ID"
LEFT JOIN public.countries cntrs ON main_tab.country = cntrs."ID"
LEFT JOIN public.investment_types i_types ON main_tab.type = i_types."ID"
LEFT JOIN public.plans p ON main_tab.plan = p."ID";


ALTER TABLE public.stocks_explained OWNER TO db_user;

--
-- Name: generate_financial_view_1day(text); Type: FUNCTION; Schema: public; Owner: db_user
--

CREATE FUNCTION public.generate_financial_view_1day(tbl_name text) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE
    view_name TEXT;
BEGIN
    view_name := tbl_name || '_view';

    EXECUTE format('
        CREATE OR REPLACE VIEW "1day_time_series".%I AS
        SELECT
            *,
            (fin_tab.open > fin_tab.close) AS bearish,
            (fin_tab.open < fin_tab.close) AS bullish
        FROM "1day_time_series".%I fin_tab', view_name, tbl_name);
END;
$$;


ALTER FUNCTION public.generate_financial_view_1day(tbl_name text) OWNER TO db_user;


--
-- Name: generate_financial_view_1min(text); Type: FUNCTION; Schema: public; Owner: db_user
--

CREATE FUNCTION public.generate_financial_view_1min(tbl_name text) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE
    view_name TEXT;
BEGIN
    view_name := tbl_name || '_view';

    EXECUTE format('
        CREATE OR REPLACE VIEW "1min_time_series".%I AS
        SELECT
            *,
            (fin_tab.open > fin_tab.close) AS bearish,
            (fin_tab.open < fin_tab.close) AS bullish
        FROM "1min_time_series".%I fin_tab', view_name, tbl_name);
END;
$$;


ALTER FUNCTION public.generate_financial_view_1min(tbl_name text) OWNER TO db_user;


--
-- Name: generate_forex_view(text); Type: FUNCTION; Schema: public; Owner: db_user
--

CREATE FUNCTION public.generate_forex_view(tbl_name text) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE
    view_name TEXT;
BEGIN
    view_name := tbl_name || '_view';

    EXECUTE format('
        CREATE OR REPLACE VIEW "forex_time_series".%I AS
        SELECT
            *,
            (fin_tab.open > fin_tab.close) AS bearish,
            (fin_tab.open < fin_tab.close) AS bullish
        FROM "forex_time_series".%I fin_tab', view_name, tbl_name);
END;
$$;


ALTER FUNCTION public.generate_forex_view(tbl_name text) OWNER TO db_user;

--
-- Name: check_is_stock(text); Type: FUNCTION; Schema: public; Owner: db_user
--

CREATE FUNCTION public.check_is_stock(symbol_to_check text) RETURNS boolean
    LANGUAGE plpgsql
    AS $$
BEGIN
    RETURN EXISTS (
        SELECT 1 FROM public.stocks
        WHERE symbol = symbol_to_check
    );
END;
$$;


ALTER FUNCTION public.check_is_stock(symbol_to_check text) OWNER TO db_user;


--
-- Name: check_is_forex_pair(text); Type: FUNCTION; Schema: public; Owner: db_user
--

CREATE FUNCTION public.check_is_forex_pair(symbol_to_check text) RETURNS boolean
    LANGUAGE plpgsql
    AS $$
BEGIN
    RETURN EXISTS (
        SELECT 1 FROM public.forex_pairs
        WHERE symbol = symbol_to_check
    );
END;
$$;


ALTER FUNCTION public.check_is_forex_pair(symbol_to_check text) OWNER TO db_user;


--
-- Name: markets ID; Type: CONSTRAINT; Schema: public; Owner: db_user
--

ALTER TABLE ONLY public.markets
    ADD CONSTRAINT "ID" PRIMARY KEY ("ID");


--
-- Name: plans Plans_pkey; Type: CONSTRAINT; Schema: public; Owner: db_user
--

ALTER TABLE ONLY public.plans
    ADD CONSTRAINT "Plans_pkey" PRIMARY KEY ("ID");


--
-- Name: countries country_pkey; Type: CONSTRAINT; Schema: public; Owner: db_user
--

ALTER TABLE ONLY public.countries
    ADD CONSTRAINT country_pkey PRIMARY KEY ("ID");


--
-- Name: currencies currencies_pkey; Type: CONSTRAINT; Schema: public; Owner: db_user
--

ALTER TABLE ONLY public.currencies
    ADD CONSTRAINT currencies_pkey PRIMARY KEY ("ID");


--
-- Name: forex_currency_groups currency_group_pkey; Type: CONSTRAINT; Schema: public; Owner: db_user
--

ALTER TABLE ONLY public.forex_currency_groups
    ADD CONSTRAINT currency_group_pkey PRIMARY KEY ("ID");


--
-- Name: forex_pairs forex_pairs_pkey; Type: CONSTRAINT; Schema: public; Owner: db_user
--

ALTER TABLE ONLY public.forex_pairs
    ADD CONSTRAINT forex_pairs_pkey PRIMARY KEY ("ID");


--
-- Name: investment_types investment_types_pkey; Type: CONSTRAINT; Schema: public; Owner: db_user
--

ALTER TABLE ONLY public.investment_types
    ADD CONSTRAINT investment_types_pkey PRIMARY KEY ("ID");


--
-- Name: stocks stocks_pkey; Type: CONSTRAINT; Schema: public; Owner: db_user
--

ALTER TABLE ONLY public.stocks
    ADD CONSTRAINT stocks_pkey PRIMARY KEY ("ID");


--
-- Name: time_tracking_info time_tracking_pkey; Type: CONSTRAINT; Schema: public; Owner: db_user
--

ALTER TABLE ONLY public.time_tracking_info
    ADD CONSTRAINT time_tracking_pkey PRIMARY KEY ("ID");


--
-- Name: timezones timezones_pkey; Type: CONSTRAINT; Schema: public; Owner: db_user
--

ALTER TABLE ONLY public.timezones
    ADD CONSTRAINT timezones_pkey PRIMARY KEY ("ID");


--
-- Name: markets access; Type: FK CONSTRAINT; Schema: public; Owner: db_user
--

ALTER TABLE ONLY public.markets
    ADD CONSTRAINT access FOREIGN KEY (access) REFERENCES public.plans("ID");


--
-- Name: markets country; Type: FK CONSTRAINT; Schema: public; Owner: db_user
--

ALTER TABLE ONLY public.markets
    ADD CONSTRAINT country FOREIGN KEY (country) REFERENCES public.countries("ID");


--
-- Name: stocks country; Type: FK CONSTRAINT; Schema: public; Owner: db_user
--

ALTER TABLE ONLY public.stocks
    ADD CONSTRAINT country FOREIGN KEY (country) REFERENCES public.countries("ID");


--
-- Name: stocks currency; Type: FK CONSTRAINT; Schema: public; Owner: db_user
--

ALTER TABLE ONLY public.stocks
    ADD CONSTRAINT currency FOREIGN KEY (currency) REFERENCES public.currencies("ID");


--
-- Name: forex_pairs currency_base; Type: FK CONSTRAINT; Schema: public; Owner: db_user
--

ALTER TABLE ONLY public.forex_pairs
    ADD CONSTRAINT currency_base FOREIGN KEY (currency_base) REFERENCES public.currencies("ID");


--
-- Name: forex_pairs currency_group; Type: FK CONSTRAINT; Schema: public; Owner: db_user
--

ALTER TABLE ONLY public.forex_pairs
    ADD CONSTRAINT currency_group FOREIGN KEY (currency_group) REFERENCES public.forex_currency_groups("ID");


--
-- Name: forex_pairs currency_quote; Type: FK CONSTRAINT; Schema: public; Owner: db_user
--

ALTER TABLE ONLY public.forex_pairs
    ADD CONSTRAINT currency_quote FOREIGN KEY (currency_quote) REFERENCES public.currencies("ID");


--
-- Name: stocks exchange; Type: FK CONSTRAINT; Schema: public; Owner: db_user
--

ALTER TABLE ONLY public.stocks
    ADD CONSTRAINT exchange FOREIGN KEY (exchange) REFERENCES public.markets("ID");


--
-- Name: stocks investment_type; Type: FK CONSTRAINT; Schema: public; Owner: db_user
--

ALTER TABLE ONLY public.stocks
    ADD CONSTRAINT investment_type FOREIGN KEY (type) REFERENCES public.investment_types("ID");


--
-- Name: stocks plan; Type: FK CONSTRAINT; Schema: public; Owner: db_user
--

ALTER TABLE ONLY public.stocks
    ADD CONSTRAINT plan FOREIGN KEY (plan) REFERENCES public.plans("ID");


--
-- Name: time_tracking_info stock_id; Type: FK CONSTRAINT; Schema: public; Owner: db_user
--

ALTER TABLE ONLY public.time_tracking_info
    ADD CONSTRAINT stock_id FOREIGN KEY (stock) REFERENCES public.stocks("ID");


--
-- Name: markets timezone; Type: FK CONSTRAINT; Schema: public; Owner: db_user
--

ALTER TABLE ONLY public.markets
    ADD CONSTRAINT timezone FOREIGN KEY (timezone) REFERENCES public.timezones("ID");


--
-- PostgreSQL database dump complete
--

