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
-- Name: AAPL_XNGS; Type: TABLE; Schema: 1min_time_series; Owner: db_user
--

CREATE TABLE IF NOT EXISTS "1min_time_series"."AAPL_XNGS" (
    "ID" integer NOT NULL,
    datetime timestamp without time zone,
    open numeric(10,5),
    close numeric(10,5),
    high numeric(10,5),
    low numeric(10,5),
    volume integer
);


ALTER TABLE "1min_time_series"."AAPL_XNGS" OWNER TO db_user;

--
-- Name: NVDA_XNGS; Type: TABLE; Schema: 1min_time_series; Owner: db_user
--

CREATE TABLE IF NOT EXISTS "1min_time_series"."NVDA_XNGS" (
    "ID" integer NOT NULL,
    datetime timestamp without time zone,
    open numeric(10,5),
    close numeric(10,5),
    high numeric(10,5),
    low numeric(10,5),
    volume integer
);


ALTER TABLE "1min_time_series"."NVDA_XNGS" OWNER TO db_user;

--
-- Name: OTEX_XNGS; Type: TABLE; Schema: 1min_time_series; Owner: db_user
--

CREATE TABLE IF NOT EXISTS "1min_time_series"."OTEX_XNGS" (
    "ID" integer NOT NULL,
    datetime timestamp without time zone,
    open numeric(10,5),
    close numeric(10,5),
    high numeric(10,5),
    low numeric(10,5),
    volume integer
);


ALTER TABLE "1min_time_series"."OTEX_XNGS" OWNER TO db_user;


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
-- Name: generate_financial_view(text); Type: FUNCTION; Schema: public; Owner: db_user
--

CREATE FUNCTION public.generate_financial_view_1day(tbl_name text) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE
    view_name TEXT;
BEGIN
    view_name := tbl_name || '_view';

    EXECUTE format('
        CREATE OR REPLACE VIEW "public".%I AS
        SELECT
            *,
            (fin_tab.open - fin_tab.close > 0) AS bearish,
            (fin_tab.open - fin_tab.close < 0) AS bullish,
            (ABS(fin_tab.high - fin_tab.low) > 5) AS strong_trend
        FROM "1day_time_series".%I fin_tab', view_name, tbl_name);
END;
$$;


ALTER FUNCTION public.generate_financial_view_1day(tbl_name text) OWNER TO db_user;


--
-- Name: generate_financial_view(text); Type: FUNCTION; Schema: public; Owner: db_user
--

CREATE FUNCTION public.generate_financial_view_1min(tbl_name text) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE
    view_name TEXT;
BEGIN
    view_name := tbl_name || '_view';

    EXECUTE format('
        CREATE OR REPLACE VIEW "public".%I AS
        SELECT
            *,
            (fin_tab.open - fin_tab.close > 0) AS bearish,
            (fin_tab.open - fin_tab.close < 0) AS bullish,
            (ABS(fin_tab.high - fin_tab.low) > 5) AS strong_trend
        FROM "1min_time_series".%I fin_tab', view_name, tbl_name);
END;
$$;


ALTER FUNCTION public.generate_financial_view_1min(tbl_name text) OWNER TO db_user;


--
-- Name: AAPL_XNGS aapl_time_series_key; Type: CONSTRAINT; Schema: 1min_time_series; Owner: db_user
--

ALTER TABLE ONLY "1min_time_series"."AAPL_XNGS"
    ADD CONSTRAINT aapl_time_series_key PRIMARY KEY ("ID");


--
-- Name: NVDA_XNGS nvda_time_series_pkey; Type: CONSTRAINT; Schema: 1min_time_series; Owner: db_user
--

ALTER TABLE ONLY "1min_time_series"."NVDA_XNGS"
    ADD CONSTRAINT nvda_time_series_pkey PRIMARY KEY ("ID");


--
-- Name: OTEX_XNGS otex_time_series_pkey; Type: CONSTRAINT; Schema: 1min_time_series; Owner: db_user
--

ALTER TABLE ONLY "1min_time_series"."OTEX_XNGS"
    ADD CONSTRAINT otex_time_series_pkey PRIMARY KEY ("ID");



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

