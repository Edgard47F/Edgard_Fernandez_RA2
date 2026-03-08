-- DDL for RA2 project: dimensions and fact table (NeonDB/Postgres)
create schema if not exists polymarket;

-- Dimension: Event
create table if not exists polymarket.dim_event (
  event_id bigint primary key,
  title text,
  description text,
  category text,
  start_ts timestamptz,
  end_ts timestamptz
);

-- Dimension: Market
create table if not exists polymarket.dim_market (
  market_id bigint primary key,
  question text,
  category text,
  active boolean,
  closed boolean,
  archived boolean,
  liquidity double precision,
  volume double precision,
  event_id bigint,
  end_ts timestamptz
);

-- Dimension: Series (simple historic/probability info)
create table if not exists polymarket.dim_series (
  series_id bigint primary key,
  market_id bigint,
  recorded_ts timestamptz,
  probability double precision,
  price double precision
);

-- Dimension: Tag
create table if not exists polymarket.dim_tag (
  tag_id bigint primary key,
  name text,
  event_id bigint
);

-- Time dimension (optional)
create table if not exists polymarket.dim_time (
  ts timestamptz primary key,
  year int,
  month int,
  day int,
  hour int
);

-- Clean time dimension (derived from event/market/fact timestamps)
create table if not exists polymarket.dim_time_clean (
  ts timestamptz primary key,
  year int,
  month int,
  day int,
  hour int
);

-- Fact table (no FK constraints to allow flexible incremental loads)
create table if not exists polymarket.fact_market_event (
  fact_id bigserial primary key,
  market_id bigint,
  event_id bigint,
  series_id bigint,
  tag_id bigint,
  liquidity double precision,
  volume double precision,
  active boolean,
  closed boolean,
  recorded_ts timestamptz
);

-- Indexes to improve analytical queries
create index if not exists idx_dim_market_category on polymarket.dim_market(category);
create index if not exists idx_fact_recorded_ts on polymarket.fact_market_event(recorded_ts);

-- CLEAN (normalized) dimensions with stable PKs
create table if not exists polymarket.dim_event_clean (
  event_id bigint primary key,
  title text,
  category text,
  end_ts timestamptz
);

create table if not exists polymarket.dim_market_clean (
  market_id bigint primary key,
  question text,
  category text,
  active boolean,
  closed boolean,
  archived boolean,
  liquidity double precision,
  volume double precision,
  event_id bigint,
  end_ts timestamptz
);

create table if not exists polymarket.dim_series_clean (
  series_id bigint primary key,
  market_id bigint,
  title text,
  recorded_ts timestamptz,
  probability double precision
);

create table if not exists polymarket.dim_tag_clean (
  tag_id bigint primary key,
  name text,
  event_id bigint
);

create table if not exists polymarket.fact_market_event_clean (
  fact_id bigserial primary key,
  market_id bigint references polymarket.dim_market_clean(market_id),
  event_id bigint references polymarket.dim_event_clean(event_id),
  series_id bigint references polymarket.dim_series_clean(series_id),
  tag_id bigint references polymarket.dim_tag_clean(tag_id),
  liquidity double precision,
  volume double precision,
  active boolean,
  closed boolean,
  recorded_ts timestamptz
);

create index if not exists idx_fact_clean_recorded_ts on polymarket.fact_market_event_clean(recorded_ts);

-- Bridge table linking events and tags (many-to-many, normalized)
create table if not exists polymarket.event_tag_bridge (
  event_id bigint not null references polymarket.dim_event_clean(event_id),
  tag_id bigint not null references polymarket.dim_tag_clean(tag_id),
  primary key (event_id, tag_id)
);
create index if not exists idx_event_tag_event_id on polymarket.event_tag_bridge(event_id);
create index if not exists idx_event_tag_tag_id on polymarket.event_tag_bridge(tag_id);
