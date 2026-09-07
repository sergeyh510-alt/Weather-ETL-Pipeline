-- Table: weather_data.historical_weather

-- DROP TABLE IF EXISTS weather_data.historical_weather;

CREATE TABLE IF NOT EXISTS weather_data.historical_weather
(
    id bigint NOT NULL DEFAULT nextval('weather_data.historical_weather_id_seq'::regclass),
    timestamp_utc timestamp with time zone NOT NULL,
                                timezone_offset character varying(10) COLLATE pg_catalog."default" DEFAULT '+3'::character varying,
    temperature_2m double precision,
    relative_humidity_2m double precision,
    dew_point_2m double precision,
    apparent_temperature double precision,
    precipitation double precision,
    rain double precision,
    snowfall double precision,
    snow_depth double precision,
    weather_code integer,
    pressure_msl double precision,
    surface_pressure double precision,
    cloud_cover double precision,
    cloud_cover_low double precision,
    cloud_cover_mid double precision,
    cloud_cover_high double precision,
    et0_fao_evapotranspiration double precision,
    vapour_pressure_deficit double precision,
    wind_speed_10m double precision,
    wind_speed_100m double precision,
    wind_direction_10m double precision,
    wind_direction_100m double precision,
    wind_gusts_10m double precision,
    soil_temperature_0_to_7cm double precision,
    soil_temperature_7_to_28cm double precision,
    soil_temperature_28_to_100cm double precision,
    soil_temperature_100_to_255cm double precision,
    soil_moisture_0_to_7cm double precision,
    soil_moisture_7_to_28cm double precision,
    soil_moisture_28_to_100cm double precision,
    soil_moisture_100_to_255cm double precision,
    latitude double precision,
    longitude double precision,
    location_name character varying(100) COLLATE pg_catalog."default",
    data_source character varying(100) COLLATE pg_catalog."default",
    load_start_date date,
    load_end_date date,
    hour_utc integer,
    day_of_week_utc integer,
    month_utc integer,
    year_utc integer,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
                                CONSTRAINT historical_weather_pkey PRIMARY KEY (id)
    )

    TABLESPACE pg_default;

ALTER TABLE IF EXISTS weather_data.historical_weather
    OWNER to postgres;

COMMENT ON TABLE weather_data.historical_weather
    IS 'Таблица с историческими и прогнозными погодными данными';

COMMENT ON COLUMN weather_data.historical_weather.timestamp_utc
    IS 'Время в UTC (без смещения)';

COMMENT ON COLUMN weather_data.historical_weather.temperature_2m
    IS 'Температура на высоте 2м в °C';

COMMENT ON COLUMN weather_data.historical_weather.relative_humidity_2m
    IS 'Относительная влажность на высоте 2м в %';
-- Index: idx_weather_location

-- DROP INDEX IF EXISTS weather_data.idx_weather_location;

CREATE INDEX IF NOT EXISTS idx_weather_location
    ON weather_data.historical_weather USING btree
    (latitude ASC NULLS LAST, longitude ASC NULLS LAST)
    WITH (fillfactor=100, deduplicate_items=True)
    TABLESPACE pg_default;
-- Index: idx_weather_temp

-- DROP INDEX IF EXISTS weather_data.idx_weather_temp;

CREATE INDEX IF NOT EXISTS idx_weather_temp
    ON weather_data.historical_weather USING btree
    (temperature_2m ASC NULLS LAST)
    WITH (fillfactor=100, deduplicate_items=True)
    TABLESPACE pg_default;
-- Index: idx_weather_timestamp_utc

-- DROP INDEX IF EXISTS weather_data.idx_weather_timestamp_utc;

CREATE INDEX IF NOT EXISTS idx_weather_timestamp_utc
    ON weather_data.historical_weather USING btree
    (timestamp_utc ASC NULLS LAST)
    WITH (fillfactor=100, deduplicate_items=True)
    TABLESPACE pg_default;
-- Index: idx_weather_unique

-- DROP INDEX IF EXISTS weather_data.idx_weather_unique;

CREATE UNIQUE INDEX IF NOT EXISTS idx_weather_unique
    ON weather_data.historical_weather USING btree
    (timestamp_utc ASC NULLS LAST, latitude ASC NULLS LAST, longitude ASC NULLS LAST)
    WITH (fillfactor=100, deduplicate_items=True)
    TABLESPACE pg_default;