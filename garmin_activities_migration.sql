-- garmin_activities_migration.sql
-- Run once in Supabase SQL editor to create the garmin_activities table.

CREATE TABLE IF NOT EXISTS public.garmin_activities (
    activity_id          bigint PRIMARY KEY,
    activity_name        text,
    activity_type        text,
    sport_type           text,
    start_time_local     timestamptz,
    start_time_gmt       timestamptz,

    -- Distance & duration
    distance             numeric,          -- metres
    distance_km          numeric,
    duration             numeric,          -- seconds
    elapsed_duration     numeric,          -- seconds
    moving_duration      numeric,          -- seconds

    -- Speed
    average_speed        numeric,          -- m/s
    average_speed_kmh    numeric,
    max_speed            numeric,          -- m/s
    max_speed_kmh        numeric,

    -- Heart rate
    average_hr           numeric,
    max_hr               numeric,

    -- Power (cycling)
    average_power        numeric,          -- watts
    max_power            numeric,
    norm_power           numeric,

    -- Elevation
    elevation_gain       numeric,
    elevation_loss       numeric,
    min_elevation        numeric,
    max_elevation        numeric,

    -- Calories & effort
    calories             numeric,
    average_stress_score numeric,

    -- Running / cadence
    avg_run_cadence      numeric,          -- spm
    max_run_cadence      numeric,
    steps                integer,

    -- Cycling cadence
    avg_bike_cadence     numeric,
    max_bike_cadence     numeric,

    -- Swimming
    avg_stroke_rate      numeric,
    pool_length          numeric,          -- metres

    -- Training effect
    aerobic_te           numeric,
    anaerobic_te         numeric,

    -- Location
    start_latitude       numeric,
    start_longitude      numeric,

    -- Device / metadata
    device_id            bigint,
    device_name          text,
    owner_full_name      text,

    -- Timestamps
    synced_at            timestamptz DEFAULT now() NOT NULL
);

CREATE INDEX IF NOT EXISTS garmin_activities_start_time_idx
    ON public.garmin_activities (start_time_local DESC);

-- Allow authenticated reads (adjust RLS to your needs)
ALTER TABLE public.garmin_activities ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Allow authenticated reads"
    ON public.garmin_activities
    FOR SELECT
    TO authenticated
    USING (true);
