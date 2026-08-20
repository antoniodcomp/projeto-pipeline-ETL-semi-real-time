CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.sensor_uci (
    house_id VARCHAR(50),
    current_timestamp TIMESTAMP,
    original_datetime TIMESTAMP,
    global_active_power NUMERIC,
    global_reactive_power NUMERIC,
    voltage NUMERIC,
    Global_intensity NUMERIC,
    Sub_metering_1 NUMERIC,
    Sub_metering_2 NUMERIC,
    Sub_metering_3 NUMERIC
)