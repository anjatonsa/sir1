CREATE TABLE IF NOT EXISTS transit.zone_events
(
    event_time      DateTime64(3),      
    processing_time  DateTime64(3) DEFAULT now64(3),  
    vehicle_id      String,
    route_id        String,
    zone_id         String,
    zone_name       String,
    event_type      Enum8('ENTER' = 1, 'EXIT' = 2, 'DWELL' = 3, 'ALERT' = 4),
    latitude        Float64,
    longitude       Float64,
    dwell_seconds   Nullable(UInt32)   -- za EXIT (ukupno trajanje), ZA DWELL i ALERT (trajanje do sada) 
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (zone_id, vehicle_id, event_time)
TTL toDateTime(event_time) + INTERVAL 7 DAY;


CREATE TABLE IF NOT EXISTS transit.zone_density
(
    window_end      DateTime64(3),  
    zone_id         String,
    zone_name       String,
    vehicle_count   UInt32,
    route_ids       Array(String)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(window_end)
ORDER BY (zone_id, window_end)
TTL toDateTime(window_end) + INTERVAL 7 DAY;


CREATE TABLE IF NOT EXISTS transit.zone_transitions
(
    transition_time     DateTime64(3),
    vehicle_id          String,
    route_id            String,
    from_zone_id        String,
    from_zone_name      String,
    to_zone_id          String,
    to_zone_name        String,
    travel_time_seconds UInt32         
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(transition_time)
ORDER BY (from_zone_id, to_zone_id, transition_time)
TTL toDateTime(transition_time) + INTERVAL 7 DAY;