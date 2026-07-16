# Sistem za analizu tokova prostorno-vremenskih podataka

Sistem za real-time analizu kretanja autobusa u New York-u (NYC), korišćenjem Apache Kafka, Apache Flink, ClickHouse i Apache Superset.

## Podaci

Podaci o kretanju autobusa preuzimaju se iz javno dostupnog **MTA Bus Time GTFS-Realtime** feed-a (`https://gtfsrt.prod.obanyc.com/vehiclePositions`). 

## Arhitektura

Sistem se sastoji od 7 Docker servisa:

| Servis | Uloga |
|---|---|
| `producer` | Kontejnerizovan Python servis — čita MTA GTFS-RT feed svakih 15s, parsira ga i šalje u Kafka topic `vehicle-positions`. |
| `kafka` | Ulazni sloj. |
| `flink-jobmanager` / `flink-taskmanager` | PyFlink klaster (sa Kafka konektorom) — centralni sloj za obradu i analizu. |
| `clickhouse` | Skladište rezultata analize, optimizovano za analitičke upite. |
| `superset` | Vizuelizacija rezultata, povezan direktno na ClickHouse. |
| `kafka-ui` | Pomoćni alat za vizuelnu proveru Kafka topika. |


## Flink job-ovi

Sistem ima **dva nezavisna Flink job-a**, koji čitaju iz istog Kafka topic-a, ali demonstriraju dva različita pristupa stream processing-u.

### Job 1 — `geofencing_job.py`

Koristi **stateful `KeyedProcessFunction`** (tok podataka podeljen po `vehicle_id`, stanje se čuva po vozilu kroz `MapState`/`ValueState`). Za svako vozilo prati u kojim od 6 definisanih geografskih zona se nalazi, i generiše pet tipova rezultata:

- **ENTER** — ulazak vozila u zonu
- **EXIT** — izlazak, sa tačnim `dwell_seconds` (ukupno trajanje posete)
- **DWELL** — periodični signal koji ukazuje da je vozilo u zoni (svakih 60s, nakon 120s zadržavanja)
- **ALERT** — emituje se jednom, kad zadržavanje pređe prag (30 min) — detekcija anomalnog zadržavanja
- **TRANSITION** — sekvenca kretanja između zona sa vremenom putovanja

ENTER/EXIT/DWELL/ALERT upisuju se u tabelu `zone_events`, TRANSITION u `zone_transitions`.

### Job 2 — `density_job.py`

Koristi Flink-ov **ugrađeni `window()` API** (event time, tumbling window od 5 minuta, sa watermark strategijom tolerantnom na 10s kasnih zapisa). Za svaku zonu broji **broj distinktnih vozila** viđenih u toj zoni tokom trajanja prozora (gustina saobraćaja), kao i listu linija (`route_id`) koje su bile prisutne. Rezultat se upisuje u tabelu `zone_density`.

## Skladištenje rezultata

Rezultati oba job-a upisuju se u ClickHouse.

ClickHouse tabele se kreiraju automatski pri prvom pokretanju kontejnera, putem `clickhouse-init/01-create-tables.sql` skripte (`docker-entrypoint-initdb.d` mehanizam). 

## Pokretanje

### Podizanje infrastrukture
```bash
docker compose up -d --build
```

### Pokretanje Flink job-ova
```bash
docker compose exec flink-jobmanager ./bin/flink run -py /opt/flink/usrlib/geofencing_job.py
docker compose exec flink-jobmanager ./bin/flink run -py /opt/flink/usrlib/density_job.py
```

### Provera statusa job-ova
```bash
docker compose exec flink-jobmanager ./bin/flink list
```

### Zaustavljanje job-a
```bash
docker compose exec flink-jobmanager ./bin/flink cancel <JOB_ID>
```

### Pristup ClickHouse klijentu
```bash
docker compose exec -it clickhouse clickhouse-client 
```

### Web interfejsi
- Flink Web UI: `http://localhost:8081`
- Kafka UI: `http://localhost:8080`
- Superset: `http://localhost:8088`

## Korisni upiti za proveru podataka

**Istorija jednog konkretnog vozila:**
```sql
SELECT event_time, vehicle_id, zone_name, event_type, dwell_seconds
FROM transit.zone_events
WHERE vehicle_id = ''
ORDER BY event_time
```

**Istorija vozila koje je poslednje izvršilo EXIT:**
```sql
SELECT event_time, vehicle_id, zone_name, event_type, dwell_seconds
FROM transit.zone_events
WHERE vehicle_id = (
    SELECT vehicle_id FROM transit.zone_events
    WHERE event_type = 'EXIT' ORDER BY event_time DESC LIMIT 1
)
ORDER BY event_time
```

**Poslednjih 10 ALERT događaja (predugo zadržavanje):**
```sql
SELECT * FROM transit.zone_events
WHERE event_type = 'ALERT'
ORDER BY event_time DESC LIMIT 10
```

**Najnovija gustina po zoni:**
```sql
SELECT * FROM transit.zone_density
ORDER BY window_end DESC LIMIT 20
```

**Najčešće tranzicije između zona:**
```sql
SELECT from_zone_name, to_zone_name, count(*) as cnt, avg(travel_time_seconds) as avg_time
FROM transit.zone_transitions
GROUP BY from_zone_name, to_zone_name
ORDER BY cnt DESC
```

