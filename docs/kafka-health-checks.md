# Kafka Health Checks

Dette dokumentet beskriver hvordan health-checkene i `fint-core-consumer` fungerer etter innføringen av Kafka-basert readiness og liveness.

## Oversikt

Applikasjonen bruker tre forskjellige typer health/probes i Kubernetes:

- `startupProbe`: brukes bare helt tidlig for å verifisere at JVM og Spring Boot faktisk starter.
- `readinessProbe`: brukes for å avgjøre om poden trygt kan motta trafikk.
- `livenessProbe`: brukes for å avgjøre om poden fortsatt lever, eller om Kubernetes skal restarte den.

Disse probe-typene har forskjellig ansvar og skal ikke blandes:

- `startupProbe` skal ikke vite noe om hvor langt Kafka-consumerne har kommet i bootstrap.
- `readinessProbe` skal blokkere trafikk til initial bootstrap er ferdig.
- `livenessProbe` skal ikke feile bare fordi applikasjonen ligger litt etter i konsumering; den skal feile hvis Kafka-consumerne i praksis har sluttet å fungere over tid.

## Hvordan Consumer bruker dem

Consumer eksponerer actuator-endepunktene:

- `/actuator/health/readiness`
- `/actuator/health/liveness`

Readiness er koblet til en initial Kafka-bootstrap-tracker. Liveness er koblet til en separat Kafka-runtime-monitor.

## Readiness

### Hensikt

Readiness skal beskytte trafikk mot en pod som ennå ikke har bygd opp lokal cache ved oppstart.

### Hvordan den virker

Ved oppstart settes readiness til `REFUSING_TRAFFIC`.

To listeners er definert som blokkerende for initial bootstrap:

- `entity`
- `relation-update`

For hver assigned partition hentes "startup end offset" fra Kafka i det assignment skjer. Deretter spores prosesserte offsets mens records behandles.

Listeneren regnes som ferdig når alle dens assigned partitions har konsumert seg opp til offseten som gjaldt ved oppstartstidspunktet.

Applikasjonen regnes som `ready` når alle blokkerende listeners er ferdige.

### Viktig nyanse

Dette er en `initial-only` readiness.

Det betyr:

- Readiness blokkerer ved oppstart.
- Readiness går til `UP` når initial bootstrap er ferdig.
- Readiness går ikke ned igjen senere bare fordi det kommer flere meldinger, full sync, eller midlertidig lag.

Dette er bevisst. Etter at poden er sluppet i trafikk, skal vanlig Kafka-lag ikke stoppe lesetrafikk.

### Hva får readiness til å feile

Readiness blir `OUT_OF_SERVICE` hvis minst ett av disse forholdene gjelder under oppstart:

- `entity` har ikke konsumert alle sine assigned partitions opp til startup-end-offset.
- `relation-update` har ikke konsumert alle sine assigned partitions opp til startup-end-offset.
- Kafka end-offset kan ikke hentes for assigned partitions.

### Hva får ikke readiness til å feile

Følgende forhold feiler ikke readiness etter at bootstrap er ferdig:

- En ny full sync kommer inn og skaper lag.
- Det produseres mange meldinger mens appen kjører.
- Consumeren ligger midlertidig etter på topicet.
- Topicet er stille i lang tid.

## Liveness

### Hensikt

Liveness skal oppdage at Kafka-consumerne i praksis har sluttet å fungere, og gi Kubernetes grunnlag for å restarte poden.

### Hvordan den virker

Liveness monitorerer runtime-status for registrerte Kafka-listeners.

Den ser ikke på vanlig lag eller antall uprosesserte meldinger. I stedet ser den på Kafka-runtime-signaler:

- `ConsumerStartedEvent`
- `ListenerContainerIdleEvent`
- `NonResponsiveConsumerEvent`
- `ConsumerFailedToStartEvent`
- `ConsumerStoppedEvent`

Det brukes en grace-periode, default `15m`, for å unngå falske positive ved korte forstyrrelser.

### Hva som holder liveness frisk

Liveness holdes `UP` hvis en listener viser tegn til normal drift, for eksempel:

- appen prosesserer records
- listeneren sender idle-events fordi topicet er stille
- consumer-containeren starter normalt

Det betyr at stille topics ikke i seg selv gjør poden unhealthy.

### Hva får liveness til å feile

Liveness blir `DOWN` hvis en registrert listener har en runtime-feil som varer lenger enn konfigurert grace-periode.

Eksempler:

- `NonResponsiveConsumerEvent` og tilstanden varer lenger enn grace-perioden
- `ConsumerFailedToStartEvent`
- `ConsumerStoppedEvent` med annen grunn enn `NORMAL`

Typiske scenarioer dette er ment å fange:

- nettverksbrudd mellom pod og Kafka
- Kafka svarer ikke over tid
- autentiseringsfeil mot Kafka
- consumer-container stopper på grunn av feil

### Hva får ikke liveness til å feile

Følgende forhold skal ikke alene feile liveness:

- vanlig Kafka-lag
- full sync som gjør at consumeren henger litt etter
- ingen nye meldinger på topicet i flere timer
- normal rebalance mellom pods

En vanlig rebalance håndteres av partition assignment/revocation, ikke som en fatal liveness-feil.

## Startup Probe

### Hensikt

`startupProbe` bør bare brukes som en enkel oppstartssperre mens JVM og Spring Boot kommer opp.

Den bør ikke inneholde Kafka-bootstrap-logikk. Grunnen er at `startupProbe` bare brukes i den tidlige oppstartsfasen, mens readiness kan uttrykke "ikke klar enda" på en mer presis måte.

### Anbefaling

Bruk `startupProbe` mot en enkel actuator-health, mens readiness og liveness peker mot de dedikerte probe-endepunktene.

## Konfigurasjon i Consumer

Default konfigurasjon i applikasjonen:

```yaml
fint:
  consumer:
    health:
      kafka:
        idle-event-interval: 1m
        runtime-grace-period: 15m
        monitor-interval-seconds: 30
        no-poll-threshold: 3.0

management:
  endpoint:
    health:
      probes:
        enabled: true
      group:
        readiness:
          include: readinessState,initialKafkaBootstrap
        liveness:
          include: livenessState,kafkaRuntime
```

### Hva disse Kafka-innstillingene betyr

- `idle-event-interval`: hvor ofte idle-event sendes mens et topic er stille.
- `runtime-grace-period`: hvor lenge runtime-feil kan vare før liveness går ned.
- `monitor-interval-seconds`: hvor ofte Spring Kafka sjekker consumerens poll-aktivitet.
- `no-poll-threshold`: terskel for når manglende poll anses som "non-responsive".

## Metrikker

I tillegg til actuator-health eksponerer applikasjonen nå Micrometer-metrikker for Kafka-bootstrap og Kafka-runtime-health. Disse er nyttige fordi health-endepunktene bare viser nåværende status, mens metrikker gjør det mulig å følge utvikling over tid i Prometheus og Grafana.

### Bootstrap-metrikker

- `fint.consumer.kafka.bootstrap.state`
  Gauge per listener. `1` betyr at initial bootstrap fortsatt pågår, `0` betyr at den er ferdig.

- `fint.consumer.kafka.bootstrap.partitions.pending`
  Gauge per listener. Antall assigned partitions som ennå ikke har konsumert seg opp til startup-end-offset.

- `fint.consumer.kafka.bootstrap.completed`
  Counter per listener, og også for `listener=all`. Incrementes når bootstrap fullføres.

- `fint.consumer.kafka.bootstrap.duration`
  Timer per listener, og også for `listener=all`. Måler hvor lang tid initial bootstrap faktisk tok.

- `fint.consumer.kafka.bootstrap.end_offset.lookup.failures`
  Counter per listener. Incrementes når applikasjonen ikke klarer å hente startup end offset fra Kafka.

### Runtime-metrikker

- `fint.consumer.kafka.runtime.problem`
  Counter med tags `listener` og `reason`. Incrementes når runtime-monitoren ser et problem, for eksempel `NON_RESPONSIVE` eller `STOPPED_AUTH`.

- `fint.consumer.kafka.runtime.unhealthy`
  Gauge per listener. `1` betyr at listeneren har vært i problemtilstand lenger enn grace-perioden og dermed gjør liveness `DOWN`.

- `fint.consumer.kafka.runtime.problem.duration`
  Gauge per listener. Viser hvor lenge den nåværende problemtilstanden har vart, i millisekunder.

### Viktige tags

Metrikkene er bevisst tagget lavt-kardinalt:

- `listener`
- `reason`

Det brukes ikke tags som Kafka-key, partition eller corrId, for å unngå høy kardinalitet og unødvendig støy i metrics-backend.

## Eksempel i Kubernetes

Eksempel på probe-oppsett:

```yaml
startupProbe:
  httpGet:
    path: /utdanning/vurdering/actuator/health
    port: 8080
  periodSeconds: 5
  timeoutSeconds: 3
  failureThreshold: 60

readinessProbe:
  httpGet:
    path: /utdanning/vurdering/actuator/health/readiness
    port: 8080
  periodSeconds: 5
  timeoutSeconds: 3
  failureThreshold: 3

livenessProbe:
  httpGet:
    path: /utdanning/vurdering/actuator/health/liveness
    port: 8080
  periodSeconds: 30
  timeoutSeconds: 3
  failureThreshold: 3
```

## Praktiske konsekvenser

Med dette oppsettet blir flyten typisk slik:

1. Poden starter.
2. `startupProbe` verifiserer at appen faktisk kommer opp.
3. `readinessProbe` holder poden ute av trafikk mens `entity` og `relation-update` bygger initial cache.
4. Når initial bootstrap er ferdig, blir poden `Ready`.
5. Senere full sync eller vanlig Kafka-lag påvirker ikke readiness.
6. Hvis Kafka-consumerne blir ikke-responsive eller stopper over tid, blir `liveness` `DOWN` og Kubernetes kan restarte poden.

## Oppsummering

- `startupProbe` beskytter bare oppstart av prosessen.
- `readinessProbe` beskytter trafikk under initial Kafka-bootstrap.
- `livenessProbe` beskytter mot vedvarende Kafka-runtime-feil.
- Vanlig lag eller stille topics skal ikke gjøre poden unhealthy.
