# Autorelation Test Checklist

Goal: 100% accuracy for auto-relation back-linking. This checklist tracks which scenarios have dedicated integration test coverage. Tick a box when an IT exists and passes.

Legend: `[x]` covered · `[~]` in progress · `[ ]` not covered

## §1 Cardinality / structural rules

- [x] 1:M within-component — `OneToManyIT.WithinComponent`
- [x] 1:M cross-component, source side publishes — `OneToManyIT.CrossComponent`
- [ ] 1:M cross-component, target receives back-link (two Spring contexts)
- [x] M:M within-component: add, prune, non-source guard, preserve — `ManyToManyIT`
- [ ] M:M cross-component, target receives
- [ ] Shared (`felles`) resource (e.g. Person) → target in another component
- [ ] Shared-to-shared (e.g. Person ↔ Kontaktperson)
- [x] 1:1 and M:1 correctly skipped (no rule generated) — `RelationRuleValidationIT`

## §2 Lifecycle transitions

- [x] Entity-first: relation update applied on arrival — `AutoRelationIT`
- [x] Relation-update-first: buffered, applied when entity arrives — `AutoRelationIT`
- [x] Pruning when source drops a link — `OneToManyIT`, `ManyToManyIT`
- [x] Duplicate ADD is idempotent — `AutoRelationIT`
- [x] Multiple target IDs on one M:M source (all get back-links) — `ManyToManyIT` (3 targets)
- [x] DELETE-before-ADD ordering (buffer scenario) — `AutoRelationIT` (`cancels pending add when a matching delete arrives first`)
- [x] Move link: Target-A → Target-B (A loses, B gains) — `OneToManyIT.WithinComponent.moving elev link from A to B ...`
- [ ] Entity tombstone → DELETE published for all its relations
- [ ] Cache eviction after full sync → DELETE published
- [ ] Duplicate DELETE (idempotency)
- [ ] Late ADD with older timestamp than buffered DELETE

## §3 Restart / replay

- [x] `EntityConsumer` seek-to-beginning rebuilds cache correctly after restart — `EntityConsumerRestartIT`
- [x] Cold restart: cache + back-links rebuilt via entity + relation-update topic replay — `ColdRestartIT`
- [ ] `AutoRelationEntityConsumer` continue-from-offset does NOT re-publish ADDs on restart
- [ ] Restart with unresolved-relation buffer still populated drains correctly

## §4 Kafka mechanics

- [x] Compaction on relation-update topic retains only the latest message per key — `CompactionBehaviourIT`
- [x] ADD / DELETE with the same Kafka key collide on the topic (Kafka-level proof, not a loss claim on its own) — `CompactionBehaviourIT`
- [x] `FintCache.put()` rejects writes with older timestamps — `FintCacheTest` (`AutoRelationService.putInCache` dodges rejection via `maxOf(relationUpdate.timestamp, cache.lastUpdatedByResourceId(id))`; `EntityProcessingService.addToCache` uses the raw record timestamp and *can* silently drop out-of-order writes)
- [x] `UnresolvedRelationCache` evicts buffered entries after TTL — `UnresolvedRelationCacheTest`

## §5 Full sync behaviour

- [ ] Full sync evicts stale entries → DELETE published for evictees
- [ ] Concurrent full syncs (`ConcurrentFullSync` state) handled safely
- [ ] Full sync with in-flight relation updates arriving mid-sync
- [ ] Full sync followed by DELTA sync

## §6 Cross-service (two consumers on shared broker)

- [x] Service A produces entity → Service B applies back-link — `CrossServiceBackLinkIT`
- [x] Service B restarts, replays relation-update topic, rebuilds correctly — `CrossServiceRestartIT`
- [x] Service A evicts stale entity → Service B drops back-link on DELETE — `CrossServiceEvictionIT`
- [x] A + B interleaved traffic under sustained load — `CrossServiceLoadIT` (50 interleaved pairs)

---

## Test harness

- [x] Testcontainers Kafka smoke test — `TestcontainersKafkaSmokeIT`, `KafkaTestcontainersSupport`

---

## Confirmed bugs

**B1 — Stale-timestamp buffer loss** (reproduced by `StaleBufferLossIT`).
An ADD on the relation-update topic whose `timestamp` is older than `UnresolvedRelationCache.ttl` (default 30 days) and whose target is not yet cached is silently lost. Caffeine's `expireAfterCreate` returns a negative duration → clamped to 0 → the buffer entry is evicted before it can be drained. When the target later arrives, `applyPendingLinks` finds nothing and the back-link is never applied. Reachable in production when (a) a buffered ADD is replayed after a consumer restart once the message is older than TTL, or (b) a target takes longer than TTL to appear on its entity topic. Consistent with the missing_back_link class of production defects (~0.14% of links). Fix candidates: use `Instant.now()` as `createdAt` for TTL math, drop the `createdAt` field entirely and rely on Caffeine's insertion time, or dead-letter stale ADDs instead of buffering them.

## Observability / Instrumentation

Goal: surface silent-loss paths as metric counters so production can quantify each failure mode without waiting for tests to catch it. Implement one at a time, each with a focused unit/IT test that asserts the counter moves.

### A. `UnresolvedRelationCache` buffer metrics — `fint.autorelation.buffer.records` (counter)
- [x] A.1 `registered` and `appended` outcomes
- [x] A.2 `drained` outcome (per-link on `takeRelations`)
- [x] A.3 `removed_by_delete` outcome (links removed via `removeRelation`)
- [x] A.4 `stillborn` outcome (fires on the B1 path; stale registrations short-circuit out of the cache entirely)
- [x] A.5 `expired` outcome via Caffeine `evictionListener` (synchronous)
- [x] A.6 `fint.autorelation.buffer.size` gauge (total; no per-resource tag)

### B. `FintCache` write-path metrics
- [x] B.1 `fint.consumer.cache.put` counter: `accepted` / `rejected_stale`
- [x] B.2 `fint.consumer.cache.remove` counter: `accepted` / `rejected_stale` / `missing`
- [x] B.3 `fint.consumer.cache.evicted` counter: `full_sync` (from `CacheEvictionService`)

### C. `AutoRelationService` branch metrics
- [ ] C.1 `fint.autorelation.apply` counter: `applied` / `buffered` per target-id
- [ ] C.2 `apply` failure outcomes: `skipped_for_each`, `skipped_unknown_class`, `failed_deep_copy`, `failed_put`, `failed_other`
- [ ] C.3 `fint.autorelation.reconcile` counter: `preserved` / `hydrated` / `pruned`

### D. Dead-letter queue (after A–C land)
- [ ] D.1 Replace logging error handler on `RelationUpdateConsumer` with DLQ + bounded retry
- [ ] D.2 Same for `EntityConsumer`
- [ ] D.3 Same for `AutoRelationEntityConsumer`

---

## Candidate loss scenarios to investigate

Places where back-links could plausibly go missing. Not claims — hypotheses to verify with targeted tests.

1. **Race between AutoRelationEntityConsumer (ADD) and EntityConsumer prune (DELETE)** — both consume the same entity record under different consumer groups, so publication order of ADD and DELETE to the relation-update topic is non-deterministic. A DELETE-then-ADD ordering is harmless, but ADD-then-DELETE compacts to DELETE-only.

2. **Restart between a pruning DELETE and the next full-sync ADD** — if a consumer restarts in this window and the relation-update topic has been compacted, targets that were in the pre-DELETE ADD but not in the DELETE's `targetIds` temporarily lose back-links until the next full sync.

3. **`UnresolvedRelationCache` TTL expiry** — a buffered relation update whose target entity never arrives within TTL is evicted and lost.

4. **`FintCache.put()` timestamp rejection on entity writes** — `AutoRelationService.putInCache` is protected by `maxOf(relationUpdate.timestamp, cache.lastUpdatedByResourceId(id))`, so relation-update-driven writes are safe. **But `EntityProcessingService.addToCache` uses the raw Kafka record timestamp**: if a freshly-reconciled entity arrives with a timestamp older than what is already cached (e.g. out-of-order replay, seek-to-beginning reprocessing), the write is silently rejected and the reconciled links in that copy never reach the cache.

5. **`AutoRelationEntityConsumer` uses `continueFromPreviousOffset`** — on restart it does not re-publish ADDs for entities already in the entity topic, so any ADDs lost on the relation-update topic are not replenished until the next adapter-driven republish.

6. **Full-sync cache eviction race** — `CacheEvictionService` evicts stale entities after a full sync and publishes DELETEs. A relation update arriving for the target at the same moment may be rejected or buffered in a way that loses the back-link.

7. **Concurrent full syncs** (`ConcurrentFullSync` state) — overlapping full syncs can disagree on which entries are stale; the losing sync's evictions/DELETEs may misrepresent ground truth.

8. **Adapter re-publish without back-links + safety-net failure** — `reconcileLinks` preserves inverse links on re-arrival, but the safety-net filter removes managed relation names. If the classification of "managed" vs "inverse" is wrong for a rule, back-links are dropped on every re-publish.

9. **Adapter publishes each entity as its own `FULL` sync with `totalSize=1`** — `SyncTrackerService` completes a full sync on every message and `CacheEvictionService` evicts all entities not present in that sync. Everything except the just-arrived record gets evicted, and eviction tombstones trigger DELETEs on the relation-update topic. If the adapter uses `SyncType.FULL` with wrong `totalSize` or fresh `corrId` per record, this would look like massive, systematic relation-update loss — worth checking a production sync trace for adapter behaviour.
