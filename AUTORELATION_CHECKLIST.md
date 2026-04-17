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
- [ ] Multiple target IDs on one M:M source (all get back-links)
- [ ] Move link: Target-A → Target-B (A loses, B gains)
- [ ] Entity tombstone → DELETE published for all its relations
- [ ] Cache eviction after full sync → DELETE published
- [ ] Duplicate DELETE / DELETE-before-ADD ordering
- [ ] Late ADD with older timestamp than buffered DELETE

## §3 Restart / replay

- [ ] `EntityConsumer` seek-to-beginning rebuilds cache correctly after restart
- [ ] `RelationUpdateConsumer` seek-to-beginning replays buffered updates after restart
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

- [ ] Service A produces entity → Service B applies back-link
- [ ] Service B restarts, replays relation-update topic, rebuilds correctly
- [ ] Service A evicts stale entity → Service B drops back-link on DELETE
- [ ] A + B interleaved traffic under sustained load

---

## Test harness

- [x] Testcontainers Kafka smoke test — `TestcontainersKafkaSmokeIT`, `KafkaTestcontainersSupport`

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
