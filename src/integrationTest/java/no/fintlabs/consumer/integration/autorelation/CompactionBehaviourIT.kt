package no.fintlabs.consumer.integration.autorelation

import no.fintlabs.config.KafkaTestcontainersSupport
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.awaitility.kotlin.await
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.Properties
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Verifies raw Kafka log-compaction behaviour on a compacted topic configured like the
 * production relation-update topic. These tests do not boot Spring — they prove the
 * broker-level invariant (latest message per key wins) and demonstrate the ADD/DELETE
 * key-collision that is the suspected source of the ~99% relation-update loss in production.
 */
class CompactionBehaviourIT : KafkaTestcontainersSupport() {
    @Test
    fun `compaction retains only the latest message per key`() {
        val topic = "compaction-baseline-${UUID.randomUUID()}"
        createCompactedTopic(topic)

        createProducer().use { producer ->
            producer.send(ProducerRecord(topic, "target-key", "v1")).get()
            producer.send(ProducerRecord(topic, "target-key", "v2")).get()
            producer.send(ProducerRecord(topic, "target-key", "v3")).get()
            // Push bytes so the active segment rolls — the active segment is never compacted.
            producePadding(producer, topic)
        }

        await.atMost(Duration.ofSeconds(30)).untilAsserted {
            val values =
                consumeAllFromBeginning(topic)
                    .filter { (k, _) -> k == "target-key" }
                    .map { (_, v) -> v }
            assertEquals(1, values.size, "Expected one record for 'target-key' after compaction, got: $values")
            assertEquals("v3", values.single())
        }
    }

    @Test
    fun `ADD and DELETE with the same key collide after compaction`() {
        val topic = "compaction-collision-${UUID.randomUUID()}"
        createCompactedTopic(topic)

        // Mirrors the production key format:
        // {sourceResource}/{sourceId}#{targetResource}#{inverseRelation}
        val key = "kontaktlarergruppe/gruppe-1#undervisningsforhold#kontaktlarergruppe"

        createProducer().use { producer ->
            // AutoRelationEntityConsumer path: ADD back-link to all linked undervisningsforhold.
            producer.send(ProducerRecord(topic, key, "ADD targetIds=[u1,u2,u3]")).get()
            // EntityConsumer prune path: DELETE back-link on a dropped target — same key.
            producer.send(ProducerRecord(topic, key, "DELETE targetIds=[u3]")).get()
            producePadding(producer, topic)
        }

        await.atMost(Duration.ofSeconds(30)).untilAsserted {
            val collided =
                consumeAllFromBeginning(topic)
                    .filter { (k, _) -> k == key }
                    .map { (_, v) -> v }
            assertEquals(
                1,
                collided.size,
                "ADD and DELETE share the same key; after compaction only one may remain. Got: $collided",
            )
            assertTrue(
                collided.single().startsWith("DELETE"),
                "Latest wins — DELETE was produced after ADD. Got: ${collided.single()}",
            )
        }
    }

    private fun producePadding(
        producer: KafkaProducer<String, String>,
        topic: String,
        records: Int = 30,
        valueSize: Int = 200,
    ) {
        val padding = "x".repeat(valueSize)
        repeat(records) { i ->
            producer.send(ProducerRecord(topic, "padding-$i", padding)).get()
        }
    }

    private fun createProducer(): KafkaProducer<String, String> {
        val props =
            Properties().apply {
                put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.bootstrapServers)
                put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
                put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
                put(ProducerConfig.ACKS_CONFIG, "all")
            }
        return KafkaProducer(props)
    }

    private fun consumeAllFromBeginning(topic: String): List<Pair<String, String>> {
        val props =
            Properties().apply {
                put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.bootstrapServers)
                put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
                put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
                put(ConsumerConfig.GROUP_ID_CONFIG, "compaction-verify-${UUID.randomUUID()}")
                put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
            }
        KafkaConsumer<String, String>(props).use { consumer ->
            consumer.subscribe(listOf(topic))
            val records = mutableListOf<Pair<String, String>>()
            val deadline = System.currentTimeMillis() + 2000
            var idleLoops = 0
            while (System.currentTimeMillis() < deadline && idleLoops < 3) {
                val batch = consumer.poll(Duration.ofMillis(300))
                if (batch.isEmpty) {
                    idleLoops++
                } else {
                    idleLoops = 0
                    batch.forEach { records.add(it.key() to it.value()) }
                }
            }
            return records
        }
    }
}
