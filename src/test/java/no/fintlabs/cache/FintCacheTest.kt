package no.fintlabs.cache

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import no.novari.fint.model.felles.kompleksedatatyper.Identifikator
import no.novari.fint.model.resource.utdanning.elev.ElevResource
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import java.io.File
import java.nio.file.Files
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class FintCacheTest {
    private lateinit var cache: FintCache<ElevResource>
    private lateinit var dbPath: String

    @BeforeEach
    fun setUp() {
        dbPath = Files.createTempDirectory("fint-cache-test-").toString()
        cache =
            FintCache(
                dbPath = dbPath,
                blockCacheSizeBytes = 8 * 1024 * 1024L,
                objectMapper =
                    ObjectMapper()
                        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                        .registerModule(KotlinModule.Builder().build()),
            )
    }

    @AfterEach
    fun tearDown() {
        cache.close()
        File(dbPath).deleteRecursively()
    }

    @Test
    fun `cache size is empty when nothing is added`() {
        assertEquals(0, cache.size)
    }

    @Test
    fun `put with different ids are added as individual entries`() {
        val elevA = createElevResource("A")
        val elevB = createElevResource("B")
        val elevC = createElevResource("C")
        val elevD = createElevResource("D")
        cache.put(elevA.systemId.identifikatorverdi, elevA, 0)
        cache.put(elevB.systemId.identifikatorverdi, elevB, 1)
        cache.put(elevC.systemId.identifikatorverdi, elevC, 2)
        cache.put(elevD.systemId.identifikatorverdi, elevD, 3)

        assertEquals(4, cache.size)
    }

    @Test
    fun `put with same id replaces existing entry for that id`() {
        val elevAVersion1 = createElevResource("A")
        val elevAVersion2 = createElevResource("A")
        val elevAVersion3 = createElevResource("A")
        val elevAVersion4 = createElevResource("A")
        cache.put(elevAVersion1.systemId.identifikatorverdi, elevAVersion1, 0)
        cache.put(elevAVersion2.systemId.identifikatorverdi, elevAVersion2, 1)
        cache.put(elevAVersion3.systemId.identifikatorverdi, elevAVersion3, 2)
        cache.put(elevAVersion4.systemId.identifikatorverdi, elevAVersion4, 3)

        assertEquals(1, cache.size)
        assertElevEquals(elevAVersion4, cache.get(elevAVersion4.systemId.identifikatorverdi))
        assertElevEquals(elevAVersion4, cache.getByIdField("brukernavn", elevAVersion4.brukernavn.identifikatorverdi))
        assertElevEquals(elevAVersion4, cache.getByIdField("feidenavn", elevAVersion4.feidenavn.identifikatorverdi))
    }

    @Test
    fun `put with older timestamp does not overwrite newer entry`() {
        val elevV1 = createElevResource("A")
        val elevV2 = createElevResource("A")
        cache.put(elevV1.systemId.identifikatorverdi, elevV1, 10)
        cache.put(elevV2.systemId.identifikatorverdi, elevV2, 5)

        assertElevEquals(elevV1, cache.get("A"))
    }

    @Test
    fun `put with same timestamp overwrites existing entry`() {
        val elevV1 = createElevResource("A")
        val elevV2 = createElevResource("A")
        cache.put(elevV1.systemId.identifikatorverdi, elevV1, 10)
        cache.put(elevV2.systemId.identifikatorverdi, elevV2, 10)

        assertElevEquals(elevV2, cache.get("A"))
    }

    @Test
    fun `resources can be retrieved by other id fields than the main id`() {
        val elevA = createElevResource("A")
        val elevB = createElevResource("B")
        val elevC = createElevResource("C")
        val elevD = createElevResource("D")
        cache.put(elevA.systemId.identifikatorverdi, elevA, 0)
        cache.put(elevB.systemId.identifikatorverdi, elevB, 1)
        cache.put(elevC.systemId.identifikatorverdi, elevC, 2)
        cache.put(elevD.systemId.identifikatorverdi, elevD, 3)

        assertEquals(4, cache.size)

        assertElevEquals(elevA, cache.get("A"))
        assertElevEquals(elevA, cache.getByIdField("systemId", "A"))
        assertElevEquals(elevA, cache.getByIdField("brukernavn", elevA.brukernavn.identifikatorverdi))
        assertElevEquals(elevA, cache.getByIdField("feidenavn", elevA.feidenavn.identifikatorverdi))

        assertElevEquals(elevB, cache.get("B"))
        assertElevEquals(elevB, cache.getByIdField("systemId", "B"))
        assertElevEquals(elevB, cache.getByIdField("brukernavn", elevB.brukernavn.identifikatorverdi))
        assertElevEquals(elevB, cache.getByIdField("feidenavn", elevB.feidenavn.identifikatorverdi))

        assertElevEquals(elevC, cache.get("C"))
        assertElevEquals(elevC, cache.getByIdField("systemId", "C"))
        assertElevEquals(elevC, cache.getByIdField("brukernavn", elevC.brukernavn.identifikatorverdi))
        assertElevEquals(elevC, cache.getByIdField("feidenavn", elevC.feidenavn.identifikatorverdi))

        assertElevEquals(elevD, cache.get("D"))
        assertElevEquals(elevD, cache.getByIdField("systemId", "D"))
        assertElevEquals(elevD, cache.getByIdField("brukernavn", elevD.brukernavn.identifikatorverdi))
        assertElevEquals(elevD, cache.getByIdField("feidenavn", elevD.feidenavn.identifikatorverdi))
    }

    @Test
    fun `resources can be removed using the main id`() {
        val elevA = createElevResource("A")
        val elevB = createElevResource("B")
        val elevC = createElevResource("C")
        val elevD = createElevResource("D")
        cache.put(elevA.systemId.identifikatorverdi, elevA, 0)
        cache.put(elevB.systemId.identifikatorverdi, elevB, 1)
        cache.put(elevC.systemId.identifikatorverdi, elevC, 2)
        cache.put(elevD.systemId.identifikatorverdi, elevD, 3)

        assertEquals(4, cache.size)

        cache.remove(elevA.systemId.identifikatorverdi, 4)
        assertEquals(3, cache.size)

        cache.remove(elevB.systemId.identifikatorverdi, 5)
        assertEquals(2, cache.size)

        cache.remove(elevC.systemId.identifikatorverdi, 6)
        assertEquals(1, cache.size)

        cache.remove(elevD.systemId.identifikatorverdi, 7)
        assertEquals(0, cache.size)
    }

    @Test
    fun `remove with older timestamp does not remove entry`() {
        val elev = createElevResource("A")
        cache.put(elev.systemId.identifikatorverdi, elev, 10)
        cache.remove(elev.systemId.identifikatorverdi, 5)

        assertEquals(1, cache.size)
        assertElevEquals(elev, cache.get("A"))
    }

    @Test
    fun `remove with equal timestamp does not remove entry`() {
        val elev = createElevResource("A")
        cache.put(elev.systemId.identifikatorverdi, elev, 10)
        cache.remove(elev.systemId.identifikatorverdi, 10)

        assertEquals(1, cache.size)
        assertElevEquals(elev, cache.get("A"))
    }

    @Test
    fun `put with stale timestamp does not update lastUpdated`() {
        val elevV1 = createElevResource("A")
        val elevV2 = createElevResource("A")
        cache.put(elevV1.systemId.identifikatorverdi, elevV1, 10)
        cache.put(elevV2.systemId.identifikatorverdi, elevV2, 5)

        assertEquals(10, cache.lastUpdated)
    }

    @Test
    fun `remove with stale timestamp does not update lastUpdated`() {
        val elev = createElevResource("A")
        cache.put(elev.systemId.identifikatorverdi, elev, 10)
        cache.remove(elev.systemId.identifikatorverdi, 5)

        assertEquals(10, cache.lastUpdated)
    }

    @Test
    fun `lastUpdated returns timestamp of last cache change`() {
        val elevA = createElevResource("A")
        val elevB = createElevResource("B")
        val elevC = createElevResource("C")
        val elevD = createElevResource("D")

        cache.put(elevA.systemId.identifikatorverdi, elevA, 10)
        assertEquals(10, cache.lastUpdated)

        cache.put(elevB.systemId.identifikatorverdi, elevB, 11)
        assertEquals(11, cache.lastUpdated)

        cache.put(elevC.systemId.identifikatorverdi, elevC, 12)
        assertEquals(12, cache.lastUpdated)

        cache.put(elevD.systemId.identifikatorverdi, elevD, 13)
        assertEquals(13, cache.lastUpdated)

        // Evict the two first resources ->
        cache.evictExpired(12)
        assertEquals(13, cache.lastUpdated)
        assertEquals(2, cache.size)

        cache.remove(elevC.systemId.identifikatorverdi, 20)
        assertEquals(20, cache.lastUpdated)

        cache.remove(elevD.systemId.identifikatorverdi, 21)
        assertEquals(21, cache.lastUpdated)
        assertEquals(0, cache.size)
    }

    @Test
    fun `evictExpired removes entries from indexes`() {
        val elevA = createElevResource("A")
        val elevB = createElevResource("B")

        cache.put(elevA.systemId.identifikatorverdi, elevA, 10)
        cache.put(elevB.systemId.identifikatorverdi, elevB, 20)

        assertElevEquals(elevA, cache.getByIdField("brukernavn", elevA.brukernavn.identifikatorverdi))
        assertElevEquals(elevB, cache.getByIdField("brukernavn", elevB.brukernavn.identifikatorverdi))

        cache.evictExpired(15)

        assertNull(cache.getByIdField("brukernavn", elevA.brukernavn.identifikatorverdi))
        assertElevEquals(elevB, cache.getByIdField("brukernavn", elevB.brukernavn.identifikatorverdi))
    }

    @Test
    fun `removeFromIndexes handles null identifikatorverdi`() {
        val id = "crash-test-id"
        val elev = createElevResource(id)

        cache.put(id, elev, 100)

        elev.brukernavn.identifikatorverdi = null

        assertDoesNotThrow {
            cache.put(id, createElevResource(id), 101)
        }
    }

    @Test
    fun `removeFromIndexes does not throw exception when a resource inside cache has a null Identifikator object`() {
        val id = "crash-test-id"
        val elev = createElevResource(id)

        cache.put(id, elev, 200)

        elev.brukernavn = null

        assertDoesNotThrow {
            cache.put(id, createElevResource(id), 201)
        }
    }

    @Test
    fun `updateIndexes does not throws NullPointerException when putting a resource with a null identifikatorverdi`() {
        val id = "crash-test-update-indexes"
        val elev = createElevResource(id)

        elev.brukernavn.identifikatorverdi = null

        assertDoesNotThrow {
            cache.put(id, elev, 300)
        }
    }

    private fun assertElevEquals(
        expected: ElevResource,
        actual: ElevResource?,
    ) {
        assertNotNull(actual)
        assertEquals(expected.systemId.identifikatorverdi, actual.systemId.identifikatorverdi)
        assertEquals(expected.brukernavn?.identifikatorverdi, actual.brukernavn?.identifikatorverdi)
        assertEquals(expected.feidenavn?.identifikatorverdi, actual.feidenavn?.identifikatorverdi)
    }

    private fun createElevResource(id: String): ElevResource {
        val elevResource = ElevResource()
        elevResource.systemId =
            object : Identifikator() {
                init {
                    identifikatorverdi = id
                }
            }
        elevResource.brukernavn =
            object : Identifikator() {
                init {
                    identifikatorverdi = UUID.randomUUID().toString()
                }
            }
        elevResource.feidenavn =
            object : Identifikator() {
                init {
                    identifikatorverdi = UUID.randomUUID().toString()
                }
            }
        return elevResource
    }
}
