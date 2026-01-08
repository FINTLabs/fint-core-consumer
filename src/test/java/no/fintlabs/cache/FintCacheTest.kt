package no.fintlabs.cache

import no.fint.model.felles.kompleksedatatyper.Identifikator
import no.fint.model.resource.utdanning.elev.ElevResource
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertSame

class FintCacheTest {
    private lateinit var cache: FintCache<ElevResource>

    @BeforeEach
    fun setUp() {
        cache = FintCache<ElevResource>()
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
        val elevA_version1 = createElevResource("A")
        val elevA_version2 = createElevResource("A")
        val elevA_version3 = createElevResource("A")
        val elevA_version4 = createElevResource("A")
        cache.put(elevA_version1.systemId.identifikatorverdi, elevA_version1, 0)
        cache.put(elevA_version2.systemId.identifikatorverdi, elevA_version2, 1)
        cache.put(elevA_version3.systemId.identifikatorverdi, elevA_version3, 2)
        cache.put(elevA_version4.systemId.identifikatorverdi, elevA_version4, 3)

        assertEquals(1, cache.size)
        assertSame(elevA_version4, cache.get(elevA_version4.systemId.identifikatorverdi))
        assertSame(elevA_version4, cache.getByIdField("brukernavn", elevA_version4.brukernavn.identifikatorverdi))
        assertSame(elevA_version4, cache.getByIdField("feidenavn", elevA_version4.feidenavn.identifikatorverdi))
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

        assertSame(elevA, cache.get("A"))
        assertSame(elevA, cache.getByIdField("systemId","A"))
        assertSame(elevA, cache.getByIdField("brukernavn",elevA.brukernavn.identifikatorverdi))
        assertSame(elevA, cache.getByIdField("feidenavn",elevA.feidenavn.identifikatorverdi))

        assertSame(elevB, cache.get("B"))
        assertSame(elevB, cache.getByIdField("systemId","B"))
        assertSame(elevB, cache.getByIdField("brukernavn",elevB.brukernavn.identifikatorverdi))
        assertSame(elevB, cache.getByIdField("feidenavn",elevB.feidenavn.identifikatorverdi))

        assertSame(elevC, cache.get("C"))
        assertSame(elevC, cache.getByIdField("systemId","C"))
        assertSame(elevC, cache.getByIdField("brukernavn",elevC.brukernavn.identifikatorverdi))
        assertSame(elevC, cache.getByIdField("feidenavn",elevC.feidenavn.identifikatorverdi))

        assertSame(elevD, cache.get("D"))
        assertSame(elevD, cache.getByIdField("systemId","D"))
        assertSame(elevD, cache.getByIdField("brukernavn",elevD.brukernavn.identifikatorverdi))
        assertSame(elevD, cache.getByIdField("feidenavn",elevD.feidenavn.identifikatorverdi))
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
    fun `lastUpdated returns timestamp of last cache change`() {
        val elevA = createElevResource("A")
        val elevB = createElevResource("B")
        val elevC = createElevResource("C")
        val elevD = createElevResource("D")

        cache.put(elevA.systemId.identifikatorverdi, elevA, 10)
        assertSame(10, cache.lastUpdated)

        cache.put(elevB.systemId.identifikatorverdi, elevB, 11)
        assertSame(11, cache.lastUpdated)

        cache.put(elevC.systemId.identifikatorverdi, elevC, 12)
        assertSame(12, cache.lastUpdated)

        cache.put(elevD.systemId.identifikatorverdi, elevD, 13)
        assertSame(13, cache.lastUpdated)

        // Evict the two first resources ->
        cache.evictExpired(12)
        assertSame(13, cache.lastUpdated)
        assertEquals(2, cache.size)

        cache.remove(elevC.systemId.identifikatorverdi, 20)
        assertEquals(20, cache.lastUpdated)

        cache.remove(elevD.systemId.identifikatorverdi, 21)
        assertEquals(21, cache.lastUpdated)
        assertSame(0, cache.size)
    }

    private fun createElevResource(id: String): ElevResource {
        val elevResource = ElevResource()
        elevResource.systemId = object : Identifikator() {
            init {
                identifikatorverdi = id
            }
        }
        elevResource.brukernavn = object : Identifikator() {
            init {
                identifikatorverdi = UUID.randomUUID().toString()
            }
        }
        elevResource.feidenavn = object : Identifikator() {
            init {
                identifikatorverdi = UUID.randomUUID().toString()
            }
        }
        return elevResource
    }
}