package no.fintlabs.cache

import no.novari.fint.model.felles.kompleksedatatyper.Identifikator
import no.novari.fint.model.resource.utdanning.elev.ElevResource
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertSame

class FintCacheTest {
    private lateinit var cache: FintCache<ElevResource>

    @BeforeEach
    fun setUp() {
        cache = FintCache()
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
        assertSame(elevAVersion4, cache.get(elevAVersion4.systemId.identifikatorverdi))
        assertSame(elevAVersion4, cache.getByIdField("brukernavn", elevAVersion4.brukernavn.identifikatorverdi))
        assertSame(elevAVersion4, cache.getByIdField("feidenavn", elevAVersion4.feidenavn.identifikatorverdi))
    }

    @Test
    fun `put with older timestamp does not overwrite newer entry`() {
        val elevV1 = createElevResource("A")
        val elevV2 = createElevResource("A")
        cache.put(elevV1.systemId.identifikatorverdi, elevV1, 10)
        cache.put(elevV2.systemId.identifikatorverdi, elevV2, 5)

        assertSame(elevV1, cache.get("A"))
    }

    @Test
    fun `put with same timestamp overwrites existing entry`() {
        val elevV1 = createElevResource("A")
        val elevV2 = createElevResource("A")
        cache.put(elevV1.systemId.identifikatorverdi, elevV1, 10)
        cache.put(elevV2.systemId.identifikatorverdi, elevV2, 10)

        assertSame(elevV2, cache.get("A"))
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
        assertSame(elevA, cache.getByIdField("systemId", "A"))
        assertSame(elevA, cache.getByIdField("brukernavn", elevA.brukernavn.identifikatorverdi))
        assertSame(elevA, cache.getByIdField("feidenavn", elevA.feidenavn.identifikatorverdi))

        assertSame(elevB, cache.get("B"))
        assertSame(elevB, cache.getByIdField("systemId", "B"))
        assertSame(elevB, cache.getByIdField("brukernavn", elevB.brukernavn.identifikatorverdi))
        assertSame(elevB, cache.getByIdField("feidenavn", elevB.feidenavn.identifikatorverdi))

        assertSame(elevC, cache.get("C"))
        assertSame(elevC, cache.getByIdField("systemId", "C"))
        assertSame(elevC, cache.getByIdField("brukernavn", elevC.brukernavn.identifikatorverdi))
        assertSame(elevC, cache.getByIdField("feidenavn", elevC.feidenavn.identifikatorverdi))

        assertSame(elevD, cache.get("D"))
        assertSame(elevD, cache.getByIdField("systemId", "D"))
        assertSame(elevD, cache.getByIdField("brukernavn", elevD.brukernavn.identifikatorverdi))
        assertSame(elevD, cache.getByIdField("feidenavn", elevD.feidenavn.identifikatorverdi))
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
        assertSame(elev, cache.get("A"))
    }

    @Test
    fun `remove with equal timestamp does not remove entry`() {
        val elev = createElevResource("A")
        cache.put(elev.systemId.identifikatorverdi, elev, 10)
        cache.remove(elev.systemId.identifikatorverdi, 10)

        assertEquals(1, cache.size)
        assertSame(elev, cache.get("A"))
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

    @Test
    fun `evictExpired removes entries from indexes`() {
        val elevA = createElevResource("A")
        val elevB = createElevResource("B")

        cache.put(elevA.systemId.identifikatorverdi, elevA, 10)
        cache.put(elevB.systemId.identifikatorverdi, elevB, 20)

        assertSame(elevA, cache.getByIdField("brukernavn", elevA.brukernavn.identifikatorverdi))
        assertSame(elevB, cache.getByIdField("brukernavn", elevB.brukernavn.identifikatorverdi))

        cache.evictExpired(15)

        assertNull(cache.getByIdField("brukernavn", elevA.brukernavn.identifikatorverdi))
        assertSame(elevB, cache.getByIdField("brukernavn", elevB.brukernavn.identifikatorverdi))
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

    @Test
    fun `getPage reports the cache size as totalItems when no timestamp or filter is given`() {
        putElever("A", "B", "C", "D", "E")

        val page = cache.getPage(2, 2, 0, null)

        assertEquals(5, page.totalItems)
        assertEquals(listOf("C", "D"), ids(page))
    }

    @Test
    fun `getPage counts only entries updated since the timestamp as totalItems`() {
        putElever("A", "B", "C", "D", "E")

        val page = cache.getPage(2, 0, 30, null)

        assertEquals(3, page.totalItems)
        assertEquals(listOf("C", "D"), ids(page))
    }

    @Test
    fun `getPage applies offset within the entries updated since the timestamp`() {
        putElever("A", "B", "C", "D", "E")

        val page = cache.getPage(2, 2, 30, null)

        assertEquals(3, page.totalItems)
        assertEquals(listOf("E"), ids(page))
    }

    @Test
    fun `getPage reports zero totalItems when nothing is updated since the timestamp`() {
        putElever("A", "B", "C")

        val page = cache.getPage(2, 0, 100, null)

        assertEquals(0, page.totalItems)
        assertEquals(emptyList<String>(), ids(page))
    }

    @Test
    fun `getPage does not change totalItems for a filter`() {
        putElever("A", "B", "C")

        val page = cache.getPage(10, 0, 0, "systemId/identifikatorverdi eq 'B'")

        assertEquals(3, page.totalItems)
        assertEquals(listOf("B"), ids(page))
    }

    @Test
    fun `getPage counts the entries updated since the timestamp when a filter is also given`() {
        putElever("A", "B", "C", "D", "E")

        val page = cache.getPage(10, 0, 30, "systemId/identifikatorverdi eq 'D'")

        assertEquals(3, page.totalItems)
        assertEquals(listOf("D"), ids(page))
    }

    @Test
    fun `getPage returns every matching entry when size is zero`() {
        putElever("A", "B", "C", "D", "E")

        val page = cache.getPage(0, 0, 30, null)

        assertEquals(3, page.totalItems)
        assertEquals(listOf("C", "D", "E"), ids(page))
    }

    private fun putElever(vararg ids: String) {
        ids.forEachIndexed { index, id ->
            cache.put(id, createElevResource(id), (index + 1) * 10L)
        }
    }

    private fun ids(page: CachePage<ElevResource>): List<String> = page.resources.map { it.systemId.identifikatorverdi }

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
