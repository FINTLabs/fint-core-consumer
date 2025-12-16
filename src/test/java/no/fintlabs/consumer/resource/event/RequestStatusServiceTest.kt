package no.fintlabs.consumer.resource.event

import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import no.fint.model.resource.FintResource
import no.fint.model.resource.Link
import no.fint.model.resource.utdanning.vurdering.ElevfravarResource
import no.fintlabs.adapter.models.event.ResponseFintEvent
import no.fintlabs.adapter.operation.OperationType
import no.fintlabs.cache.CacheService
import no.fintlabs.cache.FintCache
import no.fintlabs.consumer.links.LinkService
import no.fintlabs.consumer.resource.ResourceConverter
import no.fintlabs.consumer.resource.event.RequestFailed.FailureType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.net.URI

class RequestStatusServiceTest {
    private val eventService: EventService = mockk()
    private val cacheService: CacheService = mockk()
    private val resourceConverter: ResourceConverter = mockk()
    private val linkService: LinkService = mockk()
    private val resourceCache: FintCache<FintResource> = mockk()

    private val service = RequestStatusService(eventService, cacheService, resourceConverter, linkService)

    private val resourceName = "student"
    private val corrId = "abc-123"

    @BeforeEach
    fun setup() {
        every { linkService.mapLinks(resourceName, any()) } just Runs
        every { cacheService.getCache(resourceName) } returns resourceCache
    }

    @Test
    fun `should return GONE when event does not exist`() {
        every { eventService.getResponse(corrId) } returns null
        every { eventService.requestExists(corrId) } returns false

        val result = service.getStatusResponse(resourceName, corrId)

        assertEquals(RequestGone, result)
    }

    @Test
    fun `should return ACCEPTED when event is still running`() {
        every { eventService.getResponse(corrId) } returns null
        every { eventService.requestExists(corrId) } returns true

        val result = service.getStatusResponse(resourceName, corrId)

        assertEquals(RequestAccepted, result)
    }

    @Test
    fun `should return FAILED when event is failed`() {
        val event = mockResponse(failed = true)
        every { eventService.getResponse(corrId) } returns event

        val result = service.getStatusResponse(resourceName, corrId)

        assertInstanceOf(RequestFailed::class.java, result)
        assertEquals(FailureType.ERROR, (result as RequestFailed).failureType)
    }

    @Test
    fun `should return REJECTED when event is rejected`() {
        val event = mockResponse(rejected = true)
        every { eventService.getResponse(corrId) } returns event

        val result = service.getStatusResponse(resourceName, corrId)

        assertInstanceOf(RequestFailed::class.java, result)
        assertEquals(FailureType.REJECTED, (result as RequestFailed).failureType)
    }

    @Test
    fun `should return CONFLICT when event is conflicted`() {
        val event = mockResponse(conflicted = true)
        val realResource = ElevfravarResource()

        every { eventService.getResponse(corrId) } returns event
        every { resourceConverter.convert(resourceName, any()) } returns realResource

        val result = service.getStatusResponse(resourceName, corrId)

        assertInstanceOf(RequestFailed::class.java, result)
        val failedResult = result as RequestFailed
        assertEquals(FailureType.CONFLICT, failedResult.failureType)
        assertEquals(realResource, failedResult.body)
    }

    @Test
    fun `should return DELETED when operation is DELETE`() {
        val event = mockResponse(opType = OperationType.DELETE)
        every { eventService.getResponse(corrId) } returns event

        val result = service.getStatusResponse(resourceName, corrId)

        assertEquals(ResourceDeleted, result)
    }

    @Test
    fun `should return ACCEPTED if cache is lagging (timestamp mismatch)`() {
        val handledTime = 1000L
        val event = mockResponse(opType = OperationType.CREATE, handledAt = handledTime)

        every { eventService.getResponse(corrId) } returns event
        every { eventService.requestExists(corrId) } returns true

        // CACHE SCENARIO: The cache only has data from time 900.
        // The event finished at 1000. Therefore, the cache is STALE.
        every { resourceCache.getLastDelivered("my-id") } returns 900L

        val result = service.getStatusResponse(resourceName, corrId)

        // We expect ACCEPTED because we are waiting for the cache to catch up
        assertEquals(RequestAccepted, result)
    }

    @Test
    fun `should return CREATED if cache is synced`() {
        val handledTime = 1000L
        val event = mockResponse(opType = OperationType.CREATE, handledAt = handledTime)
        val selfLink = "http://my-url.com"

        val realResource =
            ElevfravarResource().apply {
                addSelf(Link.with(selfLink))
            }

        every { eventService.getResponse(corrId) } returns event

        // CACHE SCENARIO: The cache has data from time 1000.
        // This matches the event. We are safe to return the object.
        every { resourceCache.getLastDelivered("my-id") } returns handledTime
        every { resourceCache.get("my-id") } returns realResource

        val result = service.getStatusResponse(resourceName, corrId)

        assertInstanceOf(ResourceCreated::class.java, result)
        val createdResult = result as ResourceCreated
        assertEquals(realResource, createdResult.body)
        assertEquals(URI.create(selfLink), createdResult.location)
    }

    private fun mockResponse(
        failed: Boolean = false,
        rejected: Boolean = false,
        conflicted: Boolean = false,
        opType: OperationType = OperationType.CREATE,
        handledAt: Long = 1000L,
    ): ResponseFintEvent {
        val event = mockk<ResponseFintEvent>(relaxed = true)
        every { event.isFailed } returns failed
        every { event.isRejected } returns rejected
        every { event.isConflicted } returns conflicted
        every { event.operationType } returns opType
        every { event.corrId } returns corrId
        every { event.handledAt } returns handledAt
        every { event.value.identifier } returns "my-id"
        return event
    }
}