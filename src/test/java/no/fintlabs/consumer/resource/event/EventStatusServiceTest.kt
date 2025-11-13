package no.fintlabs.consumer.resource.event

import io.mockk.mockk
import no.fintlabs.consumer.config.ConsumerConfiguration
import no.fintlabs.consumer.links.LinkService
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test


class EventStatusServiceTest {
    private lateinit var configuration: ConsumerConfiguration
    private lateinit var eventService: EventService
    private lateinit var linkService: LinkService

    @BeforeEach
    fun setUp() {
        configuration = mockk(relaxed = true)
        eventService = mockk(relaxed = true)
        linkService = mockk(relaxed = true)
    }

    @Test
    fun `handles successful event`() {
    }


    @Test
    fun 'handles unfinished event'() {

    }

    @Test
    fun 'handles failed event'() {

    }

    @Test
    fun 'handles rejected event'() {

    }

    @Test
    fun 'handles conflicted event'() {

    }

    @Test
    fun 'creates correct status href'() {

    }

    @Test
    fun 'creates correct uri'() {

    }


    private fun createEvent() {

    }
}
