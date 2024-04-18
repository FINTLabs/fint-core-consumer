package no.fintlabs.consumer.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import static no.fintlabs.consumer.config.Endpoints.DEFAULT_ENDPOINT;

@RestController
@RequestMapping(DEFAULT_ENDPOINT)
public class DefaultController {

    @GetMapping // Default endpoint, display all endpoints of consumer
    public void displayEndpoints() {
    }

}
