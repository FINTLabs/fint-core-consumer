package no.fintlabs.consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
@ComponentScan("no.fintlabs")
public class Application {

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

}
