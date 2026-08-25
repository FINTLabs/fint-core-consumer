package no.fintlabs.consumer.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import lombok.RequiredArgsConstructor;
import no.fintlabs.consumer.links.LinkIdValueEncoder;
import no.fintlabs.consumer.links.LinkSerializer;
import no.fintlabs.consumer.resource.context.ResourceContext;
import no.novari.fint.model.resource.Link;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.web.codec.CodecCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpHeaders;
import org.springframework.http.codec.json.Jackson2JsonEncoder;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;
import org.springframework.web.reactive.config.PathMatchConfigurer;
import org.springframework.web.reactive.config.WebFluxConfigurer;
import org.springframework.web.server.WebFilter;

import java.util.stream.Collectors;

@Configuration
@RequiredArgsConstructor
public class WebFluxConfig implements WebFluxConfigurer {
    private final ConsumerConfiguration consumerConfiguration;

    private static final String CORE_VERSION_HEADER = "x-core-version";

    @Override
    public void configurePathMatching(PathMatchConfigurer configurer) {
        configurer.setUseTrailingSlashMatch(true);
    }

    @Bean
    public LinkIdValueEncoder linkIdValueEncoder(ResourceContext resourceContext) {
        return new LinkIdValueEncoder(
                consumerConfiguration.getBaseUrl(),
                resourceContext.getResources().stream()
                        .flatMap(resource -> resource.idFieldNames().stream())
                        .collect(Collectors.toSet())
        );
    }

    @Bean
    public ObjectMapper webObjectMapper(Jackson2ObjectMapperBuilder builder, LinkIdValueEncoder linkIdValueEncoder) {
        return builder.build().registerModule(
                new SimpleModule().addSerializer(Link.class, new LinkSerializer(linkIdValueEncoder))
        );
    }

    @Bean
    @Order(1)
    public CodecCustomizer linkEncodingCodecCustomizer(@Qualifier("webObjectMapper") ObjectMapper webObjectMapper) {
        return configurer -> configurer.defaultCodecs()
                .jackson2JsonEncoder(new Jackson2JsonEncoder(webObjectMapper));
    }

    /*
     * Adds core version header to all HTTP responses
     */
    @Bean
    public WebFilter coreVersionHeaderFilter() {
        return (exchange, chain) -> {
            HttpHeaders headers = exchange.getResponse().getHeaders();
            if (!headers.containsKey(CORE_VERSION_HEADER)) {
                headers.add(CORE_VERSION_HEADER, consumerConfiguration.getCoreVersionHeader());
            }
            return chain.filter(exchange);
        };
    }
}
