package no.fintlabs.consumer.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import jakarta.annotation.PostConstruct;
import no.fintlabs.consumer.filter.interfaces.OpaFilter;
import no.fintlabs.reflection.ReflectionCache;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Configuration;

import java.util.List;


@Configuration
public class JacksonOpaConfig {

    private final ReflectionCache reflectionCache;
    private final List<ObjectMapper> objectMappers;

    public JacksonOpaConfig(ReflectionCache reflectionCache,
                            ObjectMapper objectMapper,
                            @Qualifier("webObjectMapper") ObjectMapper webObjectMapper) {
        this.reflectionCache = reflectionCache;
        this.objectMappers = List.of(objectMapper, webObjectMapper);
    }

    @PostConstruct
    public void addMixIns() {
        objectMappers.forEach(mapper -> {
            reflectionCache.getAllResourceSubtypes()
                    .forEach(type -> mapper.addMixIn(type, OpaFilter.class));

            mapper.setFilterProvider(
                    new SimpleFilterProvider().setFailOnUnknownId(false)
            );
        });
    }

}
