package no.fintlabs.consumer.filter;

import com.fasterxml.jackson.databind.ser.FilterProvider;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import no.fint.model.resource.FintResource;
import no.fint.model.resource.FintResources;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.codec.ServerCodecConfigurer;
import org.springframework.http.converter.json.MappingJacksonValue;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.HandlerResult;
import org.springframework.web.reactive.accept.RequestedContentTypeResolver;
import org.springframework.web.reactive.result.method.annotation.ResponseBodyResultHandler;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.util.Set;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class OpaFieldAdvice extends ResponseBodyResultHandler {

    public OpaFieldAdvice(ServerCodecConfigurer codecs,
                          RequestedContentTypeResolver resolver) {
        super(codecs.getWriters(), resolver);
    }

    @Override
    public Mono<Void> handleResult(ServerWebExchange exchange, HandlerResult result) {
        Set<String> fields = Set.of("opprettet");
        Set<String> rels = Set.of();

        Object body = result.getReturnValue();
        Object filtered = applyPruning(body, fields, rels);

        return super.handleResult(
                exchange, new HandlerResult(result.getHandler(), filtered, result.getReturnTypeSource()));
    }

    private Object applyPruning(Object value, Set<String> fields, Set<String> rels) {

        if (value instanceof FintResources col) {
            col.getContent().forEach(r -> LinkPruner.prune(r, rels, fields));
        } else if (value instanceof FintResource res) {
            LinkPruner.prune(res, rels, fields);
        }

        FilterProvider fp = new SimpleFilterProvider()
                .addFilter("opa", new CaseInsensitivePropertyFilter(fields, true))
                .setFailOnUnknownId(false);

        MappingJacksonValue mjv = new MappingJacksonValue(value);
        mjv.setFilters(fp);
        return mjv;
    }
}
