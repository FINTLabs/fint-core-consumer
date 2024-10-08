package no.fintlabs.consumer.resource.aspect;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.fintlabs.consumer.exception.IdentificatorNotFoundException;
import no.fintlabs.reflection.ResourceContext;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

@Aspect
@Component
@Slf4j
@RequiredArgsConstructor
@Order(1)
public class IdentifierAspect {

    private final ResourceContext resourceContext;

    @Pointcut("execution(* no.fintlabs.consumer.resource.ResourceController.*(..)) && args(resource, idField, ..) && @annotation(org.springframework.web.bind.annotation.PathVariable)")
    public void resourceMethods(String resource, String idField) {
    }

    @Before(value = "resourceMethods(resource,idField)", argNames = "resource,idField")
    public void checkIdField(String resource, String idField) {
        log.info("{} - {}", resource, idField);
        if (!resourceContext.resourceHasIdField(resource, idField))
            throw new IdentificatorNotFoundException();
    }

}
