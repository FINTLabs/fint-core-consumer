package no.fintlabs.consumer.resource.aspect;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.fintlabs.consumer.exception.ResourceNotWriteableException;
import no.fintlabs.reflection.ReflectionService;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

@Aspect
@Component
@Slf4j
@RequiredArgsConstructor
@Order(2)
public class WriteableAspect {

    private final ReflectionService reflectionService;

    @Pointcut("execution(* no.fintlabs.consumer.resource.ResourceController.*(..)) && args(resource, ..) && @annotation(WriteableResource)")
    public void resourceMethods(String resource) {
    }

    @Before(value = "resourceMethods(resource)", argNames = "resource")
    public void checkIdField(String resource) {
        if (!reflectionService.getResources().get(resource).isWriteable())
            throw new ResourceNotWriteableException(resource);
    }

}
