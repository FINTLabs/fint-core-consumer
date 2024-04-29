package no.fintlabs.consumer.resource.aspect;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.fintlabs.consumer.exception.ResourceNotFoundException;
import no.fintlabs.reflection.ReflectionService;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.stereotype.Component;

@Aspect
@Component
@Slf4j
@RequiredArgsConstructor
public class ResourceAspect {

    private final ReflectionService reflectionService;

    @Pointcut("execution(* no.fintlabs.consumer.resource.ResourceController.*(..)) && args(resource,..)")
    public void resourceMethods(String resource) {
    }

    @Before(value = "resourceMethods(resource)", argNames = "resource")
    public void beforeResourceMethod(String resource) {
        if (!reflectionService.getResources().containsKey(resource.toLowerCase())) throw new ResourceNotFoundException();
    }

}
