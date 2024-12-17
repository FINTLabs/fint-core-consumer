package no.fintlabs.consumer.offset;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/offset")
@RequiredArgsConstructor
public class OffsetController {

    private final OffsetService offsetService;

    @GetMapping
    public Map<String, Long> getOffsets() {
        return offsetService.getOffsetMap();
    }

}
