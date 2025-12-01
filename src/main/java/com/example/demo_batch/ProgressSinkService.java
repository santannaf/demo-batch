package com.example.demo_batch;

import org.jspecify.annotations.NonNull;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class ProgressSinkService {
    private final Map<String, Sinks.Many<@NonNull String>> sinks = new ConcurrentHashMap<>();

    public Sinks.Many<@NonNull String> getSink(String uploadId) {
        return sinks.computeIfAbsent(uploadId, id ->
                Sinks.many().multicast().onBackpressureBuffer()
        );
    }

    public void emit(String uploadId, String message) {
        var sink = getSink(uploadId);
        sink.tryEmitNext(message);
    }

    public Flux<@NonNull String> stream(String uploadId) {
        return getSink(uploadId).asFlux();
    }
}
