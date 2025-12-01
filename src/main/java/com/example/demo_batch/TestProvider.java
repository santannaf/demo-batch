package com.example.demo_batch;

import org.jspecify.annotations.NonNull;
import reactor.core.publisher.Mono;

import java.util.List;

public interface TestProvider {
    Mono<@NonNull Long> batchInsert(List<Measurement> batch);
}
