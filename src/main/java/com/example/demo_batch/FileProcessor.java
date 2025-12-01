package com.example.demo_batch;

import org.jspecify.annotations.NonNull;
import org.springframework.http.codec.multipart.FilePart;
import reactor.core.publisher.Mono;

public interface FileProcessor {
    Mono<@NonNull Void> processFilePartStreaming(String uploadId, FilePart filePart, int batchSize);
}
