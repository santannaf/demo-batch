package com.example.demo_batch;

import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Component
public record FileProcessorImpl(TestProvider testProvider) implements FileProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileProcessorImpl.class);

    @Override
    public Mono<@NonNull Void> processFilePartStreaming(String uploadId, FilePart filePart, int batchSize) {
        String filename = filePart.filename();
        long fileStart = System.currentTimeMillis();

        LOGGER.info("UPLOAD {} → Arquivo {} → Início do processamento", uploadId, filename);

        return filePartToLines(filePart)
                .<Measurement>handle((line, sink) -> {
                    String[] parts = line.split(";", 2);
                    if (parts.length != 2) {
                        sink.error(new IllegalArgumentException("Linha inválida: " + line));
                        return;
                    }

                    var v = 0.00;
                    try {
                        v = Double.parseDouble(parts[1]);
                    } catch (Exception error) {
                        // ignore exception
                    }

                    sink.next(new Measurement(parts[0].trim(), v));
                })
                // monta "baldes" de Xyz
                .bufferTimeout(batchSize, Duration.ofSeconds(5))
                .flatMap(bucket ->
                                testProvider.batchInsert(bucket)
                                        .doOnNext(rows -> LOGGER.debug(
                                                "UPLOAD {} → Arquivo {} → batch de {} registros inserido ({} linhas afetadas)",
                                                uploadId,
                                                filename,
                                                bucket.size(),
                                                rows
                                        ))
                                        .then(),         // Mono<Void> para o flatMap
                        4 // batches em paralelo
                )
                .doOnComplete(() -> {
                    long ms = System.currentTimeMillis() - fileStart;
                    LOGGER.info(
                            "UPLOAD {} → Arquivo {} → Finalizado em {} ms ({} segundos)",
                            uploadId,
                            filename,
                            ms,
                            ms / 1000.0
                    );
                })
                .then();
    }

    private Flux<@NonNull String> filePartToLines(FilePart filePart) {
        return filePart.content()
                .map(dataBuffer -> {
                    String text = dataBufferToString(dataBuffer);
                    DataBufferUtils.release(dataBuffer);
                    return text;
                })
                .transform(stringFlux ->
                        Flux.defer(() -> {
                            StringBuilder remainder = new StringBuilder();

                            return stringFlux
                                    .flatMapIterable(chunk -> {
                                        remainder.append(chunk);
                                        String full = remainder.toString();

                                        List<String> lines = Arrays.asList(full.split("\n"));

                                        if (full.endsWith("\n")) {
                                            // todas as linhas estão completas
                                            remainder.setLength(0);
                                            return lines;
                                        } else {
                                            remainder.setLength(0);

                                            if (lines.size() > 1) {
                                                // últimas linhas completas + 1 parcial
                                                List<String> complete = lines.subList(0, lines.size() - 1);
                                                String partial = lines.getLast();
                                                remainder.append(partial);
                                                return complete;
                                            } else {
                                                // só temos uma linha e ela é parcial
                                                return Collections.emptyList();
                                            }
                                        }
                                    })
                                    .concatWith(
                                            Mono.defer(() -> {
                                                if (!remainder.isEmpty()) {
                                                    return Mono.just(remainder.toString());
                                                } else {
                                                    return Mono.empty();
                                                }
                                            })
                                    );
                        })
                )
                .map(s -> {
                    // equivalente ao trimEnd('\r')
                    if (s.endsWith("\r")) {
                        return s.substring(0, s.length() - 1);
                    }
                    return s;
                })
                .filter(s -> !s.isBlank());
    }

    private String dataBufferToString(DataBuffer dataBuffer) {
        return dataBuffer.toString(StandardCharsets.UTF_8);
    }
}
