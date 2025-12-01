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
import reactor.core.scheduler.Schedulers;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.IO.println;

@Component
public record FileProcessorImpl(TestProvider testProvider, ProgressSinkService progress) implements FileProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileProcessorImpl.class);

    @Override
    public Mono<@NonNull Void> processFilePartStreaming(String uploadId, FilePart filePart, int batchSize) {
        String filename = filePart.filename();
        long fileStart = System.currentTimeMillis();

        LOGGER.info("UPLOAD {} → Arquivo {} → Início do processamento", uploadId, filename);

        progress.emit(uploadId, "FILE_START:" + filename);

        return filePartToLines(uploadId, filePart)
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
                .flatMap(bucket -> {
                            return testProvider.batchInsert(bucket)
                                    .doOnNext(rows -> {
                                        LOGGER.debug(
                                                "UPLOAD {} → Arquivo {} → batch de {} registros inserido ({} linhas afetadas)",
                                                uploadId,
                                                filename,
                                                bucket.size(),
                                                rows
                                        );
                                        progress.emit(
                                                uploadId,
                                                "BATCH:" + bucket.size() + ":" + rows
                                        );
                                    })
                                    .then();     // Mono<Void> para o flatMap
                        }, 3 // batches em paralelo
                )
                .publishOn(Schedulers.boundedElastic())
                .doOnComplete(() -> {
                    long ms = System.currentTimeMillis() - fileStart;
                    LOGGER.info(
                            "UPLOAD {} → Arquivo {} → Finalizado em {} ms ({} segundos)",
                            uploadId,
                            filename,
                            ms,
                            ms / 1000.0
                    );
                    progress.emit(uploadId, "FILE_DONE:" + filename);
                })
                .then();
    }

    private Flux<@NonNull String> filePartToLines(String uploadId, FilePart filePart) {
        AtomicLong totalLines = new AtomicLong(0);
        AtomicLong processedLines = new AtomicLong(0);

        return filePart.content()
                .map(dataBuffer -> {
                    String text = decode(dataBuffer);
                    long count = text.chars().filter(c -> c == '\n').count();
                    if (count > 0) {
                        long tl = totalLines.addAndGet(count);
                        progress.emit(uploadId, "TOTAL_LINES:" + tl);
                    }

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
                .filter(s -> !s.isBlank())
                .doOnNext(_ -> {
                    long pl = processedLines.incrementAndGet();

                    long tl = totalLines.get();
                    if (tl > 0) {
                        double pct = (pl * 100.0) / tl;
                        progress.emit(uploadId, "PROCESS_PCT:" + String.format(Locale.US, "%.2f", pct));
                    }
                });
    }

//    private String dataBufferToString(DataBuffer dataBuffer) {
//        return dataBuffer.toString(StandardCharsets.UTF_8);
//    }

    private String decode(DataBuffer dataBuffer) {
        byte[] bytes = new byte[dataBuffer.readableByteCount()];
        dataBuffer.read(bytes);
        DataBufferUtils.release(dataBuffer);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
