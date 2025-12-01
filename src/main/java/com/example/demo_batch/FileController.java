package com.example.demo_batch;

import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.UUID;

@RestController
@RequestMapping(path = "/files")
public class FileController {
    private static final Logger logger = LoggerFactory.getLogger(FileController.class);

    private final FileProcessor processor;

    public FileController(FileProcessor processor) {
        this.processor = processor;
    }

    @PostMapping(path = "/upload", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    Mono<@NonNull ResponseEntity<@NonNull String>> uploadXyz(
            @RequestPart(name = "name", value = "name", required = false) String name,
            @RequestPart(name = "files", value = "files") Flux<@NonNull FilePart> files
    ) {
        logger.info("Name {}", name);
        String uploadId = UUID.randomUUID().toString();
        long startUpload = System.currentTimeMillis();

        logger.info("UPLOAD {} → Iniciado processamento do(s) arquivo(s)", uploadId);

        files.flatMap(filePart -> processor.processFilePartStreaming(uploadId, filePart, 50_000), 2)
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                        _ -> {
                            // onNext por arquivo
                        },
                        ex -> logger.error("UPLOAD {} → ERRO: {}", uploadId, ex.getMessage(), ex),
                        () -> {
                            long totalMs = System.currentTimeMillis() - startUpload;
                            logger.info(
                                    "UPLOAD {} → Finalizado processamento de TODOS os arquivos em {} ms ({} segundos)",
                                    uploadId,
                                    totalMs,
                                    totalMs / 1000.0
                            );
                        }
                );

        return Mono.just(
                ResponseEntity
                        .accepted()
                        .body("Upload " + uploadId + " recebido e será processado em background.")
        );
    }
}
