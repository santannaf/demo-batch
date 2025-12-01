package com.example.demo_batch;

import io.r2dbc.spi.Batch;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import org.jspecify.annotations.NonNull;
import org.springframework.r2dbc.connection.ConnectionFactoryUtils;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static java.lang.IO.println;

@Repository
public class TestOracleProvider implements TestProvider {
    private final ConnectionFactory connectionFactory;

    private static final int BATCH_SIZE = 100;

    public TestOracleProvider(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    public Mono<@NonNull Long> batchInsert(List<Measurement> batch) {
        return Mono.usingWhen(
                ConnectionFactoryUtils.getConnection(connectionFactory), // Mono<Connection>
                connection ->
                        Flux.from(createBatch(connection, batch).execute()) // Publisher<Result> -> Flux<Result>
                                .flatMap(result -> Mono.from(result.getRowsUpdated())) // cada Result -> Mono<Integer>
                                .reduce(0L, Long::sum),
                connection -> ConnectionFactoryUtils.releaseConnection(connection, connectionFactory)
        ).onErrorContinue( (error, r) -> {
            println("Erro ao inserir dados: " + error.getMessage());
            println("Object: " + r);
        });
    }

    @SuppressWarnings("SqlSourceToSinkFlow")
    private Batch createBatch(Connection connection, List<Measurement> data) {
        var batch = connection.createBatch();
        chunk(data, BATCH_SIZE).forEach(m -> batch.add(insertCRBatch(m)));
        return batch;
    }

    public static <T> List<List<T>> chunk(List<T> list, int chunkSize) {
        List<List<T>> chunks = new ArrayList<>();

        for (int i = 0; i < list.size(); i += chunkSize) {
            int end = Math.min(list.size(), i + chunkSize);
            chunks.add(list.subList(i, end));
        }

        return chunks;
    }

    public String insertCRBatch(List<Measurement> batch) {
        // Template de uma linha do SELECT
        String baseSql = """
                select
                    %s city,
                    %s temp
                from dual
                """.trim();

        // Monta o bloco de SELECT com UNION ALL
        String sqlData = batch.stream()
                .map(entity -> String.format(
                        baseSql,
                        quoted(entity.city()),
                        entity.temperature()
                ))
                .collect(Collectors.joining(" union all "));

        // Monta o INSERT final
        return """
                insert into %s.measurements (
                    city,
                    temp
                )
                WITH cr AS (
                    %s
                )
                SELECT * FROM cr
                """.formatted("test_adm", sqlData);
    }

    /**
     * Helper para "quotar" valores em SQL com escape de aspas simples.
     * Retorna "null" (sem aspas) quando o valor é nulo.
     */
    private String quoted(Object value) {
        if (value == null) {
            return "null";
        }
        String str = value.toString().replace("'", "''");
        return "'" + str + "'";
    }
}
