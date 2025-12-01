package com.example.demo_batch;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import reactor.core.publisher.Hooks;

@SpringBootApplication
public class DemoBatchApplication {
	static void main(String[] args) {
        Hooks.enableAutomaticContextPropagation();
		SpringApplication.run(DemoBatchApplication.class, args);
	}
}
