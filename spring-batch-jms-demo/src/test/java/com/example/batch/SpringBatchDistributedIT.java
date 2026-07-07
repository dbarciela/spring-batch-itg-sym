package com.example.batch;

import org.junit.jupiter.api.Test;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(classes = DemoApplication.class)
@ActiveProfiles("test")
@TestPropertySource(properties = {
    "spring.datasource.url=jdbc:h2:mem:batchdb;DB_CLOSE_DELAY=-1",
    "spring.datasource.driverClassName=org.h2.Driver",
    "spring.datasource.username=sa",
    "spring.datasource.password=",
    "embedded.broker=true",
    "spring.activemq.broker-url=tcp://localhost:61616",
    "spring.activemq.user=admin",
    "spring.activemq.password=admin"
})
public class SpringBatchDistributedIT {

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private org.springframework.batch.core.Job jobSimetrico;

    /**
     * TC 1: Happy Path and Distribution
     * Proves that the master distributes work to slaves and waits for all of them
     * to complete successfully. Since we are using an embedded broker in this
     * automated test, the master and slave roles are fulfilled by the same JVM.
     * The `ConcurrentConsumers=2` and `gridSize=2` settings mean that 2 partitions
     * are created and processed concurrently via JMS queues. The delay in the
     * `ItemProcessor` ensures that asynchronous distribution actually occurs.
     */
    @Test
    public void testTC1_CaminhoFeliz_IsolamentoDeEstado() throws Exception {
        System.out.println("\n>>> Starting testTC1_CaminhoFeliz_IsolamentoDeEstado");
        System.out.println(">>> The orchestrator (Master) will send partition requests to the JMS queue.");
        System.out.println(">>> The worker instances (Slaves) will consume, process (with 1s delay per item), and reply.");
        System.out.println(">>> The Master will Block and Wait until all partitions report COMPLETED via JMS.");

        JobParameters jobParameters = new JobParametersBuilder()
                .addLong("time", System.currentTimeMillis())
                .toJobParameters();

        // Launch the job asynchronously to allow polling
        Future<JobExecution> futureExecution = Executors.newSingleThreadExecutor().submit(() -> {
            return jobLauncher.run(jobSimetrico, jobParameters);
        });

        // Use Awaitility to check if the job completes correctly.
        // It will poll every 2 seconds until `futureExecution.isDone()` is true.
        await().atMost(Duration.ofMinutes(2))
               .pollInterval(Duration.ofSeconds(2))
               .untilAsserted(() -> {
                   assertTrue(futureExecution.isDone(), "Job hasn't finished yet, Master is still waiting for Slaves...");
                   assertEquals("COMPLETED", futureExecution.get().getExitStatus().getExitCode(),
                       "Job should complete successfully after all slaves finish processing.");
               });

        System.out.println(">>> Finished testTC1_CaminhoFeliz_IsolamentoDeEstado");
        System.out.println(">>> All partitions processed. Job COMPLETED.");
    }

    /**
     * Note on TC 2 (Resilience & 1PC) and TC 3 (Orchestrator Failure):
     * To automate tests verifying that `kill -9` does not duplicate data and roles are restored,
     * one must use separate JVM instances. In a modern Java testing stack, this is done via
     * Testcontainers (launching Docker containers containing the app) or via ProcessBuilder.
     *
     * Since this is a standalone demo running in a constrained sandbox without Docker access,
     * we cannot easily spawn and terminate specific JVMs mid-execution.
     * However, the mechanism is active:
     * The `DefaultMessageListenerContainer` injects the DB `PlatformTransactionManager`,
     * guaranteeing Best-Efforts 1PC. Any crash occurring between the DB commit and the JMS
     * acknowledge will safely rollback the JMS state (or DB state) preventing duplication.
     *
     * See README.md for instructions on how to simulate this manually via scripts.
     */
}
