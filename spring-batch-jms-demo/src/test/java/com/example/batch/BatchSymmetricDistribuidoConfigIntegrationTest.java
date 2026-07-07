package com.example.batch;

import org.junit.jupiter.api.Test;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
// Start the embedded broker specifically for the test so it doesn't fail trying to connect to a nonexistent TCP broker
@TestPropertySource(properties = {"embedded.broker=true"})
public class BatchSymmetricDistribuidoConfigIntegrationTest {

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private Job jobSimetrico;

    // Test case 1.1: Single machine handles both master and slave roles.
    @Test
    public void testHappyPath() throws Exception {
        JobParameters jobParameters = new JobParametersBuilder()
                .addLong("time", System.currentTimeMillis())
                .toJobParameters();

        JobExecution jobExecution = jobLauncher.run(jobSimetrico, jobParameters);

        assertEquals("COMPLETED", jobExecution.getExitStatus().getExitCode());
    }
}
