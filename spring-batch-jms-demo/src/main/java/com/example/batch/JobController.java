package com.example.batch;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class JobController {

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private Job jobSimetrico;

    @PostMapping("/run")
    public ResponseEntity<String> runJob() {
        try {
            JobParameters jobParameters = new JobParametersBuilder()
                    .addLong("time", System.currentTimeMillis())
                    .toJobParameters();

            jobLauncher.run(jobSimetrico, jobParameters);
            return ResponseEntity.ok("Job started successfully. Check logs for progress.\n");
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body("Job failed to start: " + e.getMessage() + "\n");
        }
    }
}
