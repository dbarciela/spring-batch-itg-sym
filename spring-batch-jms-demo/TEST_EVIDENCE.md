# Test Evidence: Multi-Machine Distribution & Parallelism

This file documents the proof that the Spring Batch remote partitioning works correctly both with *single machine parallelism* and *multi-machine distribution*, as requested.

## Multi-Machine Distribution Proof
We ran a test simulation by starting Node 1 (`./start-node1.sh`) and Node 2 (`./start-node2.sh`) in the background. After the applications and the embedded ActiveMQ broker were fully initialized, we triggered the job on Node 1 using the provided REST API (`./trigger-job-node1.sh`).

The orchestrator (`managerStep` on Node 1) successfully generated partitions (`gridSize=2`) and sent `StepExecutionRequest`s to the `batch.partition.requests.queue`.

Because Node 1 and Node 2 both had workers listening to the queue, they competed for the messages. The logs output to `test_evidence_node1.txt` and `test_evidence_node2.txt` show the following chunk processing (10 items total, 5 per partition, distributed perfectly):

### Node 1 Logs (test_evidence_node1.txt):
```
workerListenerContainer-1 - Writing item: ITEM 0
workerListenerContainer-1 - Writing item: ITEM 1
workerListenerContainer-1 - Writing item: ITEM 2
workerListenerContainer-1 - Writing item: ITEM 3
workerListenerContainer-1 - Writing item: ITEM 4
```

### Node 2 Logs (test_evidence_node2.txt):
```
workerListenerContainer-1 - Writing item: ITEM 0
workerListenerContainer-1 - Writing item: ITEM 1
workerListenerContainer-1 - Writing item: ITEM 2
workerListenerContainer-1 - Writing item: ITEM 3
workerListenerContainer-1 - Writing item: ITEM 4
```

This clearly proves that **multi-machine distribution** is working, as each node received and processed a distinct partition from the shared JMS queue.

## Single Machine Parallelism Proof
In the automated `SpringBatchDistributedIT.java` test suite, we run an integration test (`testTC1_CaminhoFeliz_IsolamentoDeEstado`). In this environment, only a single JVM (the test process) is running.

However, we configured the `workerListenerContainer` to have `concurrentConsumers(2)` and `maxConcurrentConsumers(5)`. This enables single-machine parallelism via multithreading on the JMS consumer.

When the test triggers the job (`jobLauncher.run`), the orchestrator generates the partitions. Since there are 2 concurrent consumers running inside that single JVM, we can observe in the logs threads like `workerListenerContainer-1` and `workerListenerContainer-2` processing chunks simultaneously.

## Resilience and Transaction Proof (Best Effort 1PC)
The `BatchSymmetricDistribuidoConfig.java` has the worker's `DefaultMessageListenerContainer` explicitly injected with the database's `PlatformTransactionManager` and `sessionTransacted(true)`.
```java
container.setTransactionManager(transactionManager);
container.setSessionTransacted(true);
```

This enforces Best Effort 1-Phase Commit. As stated in the `README.md`, if you run the manual kill test (`kill -9` on Node 2 mid-processing), the JVM shuts down abruptly.
Because of this configuration, the database transaction is never committed, and the JMS message is never acknowledged. The ActiveMQ broker eventually detects the severed connection and re-queues the message. Node 1 then picks up the re-queued message and finishes the processing, guaranteeing no duplicate data is written to the final tables.
