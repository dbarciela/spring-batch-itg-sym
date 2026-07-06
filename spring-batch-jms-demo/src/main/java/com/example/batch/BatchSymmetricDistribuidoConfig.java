package com.example.batch;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.integration.config.annotation.EnableBatchIntegration;
import org.springframework.batch.integration.partition.RemotePartitioningManagerStepBuilderFactory;
import org.springframework.batch.integration.partition.RemotePartitioningWorkerStepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.jms.ChannelPublishingJmsMessageListener;
import org.springframework.integration.jms.JmsMessageDrivenEndpoint;
import org.springframework.integration.jms.JmsOutboundGateway;
import org.springframework.jms.listener.DefaultMessageListenerContainer;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.integration.annotation.ServiceActivator;

import javax.jms.ConnectionFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Configuration
@EnableBatchProcessing
@EnableBatchIntegration
public class BatchSymmetricDistribuidoConfig {

    private static final String REQUEST_QUEUE = "batch.partition.requests.queue";

    private final JobBuilderFactory jobBuilderFactory;
    private final RemotePartitioningManagerStepBuilderFactory managerFactory;
    private final RemotePartitioningWorkerStepBuilderFactory workerFactory;

    public BatchSymmetricDistribuidoConfig(
            JobBuilderFactory jobBuilderFactory,
            RemotePartitioningManagerStepBuilderFactory managerFactory,
            RemotePartitioningWorkerStepBuilderFactory workerFactory) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.managerFactory = managerFactory;
        this.workerFactory = workerFactory;
    }

    @Bean
    public DirectChannel outboundRequestsChannel() {
        return new DirectChannel();
    }

    @Bean
    public QueueChannel inboundRepliesChannel() {
        return new QueueChannel();
    }

    @Bean
    public DirectChannel inboundRequestsChannel() {
        return new DirectChannel();
    }

    @Bean
    public DirectChannel outboundRepliesChannel() {
        return new DirectChannel();
    }

    @Bean
    public Job jobSimetrico(Step managerStep) {
        return jobBuilderFactory.get("meuJobResiliente")
                .start(managerStep)
                .build();
    }

    @Bean
    public Step managerStep(Partitioner meuPartitioner) {
        return managerFactory.get("managerStep")
                .partitioner("workerStep", meuPartitioner)
                .gridSize(1) // Keep grid size 1 to prevent H2 locking issues for the test
                .outputChannel(outboundRequestsChannel())
                .inputChannel(inboundRepliesChannel())
                .pollInterval(10) // Small poll interval for quick test
                .build();
    }

    @Bean
    @ServiceActivator(inputChannel = "outboundRequestsChannel")
    public JmsOutboundGateway jmsOutboundGateway(ConnectionFactory connectionFactory) {
        JmsOutboundGateway gateway = new JmsOutboundGateway();
        gateway.setConnectionFactory(connectionFactory);
        gateway.setRequestDestinationName(REQUEST_QUEUE);
        gateway.setReplyChannel(inboundRepliesChannel());
        gateway.setReceiveTimeout(5000);
        gateway.setRequiresReply(true);
        // Important: Extract the correlation ID
        gateway.setCorrelationKey("JMSCorrelationID");
        return gateway;
    }

    @Bean
    public DefaultMessageListenerContainer workerListenerContainer(
            ConnectionFactory connectionFactory,
            PlatformTransactionManager transactionManager) {

        DefaultMessageListenerContainer container = new DefaultMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.setDestinationName(REQUEST_QUEUE);

        // Disable session transacted for the H2 test but include it in comments for production
        // container.setTransactionManager(transactionManager);
        // container.setSessionTransacted(true);

        container.setConcurrentConsumers(1);

        return container;
    }

    @Bean
    public JmsMessageDrivenEndpoint jmsInboundGateway(DefaultMessageListenerContainer listenerContainer) {
        ChannelPublishingJmsMessageListener listener = new ChannelPublishingJmsMessageListener();
        listener.setRequestChannel(inboundRequestsChannel());
        listener.setReplyChannel(outboundRepliesChannel());
        listener.setExpectReply(true);
        // Ensure correlation id is copied
        listener.setCorrelationKey("JMSCorrelationID");

        return new JmsMessageDrivenEndpoint(listenerContainer, listener);
    }

    @Bean
    public Step workerStep() {
        return workerFactory.get("workerStep")
                .inputChannel(inboundRequestsChannel())
                .outputChannel(outboundRepliesChannel())
                .<String, String>chunk(5)
                .reader(meuItemReader())
                .processor(meuItemProcessor())
                .writer(meuItemWriter())
                .build();
    }

    @Bean
    public Partitioner meuPartitioner() {
        return gridSize -> {
            Map<String, ExecutionContext> map = new HashMap<>();
            for (int i = 0; i < gridSize; i++) {
                ExecutionContext context = new ExecutionContext();
                context.putInt("partition", i);
                map.put("partition" + i, context);
            }
            return map;
        };
    }

    @Bean
    public ItemReader<String> meuItemReader() {
        List<String> items = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            items.add("Item " + i);
        }
        return new ListItemReader<>(items);
    }

    @Bean
    public ItemProcessor<String, String> meuItemProcessor() {
        return item -> item.toUpperCase();
    }

    @Bean
    public ItemWriter<String> meuItemWriter() {
        return items -> {
            for (String item : items) {
                System.out.println("Writing item: " + item);
            }
        };
    }
}
