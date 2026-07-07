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

import jakarta.jms.ConnectionFactory;
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

    /* ==========================================================================
     * 1. INFRAESTRUTURA PARTILHADA (Canais em Memoria)
     * ========================================================================== */

    @Bean
    public DirectChannel outboundRequestsChannel() {
        return new DirectChannel();
    }

    @Bean
    public QueueChannel inboundRepliesChannel() {
        return new QueueChannel(); // Onde o Orquestrador espera e agrega as respostas
    }

    @Bean
    public DirectChannel inboundRequestsChannel() {
        return new DirectChannel(); // Onde o Trabalhador recebe a mensagem limpa
    }

    @Bean
    public DirectChannel outboundRepliesChannel() {
        return new DirectChannel(); // Onde o Trabalhador envia a resposta de volta
    }

    /* ==========================================================================
     * 2. O LADO DO ORQUESTRADOR (MANAGER)
     * Resolve o TC 3.1, 4.1 e 4.2 (Isolamento com Temporary Queues)
     * ========================================================================== */

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
                .gridSize(10)
                .outputChannel(outboundRequestsChannel()) // Envia os pedidos
                .inputChannel(inboundRepliesChannel())    // Espera respostas agregadas
                .build();
    }

    @Bean
    @ServiceActivator(inputChannel = "outboundRequestsChannel")
    public JmsOutboundGateway jmsOutboundGateway(ConnectionFactory connectionFactory) {
        JmsOutboundGateway gateway = new JmsOutboundGateway();
        gateway.setConnectionFactory(connectionFactory);
        gateway.setRequestDestinationName(REQUEST_QUEUE);

        // A MAGIA DO ISOLAMENTO:
        // Ao omiter gateway.setReplyDestinationName(), o Spring cria a Temporary Queue
        // e injeta o cabecalho JMSReplyTo. Zero sobreposicao de execucoes.
        gateway.setReplyChannel(inboundRepliesChannel());
        gateway.setReceiveTimeout(60000);
        gateway.setRequiresReply(true);
        gateway.setCorrelationKey("JMSCorrelationID");
        return gateway;
    }

    /* ==========================================================================
     * 3. O LADO DO TRABALHADOR (WORKER)
     * Resolve o TC 2.1 e 5.2 (Best Efforts 1 Phase Commit sem XA)
     * ========================================================================== */

    @Bean
    public DefaultMessageListenerContainer workerListenerContainer(
            ConnectionFactory connectionFactory,
            PlatformTransactionManager transactionManager) {

        DefaultMessageListenerContainer container = new DefaultMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.setDestinationName(REQUEST_QUEUE);

        // A MAGIA DA TOLERANCIA A FALHAS (1PC):
        // Injetamos o gestor de transacoes da Base de Dados.
        // A mensagem JMS so tem o Acknowledge efetuado SE o commit na base de dados tiver sucesso.
        container.setTransactionManager(transactionManager);
        container.setSessionTransacted(true);

        // Permite a JVM local processar varias particoes em simultaneo
        container.setConcurrentConsumers(2);
        container.setMaxConcurrentConsumers(5);
        container.setCacheLevel(DefaultMessageListenerContainer.CACHE_CONSUMER);

        return container;
    }

    @Bean
    public JmsMessageDrivenEndpoint jmsInboundGateway(DefaultMessageListenerContainer listenerContainer) {
        ChannelPublishingJmsMessageListener listener = new ChannelPublishingJmsMessageListener();
        listener.setRequestChannel(inboundRequestsChannel());
        listener.setReplyChannel(outboundRepliesChannel());
        listener.setExpectReply(true);
        listener.setCorrelationKey("JMSCorrelationID");

        // O gateway deteta o JMSReplyTo e devolve o estado para a fila temporaria do Master exato
        return new JmsMessageDrivenEndpoint(listenerContainer, listener);
    }

    @Bean
    public Step workerStep() {
        return workerFactory.get("workerStep")
                .inputChannel(inboundRequestsChannel())
                .outputChannel(outboundRepliesChannel())
                .<String, String>chunk(100)
                .reader(meuItemReader())
                .processor(meuItemProcessor())
                .writer(meuItemWriter())
                .build();
    }

    /* ==========================================================================
     * 4. LOGICA DE NEGOCIO (STUBS)
     * ========================================================================== */

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
        // Reduced to 10 for visibility in logs, even with chunk 100
        for (int i = 0; i < 10; i++) {
            items.add("Item " + i);
        }
        return new ListItemReader<>(items);
    }

    @Bean
    public ItemProcessor<String, String> meuItemProcessor() {
        return item -> {
            try {
                // Simulate some work taking 1 second per item so we can kill processes
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return item.toUpperCase();
        };
    }

    @Bean
    public ItemWriter<String> meuItemWriter() {
        return items -> {
            for (String item : items) {
                System.out.println(Thread.currentThread().getName() + " - Writing item: " + item);
            }
        };
    }
}
