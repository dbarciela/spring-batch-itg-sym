package com.example.batch;

import org.apache.activemq.broker.BrokerService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "embedded.broker", havingValue = "true")
public class ActiveMQBrokerConfig {

    @Bean(initMethod = "start", destroyMethod = "stop")
    public BrokerService brokerService() throws Exception {
        BrokerService broker = new BrokerService();
        broker.setBrokerName("embeddedBroker");
        broker.addConnector("tcp://localhost:61616");
        broker.setPersistent(false); // keep it simple for tests
        broker.setUseJmx(false);
        System.out.println("Starting Embedded ActiveMQ Broker on tcp://localhost:61616...");
        return broker;
    }
}
