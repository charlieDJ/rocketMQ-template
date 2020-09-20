package com.example.rocket.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class MQProducerConfiguration {

    @Value("${rocketmq.test.producer.groupName}")
    private String groupName;
    @Value("${rocketmq.namesrvAddr}")
    private String namesrvAddr;
    @Value("${rocketmq.test.topic}")
    private String topic;


    @Bean
    public DefaultMQProducer defaultMQProducer() throws RuntimeException {
        DefaultMQProducer producer = new DefaultMQProducer(this.groupName);
        producer.setNamesrvAddr(this.namesrvAddr);
        producer.setCreateTopicKey(topic);
        //如果需要同一个 jvm 中不同的 producer 往不同的 mq 集群发送消息，需要设置不同的 instanceName
        //producer.setInstanceName(instanceName);
        try {
            producer.start();
            log.info("producer is started. groupName:{}, namesrvAddr: {},topic:{}", groupName, namesrvAddr, topic);
        } catch (MQClientException e) {
            log.error("failed to start producer.", e);
            throw new RuntimeException(e);
        }
        return producer;
    }

}
