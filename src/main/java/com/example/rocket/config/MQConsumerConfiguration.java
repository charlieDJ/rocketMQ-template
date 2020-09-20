package com.example.rocket.config;

import com.example.rocket.consumer.MessageListenerHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class MQConsumerConfiguration {
    @Value("${rocketmq.namesrvAddr}")
    private String namesrvAddr;
    @Value("${rocketmq.test.consumer.groupName}")
    private String groupName;
    @Value("${rocketmq.test.consumeThreadMin}")
    private int consumeThreadMin;
    @Value("${rocketmq.test.consumeThreadMax}")
    private int consumeThreadMax;
    // 订阅指定的 topic
    @Value("${rocketmq.test.topic}")
    private String topic;

    @Autowired
    private MessageListenerHandler mqMessageListenerProcessor;

    @Bean
    public DefaultMQPushConsumer defaultMQPushConsumer() throws RuntimeException {

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(groupName);
        consumer.setNamesrvAddr(namesrvAddr);
        consumer.setConsumeThreadMin(consumeThreadMin);
        consumer.setConsumeThreadMax(consumeThreadMax);
        consumer.registerMessageListener(mqMessageListenerProcessor);

        // 设置 consumer 第一次启动是从队列头部开始消费还是队列尾部开始消费
        // 如果非第一次启动，那么按照上次消费的位置继续消费
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        // 设置消费模型，集群还是广播，默认为集群
        consumer.setMessageModel(MessageModel.CLUSTERING);

        try {
            // 设置该消费者订阅的主题和tag，如果是订阅该主题下的所有tag，使用*；
            consumer.subscribe(topic, "*");
            // 启动消费
            consumer.start();
            log.info("consumer is started. groupName:{}, topic:{}, namesrvAddr:{}", groupName, topic, namesrvAddr);

        } catch (Exception e) {
            log.error("failed to start consumer . groupName:{}, topic:{}, namesrvAddr:{}", groupName, topic, namesrvAddr, e);
            throw new RuntimeException(e);
        }
        return consumer;
    }
}
