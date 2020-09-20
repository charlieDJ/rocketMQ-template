package com.example.rocket.config;

import com.example.rocket.consumer.OrderConsumerListener;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class OrderConsumerConfig {

    @Value("${rocketmq.namesrvAddr}")
    private String namesrvAddr;
    // 订阅指定的 topic
    @Value("${rocketmq.order.topic}")
    private String topic;

    @Autowired
    private OrderConsumerListener orderConsumerListener;

    @Bean("orderConsumer")
    public DefaultMQPushConsumer defaultMQPushConsumer() throws RuntimeException {
        String groupName = "order-consumer-group";
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(groupName);
        consumer.setNamesrvAddr(namesrvAddr);
        consumer.setConsumeThreadMin(1);
        consumer.setConsumeThreadMax(1);
        consumer.registerMessageListener(orderConsumerListener);

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
