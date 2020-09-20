package com.example.rocket.producer;

import com.example.rocket.RocketApplicationTests;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import java.io.UnsupportedEncodingException;
import java.util.UUID;

@Slf4j
public class ProducerTest extends RocketApplicationTests {

    @Autowired
    private DefaultMQProducer mqProducer;

    @Value("${rocketmq.test.topic}")
    private String topic;

    @Test
    public void send() throws UnsupportedEncodingException, InterruptedException, RemotingException, MQClientException, MQBrokerException {
        // Keys 代表这条消息的业务关键词，服务器会根据keys创建哈希索引，设置后，
        // 可以在Console系统根据Topic、Keys来查询消息，由于是哈希索引，请尽可能保证key唯一，例如订单号，商品Id等。
        String keys = UUID.randomUUID().toString();
        Message message = new Message(topic, "TagA", keys,
                ("Test RocketMQ " + 9529).getBytes(RemotingHelper.DEFAULT_CHARSET));
        SendResult sendResult = mqProducer.send(message);
        log.info("发送结果：{}，消息ID：{}", sendResult.getSendStatus(), sendResult.getMsgId());
    }

}
