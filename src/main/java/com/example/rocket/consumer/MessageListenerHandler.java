package com.example.rocket.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
public class MessageListenerHandler implements MessageListenerConcurrently {

    @Value("${rocketmq.retryTimes}")
    private Integer retryTimes;

    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                    ConsumeConcurrentlyContext context) {
        if (CollectionUtils.isEmpty(msgs)) {
            log.info("receive blank msgs...");
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        }
        for (MessageExt msg : msgs) {
            if (msg.getReconsumeTimes() > retryTimes) {
                log.error("消息重试已达最大次数，将通知业务人员排查问题。msgId:{},key:{}", msg.getMsgId(), msg.getKeys());
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
            String content = new String(msg.getBody());
            mockConsume(content, msg.getMsgId());
        }

        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }

    private void mockConsume(String msg, String msgId) {
        log.info("receive msg: {}, msgId:{}", msg, msgId);
    }
}
