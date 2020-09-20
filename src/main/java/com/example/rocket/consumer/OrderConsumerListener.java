package com.example.rocket.consumer;

import com.alibaba.fastjson.JSONObject;
import com.example.rocket.dto.OrderDTO;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
public class OrderConsumerListener implements MessageListenerConcurrently {

    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext context) {
        log.info("消费者线程监听到消息。");
        try {
            for (MessageExt message : list) {
                log.info("开始处理订单数据，准备增加积分....");
                OrderDTO order = JSONObject.parseObject(message.getBody(), OrderDTO.class);
                log.info("数据内容：{}", order.toString());
//                pointsService.increasePoints(order);
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        } catch (Exception e) {
            log.error("处理消费者数据发生异常。", e);
            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
        }
    }
}
