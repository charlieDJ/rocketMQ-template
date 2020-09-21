package com.example.rocket.service;

import com.example.rocket.dto.OrderDTO;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.TransactionSendResult;

public interface OrderService {
    //执行本地事务时调用，将订单数据和事务日志写入本地数据库
//    @Transactional
    void createOrder(OrderDTO orderDTO, String transactionId);

    //前端调用，只用于向RocketMQ发送事务消息
    TransactionSendResult createOrder(OrderDTO order) throws MQClientException;
}
