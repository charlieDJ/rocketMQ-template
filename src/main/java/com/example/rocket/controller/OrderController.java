package com.example.rocket.controller;

import com.example.rocket.dto.OrderDTO;
import com.example.rocket.service.OrderService;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/order/")
public class OrderController {

    @Autowired
    OrderService orderService;

    @PostMapping("create_order")
    public void createOrder(@RequestBody OrderDTO order) throws MQClientException {
        log.info("接收到订单数据：{}", order.getCommodityCode());
        TransactionSendResult result = orderService.createOrder(order);
        log.info("生成订单结果：{}", result.getLocalTransactionState());
    }
}

