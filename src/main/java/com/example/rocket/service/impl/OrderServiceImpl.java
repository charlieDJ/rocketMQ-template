package com.example.rocket.service.impl;

import com.alibaba.fastjson.JSON;
import com.example.rocket.dto.OrderDTO;
import com.example.rocket.entity.Order;
import com.example.rocket.entity.TransactionLog;
import com.example.rocket.produce.TransactionProducer;
import com.example.rocket.service.OrderService;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.namedparam.BeanPropertySqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Slf4j
@Service
public class OrderServiceImpl implements OrderService {

    @Value("${rocketmq.order.topic}")
    private String topic;

    @Autowired
    private TransactionProducer producer;
    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    //执行本地事务时调用，将订单数据和事务日志写入本地数据库
    @Transactional
    @Override
    public void createOrder(OrderDTO orderDTO, String transactionId) {

        //1.创建订单
        Order order = new Order();
        BeanUtils.copyProperties(orderDTO, order);
        BeanPropertySqlParameterSource source = new BeanPropertySqlParameterSource(orderDTO);

        String sql = "insert into transaction_order(order_no,commodity_code) values(:orderNo,:commodityCode)";
        jdbcTemplate.update(sql, source);

        //2.写入事务日志
        TransactionLog transactionLog = new TransactionLog();
        transactionLog.setId(transactionId);
        transactionLog.setBusiness("order");
        transactionLog.setForeignKey(String.valueOf(order.getId()));
        BeanPropertySqlParameterSource transSource = new BeanPropertySqlParameterSource(transactionLog);
        String transSql = "insert into transaction_log(id,business,foreign_key) values(:id,:business,:foreignKey)";
        jdbcTemplate.update(transSql, transSource);
        log.info("订单创建完成。{}", orderDTO);
    }

    //前端调用，只用于向RocketMQ发送事务消息
    @Override
    public TransactionSendResult createOrder(OrderDTO order) throws MQClientException {
        long time = Instant.now().toEpochMilli();
        order.setId(time);
        order.setOrderNo(String.valueOf(time));
        return producer.send(JSON.toJSONString(order), topic);
    }
}

