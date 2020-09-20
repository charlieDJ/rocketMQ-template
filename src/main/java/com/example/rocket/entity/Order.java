package com.example.rocket.entity;

import lombok.Data;

@Data
public class Order {
    private Long id;
    private String orderNo;
    private String commodityCode;
}
