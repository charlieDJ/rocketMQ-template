package com.example.rocket.entity;

import lombok.Data;

@Data
public class TransactionLog {
    private String id;
    private String business;
    private String foreignKey;
}
