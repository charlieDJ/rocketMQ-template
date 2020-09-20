package com.example.rocket.service.impl;

import com.example.rocket.service.TransactionLogService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

@Transactional
@Service
public class TransactionLogServiceImpl implements TransactionLogService {
    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    @Override
    public int get(String transactionId) {
        MapSqlParameterSource source = new MapSqlParameterSource();
        source.addValue("transactionId", transactionId);
        String sql = "select id from transaction_log where transaction_id = :transactionId";
        String id = jdbcTemplate.queryForObject(sql, source, String.class);
        if (StringUtils.hasText(id)) {
            return 1;
        }
        return 0;
    }
}
