package com.learnkafkastreams.domain;

import java.math.BigDecimal;

public record TotalRevenue(String locationId,
                           Integer runningOrderCount,
                           BigDecimal runningRevenue) {

    public TotalRevenue(){
        this("", 0, BigDecimal.ZERO);
    }

    public TotalRevenue updateRunningRevenue(String key, Order order) {
        var newOrdersCount = runningOrderCount + 1;
        var newRevenue = runningRevenue.add(order.finalAmount());
        return new TotalRevenue(key, newOrdersCount, newRevenue);
    }

}
