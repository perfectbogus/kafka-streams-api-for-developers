/*
 * ====================================================================================
 *
 * Copyright (c) 2005, 2024 Oracle Ⓡ and/or its affiliates. All rights reserved.
 *
 * ====================================================================================
 */

package com.learnkafkastreams.controller;

import com.learnkafkastreams.domain.AllOrdersCountPerStoreDTO;
import com.learnkafkastreams.service.OrderService;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/v1/orders")
public class OrdersController {

  private final OrderService orderService;

  public OrdersController(OrderService orderService) {
    this.orderService = orderService;
  }

  @GetMapping("/count/{order_type}")
  public ResponseEntity<?> ordersCount(
      @PathVariable("order_type") String orderType,
      @RequestParam(value = "location_id", required = false) String locationId) {

    if (StringUtils.hasLength(locationId)) {
      return ResponseEntity.ok(orderService.getOrdersCountByLocationId(orderType, locationId));
    }

    return ResponseEntity.ok(orderService.getOrdersCount(orderType));
  }

  @GetMapping("/count")
  public List<AllOrdersCountPerStoreDTO> allOrdersCount() {
    return orderService.getAllOrdersCount();
  }

  @GetMapping("/revenue/{order_type}")
  public ResponseEntity<?> revenueByOrderType(
      @PathVariable("order_type") String orderType,
      @RequestParam(value = "location_id", required = false) String locationId
      ) {

    if (StringUtils.hasLength(locationId)) {
      return ResponseEntity.ok(orderService.getRevenueByLocationId(orderType, locationId));
    }

    return ResponseEntity.ok(orderService.revenueByOrderType(orderType));
  }

}
