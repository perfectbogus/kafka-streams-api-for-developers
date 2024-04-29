/*
 * ====================================================================================
 *
 * Copyright (c) 2005, 2024 Oracle Ⓡ and/or its affiliates. All rights reserved.
 *
 * ====================================================================================
 */

package com.learnkafkastreams.service;

import com.learnkafkastreams.domain.OrderRevenueDTO;
import com.learnkafkastreams.domain.OrderType;
import com.learnkafkastreams.domain.OrdersCountPerStoreByWindowsDTO;
import com.learnkafkastreams.domain.OrdersRevenuePerStoreByWindowsDTO;
import com.learnkafkastreams.domain.TotalRevenue;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.learnkafkastreams.service.OrderService.mapOrderType;
import static com.learnkafkastreams.topology.OrdersTopology.GENERAL_ORDERS;
import static com.learnkafkastreams.topology.OrdersTopology.GENERAL_ORDERS_COUNT;
import static com.learnkafkastreams.topology.OrdersTopology.GENERAL_ORDERS_COUNT_WINDOWS;
import static com.learnkafkastreams.topology.OrdersTopology.GENERAL_ORDERS_REVENUE_WINDOWS;
import static com.learnkafkastreams.topology.OrdersTopology.RESTAURANT_ORDERS;
import static com.learnkafkastreams.topology.OrdersTopology.RESTAURANT_ORDERS_COUNT;
import static com.learnkafkastreams.topology.OrdersTopology.RESTAURANT_ORDERS_COUNT_WINDOWS;
import static com.learnkafkastreams.topology.OrdersTopology.RESTAURANT_ORDERS_REVENUE_WINDOWS;

@Slf4j
@Service
public class OrdersWindowService {

  private final OrderStoreService orderStoreService;

  public OrdersWindowService(OrderStoreService orderStoreService) {
    this.orderStoreService = orderStoreService;
  }

  public List<OrdersCountPerStoreByWindowsDTO> getOrdersCountWindowsByType(String orderType) {

    ReadOnlyWindowStore<String, Long> countWindowsStore = getCountWindowsStore(orderType);
    OrderType orderTypeEnum = mapOrderType(orderType);
    KeyValueIterator<Windowed<String>, Long> countWindowsIterator = countWindowsStore.all();

    return mapToOrdersCountPerStoreByWindowsDTO(orderTypeEnum, countWindowsIterator);
  }

  private static List<OrdersCountPerStoreByWindowsDTO> mapToOrdersCountPerStoreByWindowsDTO(OrderType orderTypeEnum, KeyValueIterator<Windowed<String>, Long> countWindowsIterator) {
    Spliterator<KeyValue<Windowed<String>, Long>> spliterator =
        Spliterators.spliteratorUnknownSize(countWindowsIterator, 0);

    return StreamSupport.stream(spliterator, false).map(keyValue -> new OrdersCountPerStoreByWindowsDTO(keyValue.key.key(), keyValue.value, orderTypeEnum, LocalDateTime.ofInstant(keyValue.key.window().startTime(), ZoneId.of("GMT")), LocalDateTime.ofInstant(keyValue.key.window().endTime(), ZoneId.of("GMT")))).collect(Collectors.toList());
  }

  private ReadOnlyWindowStore<String, Long> getCountWindowsStore(String orderType) {
    return switch (orderType) {
      case GENERAL_ORDERS -> orderStoreService.ordersWindowsCountStore(GENERAL_ORDERS_COUNT_WINDOWS);
      case RESTAURANT_ORDERS -> orderStoreService.ordersWindowsCountStore(RESTAURANT_ORDERS_COUNT_WINDOWS);
      default -> throw new IllegalStateException("Not a valid option: " + orderType);
    };
  }

  public List<OrdersCountPerStoreByWindowsDTO> getAllOrdersCountByWindows() {
    List<OrdersCountPerStoreByWindowsDTO> generalOrdersCountWindowsByType = getOrdersCountWindowsByType(GENERAL_ORDERS);
    List<OrdersCountPerStoreByWindowsDTO> restaurantOrdersCountWindowsByType = getOrdersCountWindowsByType(RESTAURANT_ORDERS);

    return Stream.of(generalOrdersCountWindowsByType, restaurantOrdersCountWindowsByType)
        .flatMap(Collection::stream)
        .toList();
  }

  public List<OrdersCountPerStoreByWindowsDTO> getAllOrdersCountByWindows(LocalDateTime fromTime, LocalDateTime toTime) {
    Instant fromTimeInstant = fromTime.toInstant(ZoneOffset.UTC);
    Instant toTimeInstant = toTime.toInstant(ZoneOffset.UTC);


    KeyValueIterator<Windowed<String>, Long> generalOrdersCountByWindows =
        getCountWindowsStore(GENERAL_ORDERS)
//            .fetchAll(fromTimeInstant, toTimeInstant);
            .backwardFetchAll(fromTimeInstant, toTimeInstant);

    List<OrdersCountPerStoreByWindowsDTO> generalOrdersCountByWindowsDTO =
        mapToOrdersCountPerStoreByWindowsDTO(OrderType.GENERAL, generalOrdersCountByWindows);

    KeyValueIterator<Windowed<String>, Long> restaurantOrdersCountByWindows =
        getCountWindowsStore(RESTAURANT_ORDERS)
//            .fetchAll(fromTimeInstant, toTimeInstant);
            .backwardFetchAll(fromTimeInstant, toTimeInstant);

    List<OrdersCountPerStoreByWindowsDTO> restaurantOrdersCountByWindowsDTO =
        mapToOrdersCountPerStoreByWindowsDTO(OrderType.RESTAURANT, restaurantOrdersCountByWindows);

    return Stream.of(generalOrdersCountByWindowsDTO, restaurantOrdersCountByWindowsDTO)
        .flatMap(Collection::stream)
        .toList();

  }

  public List<OrdersRevenuePerStoreByWindowsDTO> gerOrdersRevenueWindowsByType(String orderType) {
    ReadOnlyWindowStore<String, TotalRevenue> revenueWindowsStore =
        getRevenueWindowsStore(orderType);
    OrderType orderTypeEnum = mapOrderType(orderType);
    KeyValueIterator<Windowed<String>, TotalRevenue> revenueWindowsIterator =
        revenueWindowsStore.all();

    Spliterator<KeyValue<Windowed<String>, TotalRevenue>> spliterator =
        Spliterators.spliteratorUnknownSize(revenueWindowsIterator, 0);

    return StreamSupport.stream(spliterator, false)
        .map(keyValue ->
            new OrdersRevenuePerStoreByWindowsDTO(
                keyValue.key.key(),
                keyValue.value,
                orderTypeEnum,
                LocalDateTime.ofInstant(keyValue.key.window().startTime(), ZoneId.of("GMT")),
                LocalDateTime.ofInstant(keyValue.key.window().endTime(), ZoneId.of("GMT"))))
        .collect(Collectors.toList());
  }

  private ReadOnlyWindowStore<String, TotalRevenue> getRevenueWindowsStore(String orderType) {
    return switch (orderType) {
      case GENERAL_ORDERS -> orderStoreService.ordersWindowsRevenueStore(GENERAL_ORDERS_REVENUE_WINDOWS);
      case RESTAURANT_ORDERS -> orderStoreService.ordersWindowsRevenueStore(RESTAURANT_ORDERS_REVENUE_WINDOWS);
      default -> throw new IllegalStateException("Not a valid option: " + orderType);
    };
  }
}
