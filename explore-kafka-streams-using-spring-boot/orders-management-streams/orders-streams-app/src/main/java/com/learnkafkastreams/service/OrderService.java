package com.learnkafkastreams.service;

import com.learnkafkastreams.domain.AllOrdersCountPerStoreDTO;
import com.learnkafkastreams.domain.OrderCountPerStoreDTO;
import com.learnkafkastreams.domain.OrderRevenueDTO;
import com.learnkafkastreams.domain.OrderType;
import com.learnkafkastreams.domain.TotalRevenue;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.learnkafkastreams.topology.OrdersTopology.GENERAL_ORDERS;
import static com.learnkafkastreams.topology.OrdersTopology.GENERAL_ORDERS_COUNT;
import static com.learnkafkastreams.topology.OrdersTopology.GENERAL_ORDERS_REVENUE;
import static com.learnkafkastreams.topology.OrdersTopology.RESTAURANT_ORDERS;
import static com.learnkafkastreams.topology.OrdersTopology.RESTAURANT_ORDERS_COUNT;
import static com.learnkafkastreams.topology.OrdersTopology.RESTAURANT_ORDERS_REVENUE;

@Service
@Slf4j
public class OrderService {

  private OrderStoreService orderStoreService;

  public OrderService(OrderStoreService orderStoreService) {
    this.orderStoreService = orderStoreService;
  }

  public List<OrderCountPerStoreDTO> getOrdersCount(String orderType) {
    ReadOnlyKeyValueStore<String, Long> ordersCountStore = getOrderStore(orderType);
    KeyValueIterator<String, Long> orders = ordersCountStore.all();

    Spliterator<KeyValue<String, Long>> spliterator = Spliterators.spliteratorUnknownSize(orders, 0);

    return StreamSupport.stream(spliterator, false)
        .map(keyValue -> new OrderCountPerStoreDTO((keyValue.key), keyValue.value))
        .collect(Collectors.toList());

  }

  private ReadOnlyKeyValueStore<String, Long> getOrderStore(String orderType) {
    return switch (orderType) {
      case GENERAL_ORDERS -> orderStoreService.ordersCountStore(GENERAL_ORDERS_COUNT);
      case RESTAURANT_ORDERS -> orderStoreService.ordersCountStore(RESTAURANT_ORDERS_COUNT);
      default -> throw new IllegalStateException("Not a valid option: " + orderType);
    };
  }

  public OrderCountPerStoreDTO getOrdersCountByLocationId(String orderType, String locationId) {
    ReadOnlyKeyValueStore<String, Long> ordersCountStore = getOrderStore(orderType);

    Long orderCount = ordersCountStore.get(locationId);

    if (orderCount != null) {
      return new OrderCountPerStoreDTO(locationId, orderCount);
    }

    return null;
  }

  public OrderRevenueDTO getRevenueByLocationId(String orderType, String locationId) {
    ReadOnlyKeyValueStore<String, TotalRevenue> revenueStoreByType = getRevenueStore(orderType);

    TotalRevenue revenue = revenueStoreByType.get(locationId);

    if (revenue != null) {
      return new OrderRevenueDTO(locationId, mapOrderType(orderType), revenue);
    }

    return null;
  }

  public List<AllOrdersCountPerStoreDTO> getAllOrdersCount() {

    BiFunction<OrderCountPerStoreDTO, OrderType, AllOrdersCountPerStoreDTO>
        mapper = (orderCountPerStoreDTO, orderType) -> new AllOrdersCountPerStoreDTO(
            orderCountPerStoreDTO.locationId(), orderCountPerStoreDTO.orderCount(), orderType);

    List<AllOrdersCountPerStoreDTO> generalOrdersCount = getOrdersCount(GENERAL_ORDERS)
        .stream()
        .map(orderCountPerStoreDTO -> mapper.apply(orderCountPerStoreDTO, OrderType.GENERAL))
        .toList();

    List<AllOrdersCountPerStoreDTO> restaurantOrdersCount = getOrdersCount(RESTAURANT_ORDERS)
        .stream()
        .map(orderCountPerStoreDTO -> mapper.apply(orderCountPerStoreDTO, OrderType.RESTAURANT))
        .toList();

    return Stream.of(generalOrdersCount, restaurantOrdersCount)
        .flatMap(Collection::stream)
        .collect(Collectors.toList());

  }

  public List<OrderRevenueDTO> revenueByOrderType(String orderType) {
    ReadOnlyKeyValueStore<String, TotalRevenue> revenueStoreByType = getRevenueStore(orderType);
    KeyValueIterator<String, TotalRevenue> revenueIterator = revenueStoreByType.all();
    Spliterator<KeyValue<String, TotalRevenue>> spliterator = Spliterators.spliteratorUnknownSize(revenueIterator, 0);

    return StreamSupport.stream(spliterator, false)
        .map(keyValue -> new OrderRevenueDTO( keyValue.key, mapOrderType(orderType), keyValue.value))
        .collect(Collectors.toList());
  }

  public static OrderType mapOrderType(String orderType) {
    return switch (orderType) {
      case GENERAL_ORDERS -> OrderType.GENERAL;
      case RESTAURANT_ORDERS -> OrderType.RESTAURANT;
      default -> throw new IllegalStateException("Not a valid option: " + orderType);
    };
  }

  private ReadOnlyKeyValueStore<String, TotalRevenue> getRevenueStore(String orderType) {
    return switch (orderType) {
      case GENERAL_ORDERS -> orderStoreService.ordersRevenueStore(GENERAL_ORDERS_REVENUE);
      case RESTAURANT_ORDERS -> orderStoreService.ordersRevenueStore(RESTAURANT_ORDERS_REVENUE);
      default -> throw new IllegalStateException("Not a valid option: " + orderType);
    };
  }
}
