package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.*;
import com.learnkafkastreams.sedes.SerdesFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;

@Slf4j
public class OrdersTopology {
    public static final String ORDERS = "orders";
    public static final String STORES = "stores";

    public static final String RESTAURANT_ORDERS = "restaurant_orders";
    public static final String GENERAL_ORDERS = "general_orders";

    public static final String GENERAL_ORDERS_COUNT = "general_orders_count";
    public static final String RESTAURANT_ORDERS_COUNT = "restaurant_orders_count";

    private static final String GENERAL_ORDERS_REVENUE = "general_orders_revenue";
    private static final String RESTAURANT_ORDERS_REVENUE = "restaurant_orders_revenue";

    private static final String GENERAL_ORDERS_COUNT_WINDOWS = "general_orders_count_window";
    private static final String RESTAURANT_ORDERS_COUNT_WINDOWS = "restaurant_orders_count_window";

    private static final String GENERAL_ORDERS_REVENUE_WINDOWS = "general_orders_revenue_window";
    private static final String RESTAURANT_ORDERS_REVENUE_WINDOWS = "restaurant_orders_revenue_window";

    public static Topology buildTopology(){
        Predicate<String, Order> generalPredicate = (key, order) -> order.orderType().equals(OrderType.GENERAL);
        Predicate<String, Order> restaurantPredicate = (key, order) -> order.orderType().equals(OrderType.RESTAURANT);
        ValueMapper<Order, Revenue> revenueMapper = order -> new Revenue(order.locationId(), order.finalAmount());
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        var ordersStream = streamsBuilder.stream(ORDERS,
                    Consumed.with(Serdes.String(), SerdesFactory.orderSerdes())
                )
                .selectKey((key, value) -> value.locationId());

        ordersStream.print(Printed.<String, Order >toSysOut().withLabel("orders"));

        //KStream-KTable
        var storesTable = streamsBuilder
                .table(
                        STORES,
                        Consumed.with(Serdes.String(), SerdesFactory.storeSerdes())
                        );

        ordersStream
                .split(Named.as("General-restaurant-stream"))
                .branch(generalPredicate,
                        Branched.withConsumer(generalOrderStream -> {
                            generalOrderStream
                                    .print(Printed.<String, Order>toSysOut().withLabel("generalStream"));

//                            generalOrderStream
//                                    .mapValues((readOnlyKey, value) -> revenueMapper.apply(value))
//                                    .to(GENERAL_ORDERS,
//                                            Produced.with(Serdes.String(), SerdesFactory.revenueSerde()));

//                            aggregateOrdersByCount(generalOrderStream, GENERAL_ORDERS_COUNT, storesTable);
                            aggregateOrdersCountByTimeWindows(generalOrderStream, GENERAL_ORDERS_COUNT_WINDOWS, storesTable);
                            //aggregateOrdersByRevenue(generalOrderStream, GENERAL_ORDERS_REVENUE, storesTable);
                            aggregateOrdersRevenueByTimeWindows(generalOrderStream, GENERAL_ORDERS_REVENUE_WINDOWS, storesTable);

                        })
                )
                .branch(restaurantPredicate,
                        Branched.withConsumer(restaurantOrderStream -> {
                            restaurantOrderStream
                                    .print(Printed.<String, Order>toSysOut().withLabel("restaurantStream"));

//                            restaurantOrderStream
//                                    .mapValues((readOnlyKey, value) -> revenueMapper.apply(value))
//                                    .to(RESTAURANT_ORDERS,
//                                            Produced.with(Serdes.String(), SerdesFactory.revenueSerde()));
//                            aggregateOrdersByCount(restaurantOrderStream, RESTAURANT_ORDERS_COUNT, storesTable);
                            aggregateOrdersCountByTimeWindows(restaurantOrderStream, RESTAURANT_ORDERS_COUNT_WINDOWS, storesTable);
//                            aggregateOrdersByRevenue(restaurantOrderStream, RESTAURANT_ORDERS_REVENUE, storesTable);
                        })
                );

        return streamsBuilder.build();
    }

    private static void aggregateOrdersRevenueByTimeWindows(KStream<String, Order> generalOrderStream, String storedName, KTable<String, Store> storesTable) {
        //Initializer
        Initializer<TotalRevenue> totalRevenueInitializer = TotalRevenue::new;
        //aggregator
        Aggregator<String, Order, TotalRevenue> aggregator = (key, value, aggregate) ->
                aggregate.updateRunningRevenue(key, value);

        var revenueKTable = generalOrderStream
                .map((key, value) -> KeyValue.pair(value.locationId(), value))
                .groupByKey(Grouped.with(Serdes.String(), SerdesFactory.orderSerdes()))
                .aggregate(
                        totalRevenueInitializer,
                        aggregator,
                        Materialized.<String, TotalRevenue, KeyValueStore<Bytes, byte[]>>as(storedName)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SerdesFactory.totalRevenueSerdes())

                );

        revenueKTable
                .toStream()
                .print(Printed.<>toSysOut().withLabel(storedName));

        //KTable-KTable Join
//        ValueJoiner<TotalRevenue, Store, TotalRevenueWithAddress> valueJoiner = TotalRevenueWithAddress::new;
//
//        var revenueWithStoreTable = revenueKTable
//                .join(
//                        storesTable,
//                        valueJoiner
//                );
//
//
//        revenueWithStoreTable
//                .toStream()
//                .print(Printed.<String, TotalRevenueWithAddress>toSysOut().withLabel(storedName + "-bystore"));
    }

    private static void aggregateOrdersCountByTimeWindows(KStream<String, Order> generalOrderStream, String storedName, KTable<String, Store> storesTable) {

        Duration windowSize = Duration.ofSeconds(15);
        TimeWindows timeWindows = TimeWindows.ofSizeWithNoGrace(windowSize);


        var ordersCountPerStore = generalOrderStream
                .map((key, value) -> KeyValue.pair(value.locationId(), value))
                .groupByKey(Grouped.with(Serdes.String(), SerdesFactory.orderSerdes()))
                .windowedBy(timeWindows)
                .count(Named.as(storedName), Materialized.as(storedName))
                .suppress(
                        Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded().shutDownWhenFull())
                );

        ordersCountPerStore
                .toStream()
                .peek((key, value) -> {
                    log.info("StoreName: {}, key: {}, value: {}", storedName, key, value);
                    printLocalDateTimes(key, value);

                })
                .print(Printed.<Windowed<String>, Long>toSysOut().withLabel(storedName));

        ValueJoiner<Long, Store, TotalCountWithAddress> valueJoiner = TotalCountWithAddress::new;

//        var revenueWithStoreTable = ordersCountPerStore
//                .join(
//                        storeKTable,
//                        valueJoiner
//                );
//
//        revenueWithStoreTable
//                .toStream()
//                .print(Printed.<String, TotalCountWithAddress>toSysOut().withLabel(storedName + "-bystore"));
    }

    private static void aggregateOrdersByRevenue(KStream<String, Order> generalOrderStream, String storedName, KTable<String, Store> storesTable) {
        //Initializer
        Initializer<TotalRevenue> totalRevenueInitializer = TotalRevenue::new;
        //aggregator
        Aggregator<String, Order, TotalRevenue> aggregator = (key, value, aggregate) ->
                aggregate.updateRunningRevenue(key, value);

        var revenueKTable = generalOrderStream
                .map((key, value) -> KeyValue.pair(value.locationId(), value))
                .groupByKey(Grouped.with(Serdes.String(), SerdesFactory.orderSerdes()))
                .aggregate(
                        totalRevenueInitializer,
                        aggregator,
                        Materialized.<String, TotalRevenue, KeyValueStore<Bytes, byte[]>>as(storedName)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SerdesFactory.totalRevenueSerdes())

                );

        //KTable-KTable Join
        ValueJoiner<TotalRevenue, Store, TotalRevenueWithAddress> valueJoiner = TotalRevenueWithAddress::new;

        var revenueWithStoreTable = revenueKTable
                .join(
                        storesTable,
                        valueJoiner
                        );


        revenueWithStoreTable
                .toStream()
                .print(Printed.<String, TotalRevenueWithAddress>toSysOut().withLabel(storedName + "-bystore"));

    }

    private static void aggregateOrdersByCount(KStream<String, Order> generalOrderStream, String storedName, KTable<String, Store> storeKTable) {
        var ordersCountPerStore = generalOrderStream
//                .map((key, value) -> KeyValue.pair(value.locationId(), value))

                .groupByKey(Grouped.with(Serdes.String(), SerdesFactory.orderSerdes()))
                .count(Named.as(storedName), Materialized.as(storedName));

        ValueJoiner<Long, Store, TotalCountWithAddress> valueJoiner = TotalCountWithAddress::new;

        var countWithStoreTable = ordersCountPerStore
                .join(
                  storeKTable,
                  valueJoiner
                );

        countWithStoreTable
                .toStream()
                .print(Printed.<String, TotalCountWithAddress>toSysOut().withLabel(storedName + "-bystore"));
    }

    private static void printLocalDateTimes(Windowed<String> key, Long value) {
        var startTime = key.window().startTime();
        var endTime = key.window().endTime();
        log.info("startTime: {} , endTime : {}", startTime, endTime);
        LocalDateTime startLDT = LocalDateTime.ofInstant(startTime, ZoneId.of(ZoneId.SHORT_IDS.get("CST")));
        LocalDateTime endLDT = LocalDateTime.ofInstant(endTime, ZoneId.of(ZoneId.SHORT_IDS.get("CST")));
        log.info("startLDT : {} , endLDT : {}, Count : {}", startLDT, endLDT, value);
    }
}
