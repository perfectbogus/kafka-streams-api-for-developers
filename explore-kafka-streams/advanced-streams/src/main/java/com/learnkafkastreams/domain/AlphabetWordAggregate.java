package com.learnkafkastreams.domain;

import lombok.extern.slf4j.Slf4j;

import java.util.HashSet;
import java.util.Set;

@Slf4j
public record AlphabetWordAggregate(String key,
                                    Set<String> valueList,
                                    int runningCount) {


    public AlphabetWordAggregate() {
       this("", new HashSet<>(), 0);
    }


    public AlphabetWordAggregate updateNewEvents(String key, String newValue){
        log.info("New Record key: {}, Value: {}", key, newValue);
        var newRunningCount = runningCount + 1;
        valueList.add(newValue);
        var aggregate = new AlphabetWordAggregate(key, valueList, newRunningCount);
        log.info("key: {}, AggregatedValue: {}", key, newValue);
        return aggregate;
    }


    public static void main(String[] args) {


        var al =new AlphabetWordAggregate();

    }

}


