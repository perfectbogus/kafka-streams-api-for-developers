package com.learnkafkastreams.topology;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafkastreams.domain.Greeting;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class GreetingStreamsTopology {

    public static String GREETINGS = "greetings";
    public static String GREETINGS_OUTPUT = "greetings-output";

    private final ObjectMapper objectMapper;

    public GreetingStreamsTopology(ObjectMapper objectMapper) {
      this.objectMapper = objectMapper;
    }
    @Autowired
    public void process(StreamsBuilder streamsBuilder){
        KStream<String, Greeting> greetingsStream = streamsBuilder
                .stream(
                        GREETINGS,
                        Consumed.with(
                            Serdes.String(),
//                            Serdes.String()
                            new JsonSerde<>(Greeting.class, objectMapper)
                        )
                );

        greetingsStream.print(
//                Printed.<String, String>toSysOut().withLabel("greetingsStream")
                Printed.<String, Greeting>toSysOut().withLabel("greetingsStream")
        );

        KStream<String, Greeting> modifiedStream = greetingsStream
                .mapValues((readOnlyKey, value) -> {
//                    value.toUpperCase()
                    if (value.message().equals("Error")) {
//                      throw new IllegalStateException("Error Occurred");
                    }
                    return  new Greeting(value.message().toUpperCase(), value.timestamp());
                  }
                );


        modifiedStream.print(
                Printed.<String, Greeting>toSysOut().withLabel("modifiedStream")
        );

        modifiedStream
                .to(GREETINGS_OUTPUT, Produced.with(Serdes.String(), new JsonSerde<>(Greeting.class, objectMapper)));
    }

}
