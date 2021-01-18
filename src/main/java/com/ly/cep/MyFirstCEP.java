package com.ly.cep;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

public class MyFirstCEP {
    public static void main(String[] args) throws Exception {
        DataStream<Event> input = null;
        Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(
                new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) throws Exception {
                        return event.getId() == 42;
                    }
                }
        ).next("middle").subtype(SubEvent.class).where(
                new SimpleCondition<SubEvent>() {
                    @Override
                    public boolean filter(SubEvent subEvent) {
                        return subEvent.getVolume() >= 10.0;
                    }
                }
        ).followedBy("end").where(
                new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) {
                        return event.getName().equals("end");
                    }
                }
        );

        PatternStream<Event> patternStream = CEP.pattern(input, pattern);

        DataStream<Alert> result = patternStream.process(
                new PatternProcessFunction<Event, Alert>() {
                    @Override
                    public void processMatch(
                            Map<String, List<Event>> pattern,
                            Context ctx,
                            Collector<Alert> out) throws Exception {
                        out.collect(createAlertFrom(pattern));
                    }
                });
    }

    public static Alert createAlertFrom(Map<String, List<Event>> pattern) {
        return null;
    }
}
