package org.muieer.flink_practice.java.example;

import lombok.*;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;

public class Example {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Person> dataStream = env.fromElements(
                new Person("Fred", 35),
                new Person("muieer", 26),
                new Person("Pebbles", 2)
        );

        SingleOutputStreamOperator<Person>
                filterOperator = dataStream.filter(person -> person.age >= 18);
        filterOperator.print();

        env.execute();
    }

    @Getter
    @Setter
    @ToString
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Person {
        private String name;
        private Integer age;
    }
}
