package CoolKafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * Sample code for kafka - useful for various situations
 *
 */
public class App {


    public static void main(String[] args) {


        OpenStatus.myDisplay();

        //properties for kafka
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "myGroup");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");


        //this is the Kafka Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        //subscribing to the stuart topic - you have to give it a list, even if there is just one topic
        consumer.subscribe(Arrays.asList("stuart"));

        //n.b. this HAS to be a while(true) loop, because we need to constantly poll kafka or we will DIE!!!
        //this is a fundamental concept with kafka. just live with it.
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(10);
                for (ConsumerRecord<String, String> record : records)
                {
                    System.out.println(record.value()+"___I'VE BEEN PROCESSED!");
                }
            }
        }
        //n.b. there is no catch statement for this try statement, which is a bit odd. just the "finally" below.
        finally {
            consumer.close();
        }

    }
}


