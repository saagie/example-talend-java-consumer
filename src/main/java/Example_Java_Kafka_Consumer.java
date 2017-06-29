import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Christophe on 27/06/2017.
 */
public class Example_Java_Kafka_Consumer {

    public static void main(String[] args) {
        //Configuration d'un consumer
        KafkaConsumer<String, String> consumer = createKafkaConsumer();

        //Configuring of the consumer
        consumer.subscribe(Collections.singletonList("TopicJAVA"), new ConsumerRebalance());

        try {
            while (true) {
                // Creation of the message
                ConsumerRecords<String,String> consumerRecords=consumer.poll(100);
                // Display topic message
                consumerRecords.forEach(Example_Java_Kafka_Consumer::display);
                // Manual commit of offset
                manualAsynchronousCommit(consumer);

            }
        } finally {
            // Close the kafka consumer
            consumer.close();
        }
    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {
        Map<String,Object> map=new HashMap<>();
        map.put("bootstrap.servers","127.0.0.1:9092,127.0.0.1:9093");
        map.put("group.id","MyGroupId");
        map.put("enable.auto.commit","false");
        map.put("auto.commit.interval.ms","1000");
        map.put("auto.offset.reset","latest");
        map.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        map.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");

        return new KafkaConsumer<>(map);
    }


    private static void display(ConsumerRecord<String, String> record) {
        System.out.println(String.format("topic = %s, partition: %d, offset: %d: %s", record.topic(), record.partition(), record.offset(), record.value()));
    }

    private static void manualAsynchronousCommit(KafkaConsumer<String, String> consumer) {
        consumer.commitAsync((offsets, exception) ->
        {
            if (exception != null) {
                exception.printStackTrace();
            } else if (offsets != null) {
                offsets.forEach((topicPartition, offsetAndMetadata) -> {
                   // System.out.printf("commited offset %d for partition %d of topic %s%n",
                   //        offsetAndMetadata.offset(), topicPartition.partition(), topicPartition.topic());
                });
            }
        });
    }

    private static class ConsumerRebalance implements ConsumerRebalanceListener {
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            partitions.forEach(
                    topicPartition ->
                            System.out.printf("Assigned partition %d of topic %s%n",
                                    topicPartition.partition(), topicPartition.topic())
            );

        }

        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

            for (TopicPartition topicPartition : partitions) {
                System.out.printf("Revoked partition %d of topic %s%n",
                        topicPartition.partition(), topicPartition.topic());
            }

        }
    }
}
