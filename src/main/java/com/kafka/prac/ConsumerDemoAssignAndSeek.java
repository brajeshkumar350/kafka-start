package com.kafka.prac;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignAndSeek {
    public static void main(String[] args) {
        Logger logger=LoggerFactory.getLogger(ConsumerDemoAssignAndSeek.class.getName());
        String server="0.0.0.0:9092";
        String topic="first_topic";
        Properties properties=new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,server);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        KafkaConsumer<String,String> consumer=new KafkaConsumer<String, String>(properties);

        TopicPartition topicPartition=new TopicPartition(topic,0);
        consumer.assign(Arrays.asList(topicPartition));
        long offset=15L;
        consumer.seek(topicPartition,offset);
        int num=0;
        while (true)
        {
            ConsumerRecords<String,String> records=consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String,String> record:records) {
                logger.info("key: "+record.key()+" value: "+record.value());
                num +=num;
                if(num>=5)
                {
                    break;
                }
            }
            }
        }
    }
