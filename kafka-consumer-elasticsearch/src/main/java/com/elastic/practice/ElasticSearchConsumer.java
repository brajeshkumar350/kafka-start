package com.elastic.practice;



import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {

    public static RestHighLevelClient createClient() {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http")));
        return client;
    }
    public static KafkaConsumer<String,String> createConsumer(String topic)
    {
        String server="0.0.0.0:9092";
        String groupid="kafka-demo-elasticsearch";
        Properties properties=new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,server);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupid);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        KafkaConsumer<String,String> consumer=new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));
        return consumer;
    }

    public static void main(String[] args) throws IOException {
        Logger logger=LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
        RestHighLevelClient client = createClient();
        /*String jsonString = "{\n" +
                "\t\"foo\":\"bar\"\n" +
                "}";
        IndexRequest request = new IndexRequest("twitter", "tweets")
                .source(jsonString, XContentType.JSON);
        IndexResponse indexResponse=client.index(request, RequestOptions.DEFAULT);*/
        //String id=indexResponse.getId();
        //logger.info("id: "+id);
        KafkaConsumer<String ,String> consumer=createConsumer("twitter_twits");
        while (true)
        {
            ConsumerRecords<String,String> records=consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String,String> record:records) {
                String twitterid=extractIdFromTwitter(record.value());
                //this will make our rquest as idempotent
                IndexRequest request = new IndexRequest("twitter",
                        "twits",
                        twitterid)
                        .source(record.value(), XContentType.JSON);
                IndexResponse indexResponse=client.index(request, RequestOptions.DEFAULT);
                String id=indexResponse.getId();
                logger.info("id: "+id);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        //client.close();

    }
    private static JsonParser jsonParser=new JsonParser();
    private static String extractIdFromTwitter(String jsonString){
        return jsonParser.parse(jsonString).getAsJsonObject().get("id_str").getAsString();

    }
}
