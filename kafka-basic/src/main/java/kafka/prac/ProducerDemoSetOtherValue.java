package kafka.prac;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemoSetOtherValue {
    public static void main(String[] args) {
        //create producer properties
        String server="0.0.0.0:9092";
        Properties properties=new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,server);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        //safe producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG,Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"5");

        //high throughtput producer(to add lag and batch size)
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,Integer.toString(32*1024));

        //create producer
        KafkaProducer<String ,String> producer=new KafkaProducer<String, String>(properties);
        //produce record

        ProducerRecord<String,String> record=new ProducerRecord<String, String>("first_topic","Hello World");
        //send data
        producer.send(record);
        producer.flush();
        producer.close();

    }

}
