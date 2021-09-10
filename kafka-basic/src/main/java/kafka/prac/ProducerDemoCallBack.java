package kafka.prac;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoCallBack {
    public static void main(String[] args) {
        final Logger logger= LoggerFactory.getLogger(ProducerDemoCallBack.class);

        //create producer properties
        String server="0.0.0.0:9092";
        Properties properties=new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,server);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create producer
        KafkaProducer<String ,String> producer=new KafkaProducer<String, String>(properties);
        //produce record

        ProducerRecord<String,String> record=new ProducerRecord<String, String>("first_topic","Hello World");
        //send data
        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if(e==null)
                {
                    logger.info("metadata  topic:"+recordMetadata.topic()+"partion"+recordMetadata.partition());
                }else
                {
                    logger.error("error occur",e);
                }
            }
        });
        producer.flush();
        producer.close();

    }

}
