package com.kafka.tutorial.twit;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
    Logger logger= LoggerFactory.getLogger(TwitterProducer.class.getName());
    String consumerKey = "lFWWWIFApLdOj0A2mwd4MITOE";
    String consumerSecret = "Vwlo46KMacjsYJURW6laAPs5nHTkBJMwUdcsJFmyKcXnIOseZ1";
    String secret = "2KRFr5WsPeaVz3SF14lvmXffCgNoPICk1PbFnsLfJhBUQ";
    String token = "3061534490-thO5Phdy7kElSZ4Uimyd0B4kjS5VkoFm6fQuXzK";

    TwitterProducer() {
    }

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run() {
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(100000);
        Client client=createTwitterClient(msgQueue);
        client.connect();
        KafkaProducer<String,String> kafkaProducer=createKafkaProducer();
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            logger.info("client stop");
            client.stop();
        logger.info("producer closed");
        kafkaProducer.close();
        }));
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if(msg!=null)
            {
                logger.info(msg);
                kafkaProducer.send(new ProducerRecord<>("twitter_twits",null,msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e!=null)
                        {
                            logger.error("error in producer");
                        }
                    }
                });

            }

        }
        logger.info("end for client");
        //BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(1000);
    }

    public KafkaProducer<String, String> createKafkaProducer() {
        String server="0.0.0.0:9092";
        Properties properties=new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,server);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create producer
        KafkaProducer<String ,String> producer=new KafkaProducer<String, String>(properties);
        return producer;
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {/** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */



        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
// Optional: set up some followings and track terms
        // List<Long> followings = Lists.newArrayList(1234L, 566788L);
        List<String> terms = Lists.newArrayList("kafka");
        //hosebirdEndpoint.followings(followings);
        hosebirdEndpoint.trackTerms(terms);

// These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));       // optional: use this if you want to process client events

        Client hosebirdClient = builder.build();
        return hosebirdClient;
// Attempts to establish a connection.
        // hosebirdClient.connect();

    }

}
