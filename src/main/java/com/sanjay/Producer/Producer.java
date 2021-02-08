package com.sanjay.Producer;

//import com.sanjay.Entity.User;

import com.sanjay.Model.User;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class Producer {
    public static void main(String[] args){
        // For example 192.168.1.1:9092,192.168.1.2:9092
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "com.sanjay.Serialization.UserSerializer");


        KafkaProducer<String, User> producer = new KafkaProducer<>(properties);
        try{

            User user = new User(1, "Sanjay", 22, "B.Tech");
            producer.send(new ProducerRecord<String, User>("user", String.valueOf(1), user));
            System.out.println("Message " + user.toString() + " sent !!");
        }
        catch (Exception e){
            e.printStackTrace();
        }
        finally {
            producer.close();
        }
    }
}