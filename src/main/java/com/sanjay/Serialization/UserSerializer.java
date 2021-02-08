package com.sanjay.Serialization;
import org.apache.kafka.common.serialization.Serializer;
import org.codehaus.jackson.map.ObjectMapper;

public class UserSerializer implements Serializer {
    @Override
    public byte[] serialize(String s, Object o) {
        byte[] returnVal = null;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            returnVal = objectMapper.writeValueAsString(o).getBytes();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return returnVal;
    }



    @Override
    public void close() {

    }
}