package com.example.kafkaStreams;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class KafkaStreamAppMain {
	
	 public static void main(final String[] args) {
		 Properties props = new Properties();
	        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "payload-to-json-streams");
	        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
	        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
	        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
	        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
	        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

	        StreamsBuilder builder = new StreamsBuilder();
	        KStream<String, String> textLines = builder.stream("transaction2", Consumed.with(Serdes.String(), Serdes.String()));
	        
	        
	        KStream<String, String> validData = textLines
	                .mapValues(KafkaStreamAppMain::extractAndConvertPayload)
	                .filter((key, value) -> !value.equals("{}"));

	        KStream<String, String> debData = validData
	                .filter((key, value) -> value.contains("transactionType\":\"debit"))
	                .selectKey((key, value) -> "debit"); //to debit q

	        KStream<String, String> credData = validData
	                .filter((key, value) -> value.contains("transactionType\":\"credit"))
	                .selectKey((key, value) -> "credit"); //to credit q

	        KStream<String, String> invalidData = textLines
	                .filter((key, value) -> {
	                    String convertedValue = extractAndConvertPayload(value);
	                    return convertedValue.equals("{}");
	                }); // to exception-q


	        debData.to("debit-q", Produced.with(Serdes.String(), Serdes.String()));
	        debData.print(Printed.toSysOut());
	        credData.to("credit-q", Produced.with(Serdes.String(), Serdes.String()));
	        credData.print(Printed.toSysOut());
	        invalidData.to("exceptions-q", Produced.with(Serdes.String(), Serdes.String()));
	        invalidData.print(Printed.toSysOut());

	        KafkaStreams streams = new KafkaStreams(builder.build(), props);
	        streams.cleanUp();
	        streams.start();
	    }

	    private static String extractAndConvertPayload(String message) {
	        ObjectMapper objectMapper = new ObjectMapper();
	        try {
	            JsonNode rootNode = objectMapper.readTree(message);
	            String payload = rootNode.get("payload").asText();
	            return convertToJSON(payload);
	        } catch (Exception e) {
	            e.printStackTrace();
	            return "{}"; // Default empty JSON
	        }
	    }

	    private static String convertToJSON(String payload) {
	        String[] parts = payload.split(",");
	        if (parts.length != 7) {
	            return "{}"; // Invalid data format
	        }

	        ObjectMapper objectMapper = new ObjectMapper();
	        ObjectNode jsonNode = JsonNodeFactory.instance.objectNode();

	        try {
	            jsonNode.put("firstName", parts[0]);
	            jsonNode.put("secondName", parts[1]);
	            jsonNode.put("accountType", parts[2]);
	            jsonNode.put("accountNumber", Integer.parseInt(parts[3]));
	            jsonNode.put("transactionType", parts[4]);
	            jsonNode.put("amount",  Float.parseFloat(parts[5]));
	            jsonNode.put("transactionDate", parts[6]);

	            return objectMapper.writeValueAsString(jsonNode);
	        } catch (Exception e) {
	            e.printStackTrace();
	            return "{}"; // Default empty JSON
	        }
	 }
}
