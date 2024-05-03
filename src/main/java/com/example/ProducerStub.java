package com.example;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.bson.Document;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;

public class ProducerStub {
    private static final String CONFIG_FILE = "config/config.properties";
    private static final String mongoURL = "Your-Mongo-DB-URL-Here";
    private final static String HOST = "localhost";
    private final static int PORT = 5672;
    
    
    public static void main(String[] args) {
        
        try {
            Properties config = loadConfig(CONFIG_FILE);
            Properties collections = getCollections(config);
            Properties routingKeys = getRoutingKeys(config);

            // Connect to RabbitMQ and publish data
            publishData(collections, routingKeys);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    

    private static void publishData(Properties collections, Properties routingKeys) {
        // Connect to RabbitMQ
        try {
            Properties config = loadConfig(CONFIG_FILE);
            
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(HOST); // Set RabbitMQ server host
            factory.setPort(PORT); // Set RabbitMQ server port
            
            try (Connection connection = factory.newConnection();
                 Channel channel = connection.createChannel()) {
                // Declare the exchange
                channel.exchangeDeclare(Exchange.EXCHANGE_NAME, "topic");

                // Iterate over collections and publish data
                for (String collectionName : collections.stringPropertyNames()) {
                	String key = collectionName.substring(16);
                	
                    String data = fetchDataFromMongo(collectionName, collections.getProperty(collectionName));
                    
                    String routingKey = routingKeys.getProperty("Route" + key); // Extract key dynamically
                    String topic = config.getProperty("Topic" + key); // Extract topic dynamically
                    
                    // Publish the data to the exchange with the appropriate routing key
                    channel.basicPublish(Exchange.EXCHANGE_NAME, topic, null, data.getBytes());
                    
                    System.out.println("Published to exchange '" + Exchange.EXCHANGE_NAME + "' with routing key '" + routingKey + "' and topic '" + topic + "': " + data);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private static String fetchDataFromMongo(String collectionName, String query) {
    	MongoClient mongoClient = MongoClients.create(mongoURL);
        MongoDatabase database = mongoClient.getDatabase("assignment-2");

        MongoCollection<Document> collection = database.getCollection(collectionName);
        MongoCursor<Document> cursor = collection.find(Document.parse(query)).iterator();

        StringBuilder result = new StringBuilder();
        while (cursor.hasNext()) {
            result.append(cursor.next().toJson()).append("\n");
        }
        cursor.close();
        mongoClient.close();

        return result.toString();
    }


    private static Properties loadConfig(String fileName) throws IOException {
        Properties config = new Properties();
        try (FileInputStream fis = new FileInputStream(fileName)) {
            config.load(fis);
        }
        return config;
    }

    private static Properties getCollections(Properties config) {
        Properties collections = new Properties();
        for (String key : config.stringPropertyNames()) {
            if (key.startsWith("EduCostStatQuery")) {
                collections.put(key, config.getProperty(key));
            }
        }
        return collections;
    }

    private static Properties getRoutingKeys(Properties config) {
        Properties routingKeys = new Properties();
        for (String key : config.stringPropertyNames()) {
            if (key.startsWith("Route")) {
                routingKeys.put(key, config.getProperty(key));
            }
        }
        return routingKeys;
    }
}
