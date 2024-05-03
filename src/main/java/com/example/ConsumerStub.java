package com.example;

import com.rabbitmq.client.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class ConsumerStub {

    private static final String CONFIG_FILE = "config/config.properties";
    private final static String HOST = "localhost";
    private final static int PORT = 5672;

    public static void main(String[] args) {
        try {
            // Load configuration from file
            Properties config = loadConfig(CONFIG_FILE);

            // Connect to RabbitMQ
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(HOST); // Set RabbitMQ server host
            factory.setPort(PORT); // Set RabbitMQ server port
            
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();

            // Declare the exchange
            channel.exchangeDeclare(Exchange.EXCHANGE_NAME, "topic");

            // Declare and bind queues for each topic
            for (String key : config.stringPropertyNames()) {
                if (key.startsWith("Topic")) {
                    String topic = config.getProperty(key);
                    String queueName = "queue_" + key.substring(5); // Extract queue name from topic
                    channel.queueDeclare(queueName, false, false, false, null);
                    channel.queueBind(queueName, Exchange.EXCHANGE_NAME, topic);
                    // System.out.println("Queue '" + queueName + "' bound to exchange '" + Exchange.EXCHANGE_NAME + "' with topic '" + topic + "'");
                }
            }

            // Create a consumer and listen for messages
            Consumer consumer = new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(
                		String consumerTag, 
                		Envelope envelope, 
                		AMQP.BasicProperties properties, 
                		byte[] body) throws IOException {
                	
                    String message = new String(body, "UTF-8");
                    System.out.println("Received message: " + message);
                }
            };

            // Start consuming messages from the queues
            for (String key : config.stringPropertyNames()) {
                if (key.startsWith("Topic")) {
                    String queueName = "queue_" + key.substring(5);
                    channel.basicConsume(queueName, true, consumer);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Properties loadConfig(String fileName) throws IOException {
        Properties config = new Properties();
        try (FileInputStream fis = new FileInputStream(fileName)) {
            config.load(fis);
        }
        return config;
    }
}
