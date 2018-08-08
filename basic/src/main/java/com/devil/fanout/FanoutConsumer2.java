package com.devil.fanout;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

import java.io.IOException;

public class FanoutConsumer2 {
    private static final String EXCHANGE_NAME = "logs";

    public static void main(String[] arg) throws IOException,InterruptedException{

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        //channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
        String queueName = channel.queueDeclare().getQueue();   //在Java客戶端中，當我們沒有向queueDeclare（）提供參數時，我們創建一個非持久的，獨占的自動刪除隊列，其中包含一個生成的名稱：
        channel.queueBind(queueName, EXCHANGE_NAME, "");

        System.out.println(" 我是第二个消费者");
        QueueingConsumer consumer = new QueueingConsumer(channel);
        channel.basicConsume(queueName, true, consumer);

        while (true){
            QueueingConsumer.Delivery delivery = consumer.nextDelivery();
            String message = new String(delivery.getBody());

            System.out.println(" [x] Received '" + message + "'");
        }
    }
}
