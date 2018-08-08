package com.devil.topic;

import com.rabbitmq.client.*;

import java.io.IOException;

public class TopicProduct {
    private static final String EXCHANGE_NAME = "topic";

    private static String getSeverity(String[] strings) {
        if (strings.length < 1)
            return "info";
        return strings[0];
    }

    private static String getMessage(String[] strings) {
        if (strings.length < 2)
            return "Hello World!";
        return joinStrings(strings, " ", 1);
    }

    private static String joinStrings(String[] strings, String delimiter, int startIndex) {
        int length = strings.length;
        if (length == 0) return "";
        if (length < startIndex) return "";
        StringBuilder words = new StringBuilder(strings[startIndex]);
        for (int i = startIndex + 1; i < length; i++) {
            words.append(delimiter).append(strings[i]);
        }
        return words.toString();
    }

    public static void main(String[] arg) throws IOException {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, "topic",true);  //声明持久化交换器


        for (int i = 0; i < 100 ; i++) {
            if (i % 3 == 1)
            {
                arg = new String[]{"info.devil"};
                String severity = getSeverity(arg);
                String message = getMessage(arg);

                channel.basicPublish(EXCHANGE_NAME, severity, null, message.getBytes());
                System.out.println(" [x] Sent '" + severity + "':'" + message + "'");
            }

            if (i % 3 == 2)
            {
                arg = new String[]{"error.devil"};
                String severity = getSeverity(arg);
                String message = getMessage(arg);
                channel.basicPublish(EXCHANGE_NAME, severity, null, message.getBytes());
                System.out.println(" [x] Sent '" + severity + "':'" + message + "'");
            }
            if (i % 3 == 0)
            {
                arg = new String[]{"warning.devil"};
                String severity = getSeverity(arg);
                String message = getMessage(arg);

                channel.basicPublish(EXCHANGE_NAME, severity, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());   //持久化消息
                System.out.println(" [x] Sent '" + severity + "':'" + message + "'");
            }
        }

        channel.close();
        connection.close();
    }
}
