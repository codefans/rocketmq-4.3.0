package org.apache.rocketmq.client.producer;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author:
 * @date: 2019-02-26 14:54:07
 */
public class MQProducerClientTest {

    /**
     * 同步发送
     * @throws MQClientException
     * @throws RemotingException
     * @throws InterruptedException
     * @throws MQBrokerException
     */
    @Test
    public void sendMsgTest() throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        String producerGroupTemp = "producerGroupName";
        String topic = "myTopicTest";
        DefaultMQProducer producer = new DefaultMQProducer(producerGroupTemp);
        producer.setNamesrvAddr("127.0.0.1:9876");
//        producer.setCompressMsgBodyOverHowmuch(16);
        Message message = new Message(topic, "This is a very huge message999!".getBytes());

        producer.start();

        SendResult sendResult = producer.send(message);
        System.out.println(sendResult);

        producer.shutdown();

    }

    /**
     * 异步发送
     * @throws MQClientException
     * @throws RemotingException
     * @throws InterruptedException
     * @throws MQBrokerException
     */
    @Test
    public void sendMsgAsyncTest() throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        String producerGroupTemp = "producerGroupName";
        String topic = "myTopicTest";
        DefaultMQProducer producer = new DefaultMQProducer(producerGroupTemp);
        producer.setNamesrvAddr("127.0.0.1:9876");
//        producer.setCompressMsgBodyOverHowmuch(16);
        Message message = new Message(topic, "This is a very huge message999!".getBytes());

        producer.start();

        SendCallback sendCallback = new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                System.out.println(sendResult);
            }

            @Override
            public void onException(Throwable e) {
                e.printStackTrace();
            }
        };
        producer.send(message, sendCallback);

        producer.shutdown();

    }

    /**
     *
     * @throws MQClientException
     * @throws RemotingException
     * @throws InterruptedException
     * @throws MQBrokerException
     */
    @Test
    public void sendMsgOnewayTest() throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        String producerGroupTemp = "producerGroupName";
        String topic = "myTopicTest";
        DefaultMQProducer producer = new DefaultMQProducer(producerGroupTemp);
        producer.setNamesrvAddr("127.0.0.1:9876");
//        producer.setCompressMsgBodyOverHowmuch(16);
        Message message = new Message(topic, "This is a very huge message999!".getBytes());

        producer.start();

        producer.sendOneway(message);

        producer.shutdown();

    }

    /**
     * 批量发送
     * @throws MQClientException
     * @throws RemotingException
     * @throws InterruptedException
     * @throws MQBrokerException
     */
    @Test
    public void sendMsgBatchTest() throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        String producerGroupTemp = "producerGroupName";
        String topic = "myTopicTest";
        DefaultMQProducer producer = new DefaultMQProducer(producerGroupTemp);
        producer.setNamesrvAddr("127.0.0.1:9876");
//        producer.setCompressMsgBodyOverHowmuch(16);
        Message message = new Message(topic, "This is a very huge message999!".getBytes());

        producer.start();

        List<Message> messageList = new ArrayList<>();
        messageList.add(message);
        SendResult sendResult = producer.send(messageList);
        System.out.println(sendResult);

        producer.shutdown();

    }

}
