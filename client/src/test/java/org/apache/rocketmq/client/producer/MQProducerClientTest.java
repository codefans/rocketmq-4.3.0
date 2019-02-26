package org.apache.rocketmq.client.producer;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.junit.Test;

/**
 * @author:
 * @date: 2019-02-26 14:54:07
 */
public class MQProducerClientTest {

    @Test
    public void sendMsgTest() throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        String producerGroupTemp = "producerGroupName";
        String topic = "myTopicTest";
        DefaultMQProducer producer = new DefaultMQProducer(producerGroupTemp);
        producer.setNamesrvAddr("10.75.167.72:9876");
//        producer.setCompressMsgBodyOverHowmuch(16);
        Message message = new Message(topic, "This is a very huge message!".getBytes());

        producer.start();

        SendResult sendResult = producer.send(message);
        System.out.println(sendResult);

        producer.shutdown();

    }

}
