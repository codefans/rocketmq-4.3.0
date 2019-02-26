package org.apache.rocketmq.client.consumer;

import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;

/**
 * @author:
 * @date: 2019-02-26 16:35:45
 */
public class MQConsumerClientTest {

    @Test
    public void consumeByPull() throws MQClientException, RemotingException, InterruptedException, MQBrokerException, UnsupportedEncodingException {
        String consumerGroup = "producerGroupName";
        String namesrvAddr = "10.75.167.72:9876";
        String brokerName = "lejr-mac-notebook.local";
        String topic = "myTopicTest";

        DefaultMQPullConsumer pullConsumer = new DefaultMQPullConsumer(consumerGroup);
        pullConsumer.setNamesrvAddr(namesrvAddr);
        pullConsumer.start();

        MessageQueue messageQueue = new MessageQueue(topic, brokerName, 4);
        PullResult pullResult = pullConsumer.pull(messageQueue, "*", 0, 3);

        System.out.println(pullResult);

        List<MessageExt> msgFoundList = pullResult.getMsgFoundList();
        for(MessageExt message : msgFoundList) {
            String msgId = message.getMsgId();
            String msgBody = new String(message.getBody(), "UTF-8");
            System.out.println("msgId=" + msgId + ", msgBody=" + msgBody);
            System.out.println("topic=" + message.getTopic() + ", queueId=" + message.getQueueId() + ", queueOffset=" + message.getQueueOffset());

            Map<String, String> properties = message.getProperties();


            System.out.println(properties);
        }

    }

    @Test
    public void consumeByPush() throws MQClientException {

        String consumerGroup = "producerGroupName";
        String namesrvAddr = "10.75.167.72:9876";
        String topic = "myTopicTest";

        DefaultMQPushConsumer pushConsumer = new DefaultMQPushConsumer(consumerGroup);
        pushConsumer.setNamesrvAddr(namesrvAddr);
        pushConsumer.setPullInterval(60 * 1000);

        pushConsumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                            ConsumeConcurrentlyContext context) throws UnsupportedEncodingException {

                for(MessageExt msg:msgs) {
                    String msgId = msg.getMsgId();
                    String msgBody = new String(msg.getBody(), "UTF-8");
                    System.out.println("msgId=" + msgId + ", msgBody=" + msgBody);
                }


                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        pushConsumer.subscribe(topic, "*");
        pushConsumer.start();

//        DefaultMQPushConsumerImpl pushConsumerImpl = pushConsumer.getDefaultMQPushConsumerImpl();
//        rebalancePushImpl = spy(new RebalancePushImpl(pushConsumer.getDefaultMQPushConsumerImpl()));
//        Field field = DefaultMQPushConsumerImpl.class.getDeclaredField("rebalanceImpl");
//        field.setAccessible(true);
//        field.set(pushConsumerImpl, rebalancePushImpl);

    }


}