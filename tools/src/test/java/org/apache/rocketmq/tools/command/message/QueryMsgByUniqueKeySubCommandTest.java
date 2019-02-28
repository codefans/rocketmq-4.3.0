package org.apache.rocketmq.tools.command.message;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExtImpl;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class QueryMsgByUniqueKeySubCommandTest {

    @Test
    public void testExecute() throws SubCommandException {

        QueryMsgByUniqueKeySubCommand queryMsgByUniqueKeySubCommand = new QueryMsgByUniqueKeySubCommand();

        //必须加这句,否则会报如下错误：
        //   Caused by: org.apache.rocketmq.client.exception.MQClientException: CODE: 17  DESC: The topic[myTopicTest] not matched route info
        System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, "127.0.0.1:9876");
//        有点地方是添加下面这个
//        System.setProperty(MixAll.NAMESRV_ADDR_ENV, "127.0.0.1:9876");

        String[] args = new String[]{"-n 127.0.0.1:9876", "-t myTopicTest", "-i 0A4BA748704118B4AAC284DFEEDA0000"};
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        CommandLine commandLine = ServerUtil.parseCmdLine("mqadmin ", args, queryMsgByUniqueKeySubCommand.buildCommandlineOptions(options), new PosixParser());
        queryMsgByUniqueKeySubCommand.execute(commandLine, options, null);

    }

}
