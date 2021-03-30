package com.raymond.queue.basis;

import com.raymond.queue.Consumption;
import com.raymond.queue.FileQueue;

import java.nio.charset.Charset;

/**
 * 集合对象的消费者
 *
 * @author :  raymond
 * @version :  V1.0
 * @date :  2021-01-21 17:31
 */
@SuppressWarnings("all")
public class ConsumptionStr extends Consumption<String> {


    public ConsumptionStr(String path, String topic, String groupName, FileQueue fileQueue, FileQueue.GrowMode growMode, String srcGroupName) throws Exception {
        super(String.class, path, topic, groupName, fileQueue, growMode, srcGroupName);
    }

    @Override
    protected String getData(byte[] bytes) {
        return new String(bytes, Charset.defaultCharset());
    }
}
