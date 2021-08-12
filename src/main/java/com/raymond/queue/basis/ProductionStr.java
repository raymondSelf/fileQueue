package com.raymond.queue.basis;

import com.raymond.queue.BlockingProduction;

import java.io.IOException;
import java.nio.charset.Charset;

/**
 * 集合对象的生产者
 *
 * @author :  raymond
 * @version :  V1.0
 * @date :  2021-01-21 17:31
 */
public class ProductionStr extends BlockingProduction<String> {

    public ProductionStr(String path, String topic) throws IOException {
        super(path, topic);
    }

    @Override
    protected byte[] getBytes(String e) {
        return e.getBytes(Charset.defaultCharset());
    }
}
