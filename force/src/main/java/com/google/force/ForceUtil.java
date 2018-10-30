package com.google.force;

import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;

public class ForceUtil {
    public static ForceInputStream createStream() {
        return new ForceInputStream();
    }
}
