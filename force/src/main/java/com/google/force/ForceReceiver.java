package com.google.force;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.json.JSONObject;

import java.util.Iterator;
import java.util.List;

public class ForceReceiver extends Receiver<String> {

    String username;
    String password;
    String clientId;
    String clientSecret;
    String topic;
    ForceConnectorHelper fconnect;

    public ForceReceiver(int timeout, String clientId, String clientSecret, String username, String password, String topic) {
        super(StorageLevel.MEMORY_AND_DISK_2());
      this.clientId = clientId;
      this.username = username;
      this.password = password;
      this.topic = topic;
    }

    @Override
    public void onStart() {

        fconnect = new ForceConnectorHelper(
                10,
                this.clientId,
                this.clientSecret,
                this.username,
                this.password,
                this.topic);

        fconnect.start();
    }
    @Override
    public void onStop() {
        // There is nothing much to do as the thread calling receive()
        // is designed to stop by itself if isStopped() returns false
    }
    private void receive() {
        List<JSONObject> jsonMsgs = fconnect.getMessages();
        Iterator<JSONObject> jsonItr = jsonMsgs.iterator();
        while(jsonItr.hasNext()) {
            store(jsonItr.next().toString());
        }
    }
}
