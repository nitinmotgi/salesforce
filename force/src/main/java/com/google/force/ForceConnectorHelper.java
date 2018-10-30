package com.google.force;

import org.cometd.bayeux.Message;
import org.cometd.bayeux.client.ClientSessionChannel;
import org.cometd.client.BayeuxClient;
import org.cometd.client.transport.LongPollingTransport;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.util.ssl.SslContextFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


import org.json.JSONObject;
import org.json.JSONTokener;

public class ForceConnectorHelper {

     int timeout;
     String server;
     String clientId;
     String clientSecret;
     String username;
     String password;
     String topic;

     String defaultPushEndpoint = "/cometd/36.0";

    public ForceConnectorHelper(int timeout, String clientId, String clientSecret, String username, String password, String topic) {

        this.timeout = timeout;
        this.server = server;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.username = username;
        this.password = password;
        this.topic = topic;
    }

    private BayeuxClient bayeuxClient = null;
    private final BlockingQueue<JSONObject> jsonQueue = new LinkedBlockingQueue<>();


    private JSONObject oauthLogin(String LOGIN_SERVER, String CLIENT_ID, String CLIENT_SECRET,
                                  String USERNAME, String PASSWORD) throws Exception {

        SslContextFactory sFactory = new SslContextFactory();
        HttpClient httpClient = new HttpClient(sFactory);
        httpClient.start();
        String url = "https://login.salesforce.com" + "/services/oauth2/token";

        String response = httpClient.POST(url).param("grant_type","password")
                .param("client_id",CLIENT_ID)
                .param("client_secret",CLIENT_SECRET)
                .param("username",USERNAME)
                .param("password",PASSWORD).send().getContentAsString();


        return new JSONObject(new JSONTokener(response));

    }

    private BayeuxClient getClient(int TIMEOUT, String DEFAULT_PUSH_ENDPOINT, String server, String clientId,
                                   String clientSecret, String username, String password) throws Exception {
        // Authenticate via OAuth
        JSONObject response = oauthLogin(server, clientId, clientSecret, username, password);
        if (!response.has("access_token")) {
            throw new Exception("OAuth failed: " + response.toString());
        }

        // Get what we need from the OAuth response
        final String sid = response.getString("access_token");
        String instance_url = response.getString("instance_url");

        SslContextFactory sFactory = new SslContextFactory();

        // Set up a Jetty HTTP client to use with CometD
        HttpClient httpClient = new HttpClient(sFactory);
        httpClient.setConnectTimeout(TIMEOUT);
        httpClient.start();

        Map<String, Object> options = new HashMap<String, Object>();
        // Adds the OAuth header in LongPollingTransport
        LongPollingTransport transport = new LongPollingTransport(
                options, httpClient) {
            @Override
            protected void customize(Request exchange) {
                super.customize(exchange);
                exchange.header("Authorization", "OAuth " + sid);
            }
        };

        // Now set up the Bayeux client itself
        BayeuxClient client = new BayeuxClient(instance_url
                + DEFAULT_PUSH_ENDPOINT, transport);

        return client;
    }


    private void waitForHandshake(BayeuxClient client,
                                  long timeoutInMilliseconds, long intervalInMilliseconds) {
        long start = System.currentTimeMillis();
        long end = start + timeoutInMilliseconds;
        while (System.currentTimeMillis() < end) {
            if (client.isHandshook())
                return;
            try {
                Thread.sleep(intervalInMilliseconds);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        throw new IllegalStateException("Client did not handshake with server");
    }

    public void start() {
        try {
            this.bayeuxClient = getClient(timeout, defaultPushEndpoint, server, clientId, clientSecret, username, password);
            this.bayeuxClient.handshake();
            waitForHandshake(this.bayeuxClient, 60 * 1000, 1000);
        } catch (Exception e) {
            e.printStackTrace();
        }

        this.bayeuxClient.getChannel("/topic/" + topic).subscribe(new ClientSessionChannel.MessageListener() {
            @Override
            public void onMessage(ClientSessionChannel channel, Message message) {
                try {
                    JSONObject jsonObject = new JSONObject(new JSONTokener(message.getJSON()));
                    jsonQueue.add(jsonObject);
                } catch (org.json.JSONException e) {
                    e.printStackTrace();
                }
            }
        });
    }
    public List<JSONObject> getMessages(){
        final List<JSONObject> objects = new ArrayList<>(10);
        jsonQueue.drainTo(objects, 10);
        if (objects.isEmpty()) {
            return objects;
        } else {
            return objects;
        }
    }
}
