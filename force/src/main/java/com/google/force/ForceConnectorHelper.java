/*
 * Copyright 2018 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.force;

import jline.internal.Log;
import org.cometd.bayeux.Message;
import org.cometd.bayeux.client.ClientSessionChannel;
import org.cometd.client.BayeuxClient;
import org.cometd.client.transport.LongPollingTransport;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.util.ssl.SslContextFactory;

import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

public class ForceConnectorHelper {

    private  int timeout;
     private String server;
    private String clientId;
    private  String clientSecret;
    private  String username;
    private  String password;
    private  String topic;
    private  String defaultPushEndpoint;

    private static Logger LOG = LoggerFactory.getLogger(ForceConnectorHelper.class);
    private final BlockingQueue<JSONObject> jsonQueue = new LinkedBlockingQueue<>();

    public ForceConnectorHelper(int timeout, String clientId, String clientSecret, String username, String password, String topic, String pushEndPoint) {

        this.timeout = timeout;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.username = username;
        this.password = password;
        this.topic = topic;
        this.defaultPushEndpoint= pushEndPoint;
    }



    private JSONObject oauthLogin(String CLIENT_ID, String CLIENT_SECRET, String USERNAME, String PASSWORD) throws Exception {

        SslContextFactory sFactory = new SslContextFactory();
        HttpClient httpClient = new HttpClient(sFactory);
        httpClient.start();
        String url = "https://login.salesforce.com/services/oauth2/token";

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

        ClientSessionChannel.MessageListener subscriptionListener = new ClientSessionChannel.MessageListener() {
            @Override
            public void onMessage(ClientSessionChannel channel, Message message) {
                LOG.debug("onMessage event: "+ message.getJSON());
            }
        };

        LOG.debug("Server: " + server + " ClientID: " + clientId + " ClientSecret: " + clientSecret + " Username: " + username);
        
        JSONObject response = oauthLogin(clientId, clientSecret, username, password);
        LOG.info("Oauth Response: " + response);
        if (!response.has("access_token")) {
            LOG.error("OAuth Failed: " + response.toString());
            throw new Exception("OAuth failed: " + response.toString());
        }

        // Get what we need from the OAuth response
        final String sid = response.getString("access_token");
        String instance_url = response.getString("instance_url");

        LOG.debug("sid: " + sid);
        LOG.debug("instance_url: " + instance_url);

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
        BayeuxClient client = new BayeuxClient(instance_url + DEFAULT_PUSH_ENDPOINT, transport);

        client.handshake(subscriptionListener);

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
        LOG.error("Client did not handshake with server");
        throw new IllegalStateException("Client did not handshake with server");
    }

    public void setNoValidation() throws Exception {
        // Create a trust manager that does not validate certificate chains
        TrustManager[] trustAllCerts = new TrustManager[]{new X509TrustManager() {
            @Override
            public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                return null;
            }

            @Override
            public void checkClientTrusted(X509Certificate[] certs,
                                           String authType) {
            }

            @Override
            public void checkServerTrusted(X509Certificate[] certs,
                                           String authType) {
            }
        }};
    }

    public void start() {

        try {
            BayeuxClient bayeuxClient = getClient(timeout, this.defaultPushEndpoint, server, clientId, clientSecret, username, password);
            setNoValidation();
            waitForHandshake(bayeuxClient, 10 * 1000, 1000);
            LOG.info("Client handshake done");
            bayeuxClient.getChannel("/topic/" + topic).subscribe(new ClientSessionChannel.MessageListener() {
                @Override
                public void onMessage(ClientSessionChannel channel, Message message) {
                    try {
                        JSONObject jsonObject = new JSONObject(new JSONTokener(message.getJSON()));
                        LOG.info("Message: " + message.getJSON());
                        jsonQueue.add(jsonObject);
                    } catch (org.json.JSONException e) {
                        LOG.error(e.getLocalizedMessage());
                    }
                }
            });

        } catch (Exception e) {
            LOG.error("could not start client: " + e.getMessage());
        }


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
