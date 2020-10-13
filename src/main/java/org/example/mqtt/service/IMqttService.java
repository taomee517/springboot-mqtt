package org.example.mqtt.service;


import org.example.mqtt.context.mqtt.*;

import java.util.List;

/**
 * @author 罗涛
 * @title IMqttProtocol
 * @date 2020/9/22 15:17
 */
public interface IMqttService {

    String parsePayload(byte[] bytes);

    boolean checkValid(String user, String pwd);

    boolean topicValidate(String topicFilter);

    boolean needSecondParse();


    //session
    void putSession(String clientIdentifier, SessionStore session);

    boolean containsSession(String clientIdentifier);

    SessionStore getSession(String clientIdentifier);

    void removeSession(String clientIdentifier);


    // DupPublish
    void putDupPublishMessage(String clientId, DupPublishMessageStore dupPublishMessageStore);

    List<DupPublishMessageStore> getDupPublishMessage(String clientIdentifier);

    void removeDupPublishMessageByClient(String clientIdentifier);

    void removeDupPublishMessage(String clientIdentifier, int messageId);



    //DupPubRel
    void putDupPubRelMessage(String clientId, DupPubRelMessageStore dupPubRelMessageStore);

    List<DupPubRelMessageStore> getDupPubRelMessage(String clientIdentifier);

    void removeDupPubRelMessageByClient(String clientIdentifier);

    void removeDupPubRelMessage(String clientId, int messageId);


    //subscribe

    void putSubscribeMessage(String topicFilter, SubscribeStore subscribeStore);

    void removeSubscribeMessage(String topicFilter, String clientId);

    void removeSubscribeByClient(String clientIdentifier);

    List<SubscribeStore> searchSubscribe(String topic);



    //retain Message

    void putRetainMessage(String topicName, RetainMessageStore retainMessageStore);

    List<RetainMessageStore> searchRetainMessage(String topicFilter);

    void removeRetainMessage(String topicName);



    int getNextMessageId(String clientId);
}
