package org.example.mqtt.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.example.mqtt.config.MqttProperties;
import org.example.mqtt.context.ContextManager;
import org.example.mqtt.context.mqtt.*;
import org.example.mqtt.utils.SnUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author 罗涛
 * @title G2cProtocol
 * @date 2020/9/22 14:14
 */

@Slf4j
@Component
public class MqttService implements IMqttService {

    @Autowired
    MqttProperties mqttProperties;

    //MQTT部分
    @Override
    public String parsePayload(byte[] bytes) {
        return new String(bytes);
    }

    @Override
    public boolean topicValidate(String topicFilter) {
//        if(!topicFilter.startsWith("$")){
//            return false;
//        }
        return true;
    }

    //session

    @Override
    public void putSession(String clientIdentifier, SessionStore session) {
        ContextManager.putSessionStore(clientIdentifier, session);
    }

    @Override
    public boolean containsSession(String clientIdentifier) {
        return ContextManager.containsSessionStore(clientIdentifier);
    }

    @Override
    public SessionStore getSession(String clientIdentifier) {
        return ContextManager.getSessionStore(clientIdentifier);
    }

    @Override
    public void removeSession(String clientIdentifier){
        ContextManager.clearSessionStore(clientIdentifier);
    }


    //dupPublish Message

    @Override
    public void putDupPublishMessage(String clientId, DupPublishMessageStore dupPublishMessageStore) {
        ContextManager.putDupPublishMessage(clientId,dupPublishMessageStore);
    }

    @Override
    public List<DupPublishMessageStore> getDupPublishMessage(String clientIdentifier) {
        return ContextManager.getDupPublishMessage(clientIdentifier);
    }

    @Override
    public void removeDupPublishMessageByClient(String clientIdentifier) {
        ContextManager.removeDupPublishMessage(clientIdentifier);
    }

    @Override
    public void removeDupPublishMessage(String clientIdentifier, int messageId) {
        ContextManager.removeDupPublishMessage(clientIdentifier, messageId);
    }




    //dupPubRel Message

    @Override
    public void putDupPubRelMessage(String clientId, DupPubRelMessageStore dupPubRelMessageStore) {
        ContextManager.putDupPubRelMessage(clientId,dupPubRelMessageStore);
    }

    @Override
    public List<DupPubRelMessageStore> getDupPubRelMessage(String clientIdentifier) {
        return ContextManager.getDupPubRelMessage(clientIdentifier);
    }


    @Override
    public void removeDupPubRelMessageByClient(String clientIdentifier) {
        ContextManager.removeDupPubRelMessage(clientIdentifier);
    }

    @Override
    public void removeDupPubRelMessage(String clientId, int messageId) {
        ContextManager.removeDupPubRelMessage(clientId, messageId);
    }



    //subscribe Message

    @Override
    public void putSubscribeMessage(String topicFilter, SubscribeStore subscribeStore) {
        ContextManager.putSubscribeMessage(topicFilter, subscribeStore);
    }

    @Override
    public void removeSubscribeMessage(String topicFilter, String clientId) {
        ContextManager.removeSubscribeMessage(topicFilter, clientId);
    }

    @Override
    public void removeSubscribeByClient(String clientIdentifier) {
        ContextManager.removeSubscribeMessage(clientIdentifier);
    }

    @Override
    public List<SubscribeStore> searchSubscribe(String topic) {
        return ContextManager.searchSubscribeMessage(topic);
    }



    //retain Message

    @Override
    public void putRetainMessage(String topicName, RetainMessageStore retainMessageStore) {
        ContextManager.putRetainMessage(topicName, retainMessageStore);
    }

    @Override
    public List<RetainMessageStore> searchRetainMessage(String topicFilter) {
        return ContextManager.searchRetainMessage(topicFilter);
    }

    @Override
    public void removeRetainMessage(String topicName) {
        ContextManager.removeRetainMessage(topicName);
    }

    @Override
    public int getNextMessageId(String clientIdentifier) {
        return SnUtil.getSn(clientIdentifier);
    }
}
