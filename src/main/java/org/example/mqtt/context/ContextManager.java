package org.example.mqtt.context;

import io.netty.channel.Channel;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.example.mqtt.context.mqtt.*;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author 罗涛
 * @title ContextManager
 * @date 2020/6/22 15:11
 */
@Slf4j
public class ContextManager {
    public static AttributeKey<String> MQTT_CLIENT_ID = AttributeKey.newInstance("mqtt_client_id");
    public static Map<String, SessionStore> sessionStoreMap = new ConcurrentHashMap<>();
    public static Map<String, ConcurrentHashMap<Integer, DupPublishMessageStore>> dupPublishMessageStoreMap = new ConcurrentHashMap<>();
    public static Map<String, ConcurrentHashMap<Integer, DupPubRelMessageStore>> dupPubRelMessageStoreMap = new ConcurrentHashMap<>();
    public static Map<String, RetainMessageStore> retainMessageStoreMap = new ConcurrentHashMap<>();
    public static Map<String, ConcurrentHashMap<String, SubscribeStore>> subscribeNotWildcardMap = new ConcurrentHashMap<>();
    public static Map<String, ConcurrentHashMap<String, SubscribeStore>> subscribeWildcardMap = new ConcurrentHashMap<>();

    public static void putClientId(Channel channel, String clientId){
        channel.attr(MQTT_CLIENT_ID).set(clientId);
    }

    public static String getClientId(Channel channel){
        Attribute<String> attr = channel.attr(MQTT_CLIENT_ID);
        if(Objects.isNull(attr)){
            return null;
        }
        String clientId = channel.attr(MQTT_CLIENT_ID).get();
        return clientId;
    }

    public static void removeClientId(Channel channel){
        channel.attr(MQTT_CLIENT_ID).set(null);
    }

    public static boolean containsSessionStore(String clientIdentifier) {
        return sessionStoreMap.containsKey(clientIdentifier);
    }

    public static SessionStore getSessionStore(String clientIdentifier) {
        return sessionStoreMap.get(clientIdentifier);
    }

    public static void clearSessionStore(String clientIdentifier) {
        sessionStoreMap.remove(clientIdentifier);
    }

    public static void putSessionStore(String clientIdentifier, SessionStore session) {
        sessionStoreMap.put(clientIdentifier, session);
    }

    public static void putDupPublishMessage(String clientIdentifier, DupPublishMessageStore dupPublishMessageStore) {
        ConcurrentHashMap<Integer, DupPublishMessageStore> map = dupPublishMessageStoreMap.containsKey(clientIdentifier) ? dupPublishMessageStoreMap.get(clientIdentifier) : new ConcurrentHashMap<Integer, DupPublishMessageStore>();
        map.put(dupPublishMessageStore.getMessageId(), dupPublishMessageStore);
        dupPublishMessageStoreMap.put(clientIdentifier, map);
    }

    public static List<DupPublishMessageStore> getDupPublishMessage(String clientIdentifier) {
        if (dupPublishMessageStoreMap.containsKey(clientIdentifier)) {
            ConcurrentHashMap<Integer, DupPublishMessageStore> map = dupPublishMessageStoreMap.get(clientIdentifier);
            Collection<DupPublishMessageStore> collection = map.values();
            return new ArrayList<DupPublishMessageStore>(collection);
        }
        return Collections.emptyList();
    }

    public static void removeDupPublishMessage(String clientIdentifier) {
        if (dupPublishMessageStoreMap.containsKey(clientIdentifier)) {
            ConcurrentHashMap<Integer, DupPublishMessageStore> map = dupPublishMessageStoreMap.get(clientIdentifier);
//            map.forEach((messageId, dupPublishMessageStore) -> {
//                messageIdService.releaseMessageId(messageId);  //todo
//            });
            map.clear();
            dupPublishMessageStoreMap.remove(clientIdentifier);
        }
    }

    public static void removeDupPublishMessage(String clientIdentifier,int messageId) {
        if (dupPublishMessageStoreMap.containsKey(clientIdentifier)) {
            ConcurrentHashMap<Integer, DupPublishMessageStore> map = dupPublishMessageStoreMap.get(clientIdentifier);
            if (map.containsKey(messageId)) {
                map.remove(messageId);
                if (map.size() > 0) {
                    dupPublishMessageStoreMap.put(clientIdentifier, map);
                } else {
                    dupPublishMessageStoreMap.remove(clientIdentifier);
                }
            }
        }

    }

    public static void putDupPubRelMessage(String clientIdentifier, DupPubRelMessageStore dupPubRelMessageStore) {
        ConcurrentHashMap<Integer, DupPubRelMessageStore> map = dupPubRelMessageStoreMap.containsKey(clientIdentifier) ? dupPubRelMessageStoreMap.get(clientIdentifier) : new ConcurrentHashMap<Integer, DupPubRelMessageStore>();
        map.put(dupPubRelMessageStore.getMessageId(), dupPubRelMessageStore);
        dupPubRelMessageStoreMap.put(clientIdentifier, map);
    }


    public static List<DupPubRelMessageStore> getDupPubRelMessage(String clientIdentifier) {
        if (dupPubRelMessageStoreMap.containsKey(clientIdentifier)) {
            ConcurrentHashMap<Integer, DupPubRelMessageStore> map = dupPubRelMessageStoreMap.get(clientIdentifier);
            Collection<DupPubRelMessageStore> collection = map.values();
            return new ArrayList<DupPubRelMessageStore>(collection);
        }
        return Collections.emptyList();
    }

    public static void removeDupPubRelMessage(String clientId, int messageId) {
        if (dupPubRelMessageStoreMap.containsKey(clientId)) {
            ConcurrentHashMap<Integer, DupPubRelMessageStore> map = dupPubRelMessageStoreMap.get(clientId);
            if (map.containsKey(messageId)) {
                map.remove(messageId);
                if (map.size() > 0) {
                    dupPubRelMessageStoreMap.put(clientId, map);
                } else {
                    dupPubRelMessageStoreMap.remove(clientId);
                }
            }
        }
    }

    public static void removeDupPubRelMessage(String clientIdentifier) {
        if (dupPubRelMessageStoreMap.containsKey(clientIdentifier)) {
            ConcurrentHashMap<Integer, DupPubRelMessageStore> map = dupPubRelMessageStoreMap.get(clientIdentifier);
//            map.forEach((messageId, dupPubRelMessageStore) -> {
//                messageIdService.releaseMessageId(messageId);
//            });
            map.clear();
            dupPubRelMessageStoreMap.remove(clientIdentifier);
        }
    }


    public static void putSubscribeMessage(String topicFilter, SubscribeStore subscribeStore) {
        //含通配符的topic
        if (StringUtils.contains(topicFilter, '#') || StringUtils.contains(topicFilter, '+')) {
            ConcurrentHashMap<String, SubscribeStore> map =
                    subscribeWildcardMap.containsKey(topicFilter) ? subscribeWildcardMap.get(topicFilter) : new ConcurrentHashMap<String, SubscribeStore>();
            map.put(subscribeStore.getClientId(), subscribeStore);
            subscribeWildcardMap.put(topicFilter, map);
        } else {
            ConcurrentHashMap<String, SubscribeStore> map =
                    subscribeNotWildcardMap.containsKey(topicFilter) ? subscribeNotWildcardMap.get(topicFilter) : new ConcurrentHashMap<String, SubscribeStore>();
            map.put(subscribeStore.getClientId(), subscribeStore);
            subscribeNotWildcardMap.put(topicFilter, map);
        }
    }

    public static void removeSubscribeMessage(String topicFilter, String clientId) {
        if (StringUtils.contains(topicFilter, '#') || StringUtils.contains(topicFilter, '+')) {
            if (subscribeWildcardMap.containsKey(topicFilter)) {
                ConcurrentHashMap<String, SubscribeStore> map = subscribeWildcardMap.get(topicFilter);
                if (map.containsKey(clientId)) {
                    map.remove(clientId);
                    if (map.size() > 0) {
                        subscribeWildcardMap.put(topicFilter, map);
                    } else {
                        subscribeWildcardMap.remove(topicFilter);
                    }
                }
            }
        } else {
            if (subscribeNotWildcardMap.containsKey(topicFilter)) {
                ConcurrentHashMap<String, SubscribeStore> map = subscribeNotWildcardMap.get(topicFilter);
                if (map.containsKey(clientId)) {
                    map.remove(clientId);
                    if (map.size() > 0) {
                        subscribeNotWildcardMap.put(topicFilter, map);
                    } else {
                        subscribeNotWildcardMap.remove(topicFilter);
                    }
                }
            }
        }
    }

    public static void removeSubscribeMessage(String clientId) {
        Set<String> topics = subscribeNotWildcardMap.keySet();
        if(!CollectionUtils.isEmpty(topics)){
            for(String topic:topics){
                ConcurrentHashMap<String, SubscribeStore> map = subscribeNotWildcardMap.get(topic);
                if (map.containsKey(clientId)) {
                    map.remove(clientId);
                    if (map.size() > 0) {
                        subscribeNotWildcardMap.put(topic, map);
                    } else {
                        subscribeNotWildcardMap.remove(topic);
                    }
                }
            }
        }

        Set<String> wildcardTopics = subscribeWildcardMap.keySet();
        if(!CollectionUtils.isEmpty(wildcardTopics)){
            for(String topic : wildcardTopics){
                ConcurrentHashMap<String, SubscribeStore> map = subscribeWildcardMap.get(topic);
                if (map.containsKey(clientId)) {
                    map.remove(clientId);
                    if (map.size() > 0) {
                        subscribeWildcardMap.put(topic, map);
                    } else {
                        subscribeWildcardMap.remove(topic);
                    }
                }
            }
        }
    }


    public static List<SubscribeStore> searchSubscribeMessage(String topic) {
        List<SubscribeStore> subscribeStores = new ArrayList<SubscribeStore>();
        if (subscribeNotWildcardMap.containsKey(topic)) {
            ConcurrentHashMap<String, SubscribeStore> map = subscribeNotWildcardMap.get(topic);
            Collection<SubscribeStore> collection = map.values();
            List<SubscribeStore> list = new ArrayList<SubscribeStore>(collection);
            subscribeStores.addAll(list);
        }
        //含通配符的topic
        subscribeWildcardMap.keySet().forEach(key -> {
            ConcurrentHashMap<String, SubscribeStore> entry = subscribeWildcardMap.get(key);
            if (StringUtils.split(topic, '/').length >= StringUtils.split(key, '/').length) {
                List<String> splitTopics = Arrays.asList(StringUtils.split(topic, '/'));
                List<String> splitTopicFilters = Arrays.asList(StringUtils.split(key, '/'));
                String newTopicFilter = "";
                for (int i = 0; i < splitTopicFilters.size(); i++) {
                    String value = splitTopicFilters.get(i);
                    if (value.equals("+")) {
                        newTopicFilter = newTopicFilter + "+/";
                    } else if (value.equals("#")) {
                        newTopicFilter = newTopicFilter + "#/";
                        break;
                    } else {
                        newTopicFilter = newTopicFilter + splitTopics.get(i) + "/";
                    }
                }
                newTopicFilter = StringUtils.removeEnd(newTopicFilter, "/");
                if (key.equals(newTopicFilter)) {
                    Collection<SubscribeStore> collection = entry.values();
                    List<SubscribeStore> list = new ArrayList<SubscribeStore>(collection);
                    subscribeStores.addAll(list);
                }
            }
        });
        return subscribeStores;
    }

    public static void putRetainMessage(String topic, RetainMessageStore retainMessageStore) {
        retainMessageStoreMap.put(topic, retainMessageStore);
    }

    public static RetainMessageStore getRetainMessage(String topic) {
        return retainMessageStoreMap.get(topic);
    }

    public static void removeRetainMessage(String topic) {
        retainMessageStoreMap.remove(topic);
    }

    public static boolean containsRetainMessageKey(String topic) {
        return retainMessageStoreMap.containsKey(topic);
    }

    public static List<RetainMessageStore> searchRetainMessage(String topicFilter) {
        List<RetainMessageStore> retainMessageStores = new ArrayList<RetainMessageStore>();
        if (!StringUtils.contains(topicFilter, '#') && !StringUtils.contains(topicFilter, '+')) {
            if (retainMessageStoreMap.containsKey(topicFilter)) {
                retainMessageStores.add(retainMessageStoreMap.get(topicFilter));
            }
        }else {
            retainMessageStoreMap.keySet().forEach(topic -> {
                RetainMessageStore retainMessageStore = retainMessageStoreMap.get(topic);
                if (StringUtils.split(topic, '/').length >= StringUtils.split(topicFilter, '/').length) {
                    List<String> splitTopics = Arrays.asList(StringUtils.split(topic, '/'));
                    List<String> splitTopicFilters = Arrays.asList(StringUtils.split(topicFilter, '/'));
                    String newTopicFilter = "";
                    for (int i = 0; i < splitTopicFilters.size(); i++) {
                        String value = splitTopicFilters.get(i);
                        if (value.equals("+")) {
                            newTopicFilter = newTopicFilter + "+/";
                        } else if (value.equals("#")) {
                            newTopicFilter = newTopicFilter + "#/";
                            break;
                        } else {
                            newTopicFilter = newTopicFilter + splitTopics.get(i) + "/";
                        }
                    }
                    newTopicFilter = StringUtils.removeEnd(newTopicFilter, "/");
                    if (topicFilter.equals(newTopicFilter)) {
                        retainMessageStores.add(retainMessageStore);
                    }
                }
            });
        }
        return retainMessageStores;
    }

}
