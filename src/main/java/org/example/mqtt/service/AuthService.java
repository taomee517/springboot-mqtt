package org.example.mqtt.service;

import org.apache.commons.lang3.StringUtils;
import org.example.mqtt.config.MqttProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class AuthService implements IAuthService {

    @Autowired
    MqttProperties mqttProperties;

    @Override
    public boolean checkValid(String user, String pwd) {
        Boolean authCheckEnable = mqttProperties.getAuthCheckEnable();
        if (!authCheckEnable) return true;
        if(StringUtils.isEmpty(user)) return false;
        if(StringUtils.isEmpty(pwd)) return false;
        String authUser = mqttProperties.getUsername();
        String authPwd = mqttProperties.getPassword();
        return StringUtils.equalsIgnoreCase(user, authUser) && StringUtils.equalsIgnoreCase(pwd, authPwd);
    }
}
