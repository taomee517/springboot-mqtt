package org.example.mqtt.service;

public interface IAuthService {
    boolean checkValid(String user, String pwd);
}
