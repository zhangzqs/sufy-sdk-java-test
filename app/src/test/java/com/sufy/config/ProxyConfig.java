package com.sufy.config;

public class ProxyConfig {
    public boolean enable;
    public String host;
    public int port;
    public String type;

    public boolean isEnable() {
        return enable;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getType() {
        return type;
    }
}