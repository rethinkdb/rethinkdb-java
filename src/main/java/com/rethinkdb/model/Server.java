package com.rethinkdb.model;

/**
 * The server information.
 */
public class Server {
    private String id;
    private String name;
    private boolean proxy;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isProxy() {
        return proxy;
    }

    public void setProxy(boolean proxy) {
        this.proxy = proxy;
    }

    @Override
    public String toString() {
        return "Server{" +
            "id='" + id + '\'' +
            ", name='" + name + '\'' +
            ", proxy=" + proxy +
            '}';
    }
}
