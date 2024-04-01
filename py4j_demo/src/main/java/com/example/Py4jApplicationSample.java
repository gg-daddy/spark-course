package py4j_demo.src.main.java.com.example;

import py4j.GatewayServer;

public class Py4jApplicationSample {
    public int addition(int first, int second) {
        StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();
        for (StackTraceElement element : stackTraceElements) {
            System.out.println(element);
        }
        return first + second;
    }

    public static void main(String[] args) {
        Py4jApplicationSample app = new Py4jApplicationSample();
        // app is now the gateway.entry_point
        GatewayServer server = new GatewayServer(app);
        server.start();
    }
}