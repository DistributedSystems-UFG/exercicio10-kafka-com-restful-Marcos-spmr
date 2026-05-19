import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpExchange;
import org.apache.kafka.clients.consumer.*;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;

public class StorageWebService_REST {

    private static final List<double[]> database = new CopyOnWriteArrayList<>();

    public static void main(String[] args) throws IOException, InterruptedException {

        Thread kafkaThread = new Thread(StorageWebService_REST::consumeKafkaEvents);
        kafkaThread.setDaemon(true);
        kafkaThread.start();

        HttpServer server = HttpServer.create(new InetSocketAddress(8080), 0);
        server.setExecutor(Executors.newFixedThreadPool(4));

        server.createContext("/sensor/latest", exchange -> {
            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendResponse(exchange, 405, "{\"error\":\"Method Not Allowed\"}");
                return;
            }
            String json;
            if (database.isEmpty()) {
                json = "{\"temperature\":0.0,\"timestamp\":0}";
            } else {
                double[] last = database.get(database.size() - 1);
                json = String.format("{\"temperature\":%.4f,\"timestamp\":%d}",
                        last[0], (long) last[1]);
            }
            sendResponse(exchange, 200, json);
        });

        server.createContext("/sensor/history", exchange -> {
            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendResponse(exchange, 405, "{\"error\":\"Method Not Allowed\"}");
                return;
            }
            StringBuilder sb = new StringBuilder("{\"records\":[");
            for (int i = 0; i < database.size(); i++) {
                double[] entry = database.get(i);
                if (i > 0) sb.append(",");
                sb.append(String.format("{\"temperature\":%.4f,\"timestamp\":%d}",
                        entry[0], (long) entry[1]));
            }
            sb.append("]}");
            sendResponse(exchange, 200, sb.toString());
        });

        server.start();
        System.out.println("Servidor REST iniciado na porta 8080...");
        System.out.println("  GET http://localhost:8080/sensor/latest");
        System.out.println("  GET http://localhost:8080/sensor/history");

        Thread.currentThread().join();
    }

    private static void consumeKafkaEvents() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "grupo-storage-rest");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.DoubleDeserializer");

        KafkaConsumer<String, Double> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("sensor-temperatura-processada"));

        while (true) {
            ConsumerRecords<String, Double> records =
                    consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, Double> record : records) {
                double[] entry = {record.value(), System.currentTimeMillis()};
                database.add(entry);
                System.out.printf("Dado persistido: %.2f°C%n", record.value());
            }
        }
    }

    private static void sendResponse(HttpExchange exchange, int status, String body)
            throws IOException {
        byte[] bytes = body.getBytes("UTF-8");
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=UTF-8");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }
}
