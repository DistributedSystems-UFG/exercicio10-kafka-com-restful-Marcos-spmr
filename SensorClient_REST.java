import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class SensorClient_REST {

    private static final String BASE_URL = "http://localhost:8080";

    public static void main(String[] args) throws Exception {
        HttpClient client = HttpClient.newHttpClient();

        System.out.println("--- Consultando Última Média ---");
        String latestJson = get(client, BASE_URL + "/sensor/latest");
        System.out.println("Resposta JSON: " + latestJson);

        double temperature = parseDouble(latestJson, "temperature");
        long timestamp = parseLong(latestJson, "timestamp");
        System.out.printf("Temperatura: %.2f°C | Timestamp: %d%n", temperature, timestamp);

        System.out.println("\n--- Consultando Histórico ---");
        String historyJson = get(client, BASE_URL + "/sensor/history");
        System.out.println("Resposta JSON bruta: " + historyJson);

        String recordsContent = historyJson
                .replaceFirst(".*\"records\":\\[", "")
                .replaceFirst("\\]\\}\\s*$", "");

        if (!recordsContent.isBlank() && !recordsContent.equals("{}")) {
            String[] records = recordsContent.split("\\},\\{");
            for (String record : records) {
                record = record.replace("{", "").replace("}", "");
                double t  = parseDouble("{" + record + "}", "temperature");
                long   ts = parseLong("{" + record + "}", "timestamp");
                System.out.printf("- %.2f°C em %d%n", t, ts);
            }
        } else {
            System.out.println("(histórico vazio)");
        }
    }

    private static String get(HttpClient client, String url) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .GET()
                .header("Accept", "application/json")
                .build();
        HttpResponse<String> response =
                client.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new RuntimeException("HTTP " + response.statusCode() + ": " + response.body());
        }
        return response.body();
    }

    private static double parseDouble(String json, String key) {
        String pattern = "\"" + key + "\":";
        int idx = json.indexOf(pattern);
        if (idx < 0) return 0.0;
        String rest = json.substring(idx + pattern.length()).trim();
        int end = rest.indexOf(',');
        if (end < 0) end = rest.indexOf('}');
        return Double.parseDouble(rest.substring(0, end).trim());
    }

    private static long parseLong(String json, String key) {
        String pattern = "\"" + key + "\":";
        int idx = json.indexOf(pattern);
        if (idx < 0) return 0L;
        String rest = json.substring(idx + pattern.length()).trim();
        int end = rest.indexOf(',');
        if (end < 0) end = rest.indexOf('}');
        return Long.parseLong(rest.substring(0, end).trim());
    }
}
