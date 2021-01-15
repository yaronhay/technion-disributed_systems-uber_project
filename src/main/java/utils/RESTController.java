package utils;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

// https://medium.com/consulner/framework-less-rest-api-in-java-dd22d4d642fa
public class RESTController {
    static final Logger log = LogManager.getLogger();

    @Retention(RetentionPolicy.RUNTIME)
    public @interface RestAPI {
        String Method() default "GET";
        String Context();
        boolean hasJSONRequest() default true;
    }

    public static class Response {
        public int httpCode;
        public JSONObject body;
    }

    final HttpServer httpServer;

    class HandleRequest {
        final HttpExchange exchange;
        final Map<String, Pair<Method, RestAPI>> operations;

        public HandleRequest(HttpExchange exchange, Map<String, Pair<Method, RestAPI>> operations) {
            this.exchange = exchange;
            this.operations = operations;
        }

        void respond(int code, String res) {
            try {
                byte[] resp = res.getBytes();
                exchange.sendResponseHeaders(code, resp.length + 1);

                try (var os = exchange.getResponseBody()) {
                    os.write(resp);
                    os.write('\n');
                }

            } catch (IOException e) {
                log.error("IO Exception when writing response", e);
            } finally {
                exchange.close();
            }
        }

        JSONObject parseJSONRequest() {
            try {

                var requestBytes = exchange.getRequestBody().readAllBytes();
                var requestString = new String(requestBytes);
                return new JSONObject(requestString);

            } catch (JSONException e) {
                log.error("Can't parse JSON", e);
                respond(400, "Can't parse JSON");
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }

        void handle() {
            var operation = operations.get(this.exchange.getRequestMethod());
            if (operation == null) {
                log.error("Invalid operation used on resource {} with method {}",
                        this.exchange.getHttpContext(),
                        this.exchange.getRequestMethod());
                respond(404, "No such method");
                return;
            }

            var method = operation.getValue0();
            var restAPI = operation.getValue1();

            JSONObject reqBody = null;
            if (restAPI.hasJSONRequest()) {
                reqBody = this.parseJSONRequest();
            }

            Response resp = new Response();
            resp.body = new JSONObject();

            try {
                method.invoke(RESTController.this, reqBody, resp);
            } catch (IllegalAccessException | InvocationTargetException e) {
                e.printStackTrace();
            }
            respond(resp.httpCode, resp.body.toString(2));
        }

    }

    public RESTController(int port) throws IOException {
        this.httpServer = HttpServer.create(new InetSocketAddress(port), 0);
        this.httpServer.setExecutor(null);
    }

    final Map<String, Map<String, Pair<Method, RestAPI>>> getRESTMethods() {
        Map<String, Map<String, Pair<Method, RestAPI>>> methods = new ConcurrentHashMap<>();

        for (Method m : this.getClass().getDeclaredMethods()) {
            var m1 = m.getDeclaredAnnotations();
            if (m.isAnnotationPresent(RestAPI.class)) {
                RestAPI restAPI = m.getAnnotation(RestAPI.class);
                var map = methods.computeIfAbsent(restAPI.Context(), k -> new ConcurrentHashMap<>());
                map.put(restAPI.Method(), Pair.with(m, restAPI));
            }
        }

        return methods;
    }

    public final void start() {
        this.registerRESTMethods();
        this.httpServer.start();
    }

    final void registerRESTMethods() {
        var methods = this.getRESTMethods();

        for (var context : methods.keySet()) {
            var operations = methods.get(context);

            this.httpServer.createContext(context, exchange -> {
                var handler = new HandleRequest(exchange, operations);
                handler.handle();
            });
        }
    }

}
