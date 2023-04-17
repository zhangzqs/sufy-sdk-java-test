package sufy.config;

import com.google.gson.Gson;

import java.io.IOException;
import java.io.InputStream;

public class TestConfig {
    public ProxyConfig proxy;
    public ObjectConfig object;


    private static TestConfig instance;

    public static TestConfig load() throws IOException {
        if (instance != null) {
            return instance;
        }
        try (InputStream is = TestConfig.class.getResourceAsStream("/test-config.json")) {
            Gson gson = new Gson();
            assert is != null;
            instance = gson.fromJson(new String(is.readAllBytes()), TestConfig.class);
            return instance;
        }
    }
}
