package io.confluent.csflt;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * {@link VaultClient} that talks to the demo mock vault over HTTP using the
 * JDK's built-in {@link HttpClient} (no third-party HTTP dependency).
 *
 * <p>The vault API key is read from the {@code VAULT_API_KEY} environment
 * variable, deliberately NOT from the schema rule params or Kafka client config
 * — the same way CSFLE resolves KMS credentials from the environment rather than
 * baking them into the schema. Swapping that env var (authorized vs. tokenize-only
 * key) is what drives the authorized-vs-unauthorized consumer demo, with no
 * change to the schema or the rule.
 */
public class MockVaultClient implements VaultClient {

  public static final String TYPE = "mock-vault";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final String baseUrl;
  private final String apiKey;
  private final HttpClient http;

  public MockVaultClient(String baseUrl) {
    this(baseUrl, System.getenv().getOrDefault("VAULT_API_KEY", ""));
  }

  /** Explicit-key constructor (used by tests; production reads VAULT_API_KEY from env). */
  public MockVaultClient(String baseUrl, String apiKey) {
    // normalize trailing slash
    this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
    this.apiKey = apiKey;
    this.http = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(5))
        .build();
  }

  @Override
  public String type() {
    return TYPE;
  }

  @Override
  public String tokenize(String tokenSpace, String value, String fieldType, boolean deterministic)
      throws Exception {
    ObjectNode body = MAPPER.createObjectNode();
    body.put("token_space", tokenSpace);
    body.put("value", value);
    body.put("field_type", fieldType);
    body.put("deterministic", deterministic);
    JsonNode resp = post("/tokenize", body);
    return resp.get("token").asText();
  }

  @Override
  public String detokenize(String tokenSpace, String token) throws Exception {
    ObjectNode body = MAPPER.createObjectNode();
    body.put("token_space", tokenSpace);
    body.put("token", token);
    JsonNode resp = post("/detokenize", body);
    return resp.get("value").asText();
  }

  private JsonNode post(String path, ObjectNode body) throws Exception {
    HttpRequest req = HttpRequest.newBuilder()
        .uri(URI.create(baseUrl + path))
        .timeout(Duration.ofSeconds(10))
        .header("Content-Type", "application/json")
        .header("X-Vault-Key", apiKey)
        .POST(HttpRequest.BodyPublishers.ofString(MAPPER.writeValueAsString(body)))
        .build();
    HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
    if (resp.statusCode() != 200) {
      // Surface a clear, actionable error. The rule framework will then apply
      // the rule's onFailure policy (ERROR on write, NONE on read -> leaves the
      // token in place for an unauthorized consumer).
      String detail = resp.body();
      throw new RuntimeException(
          "vault " + path + " failed: HTTP " + resp.statusCode() + " " + detail);
    }
    return MAPPER.readTree(resp.body());
  }
}
