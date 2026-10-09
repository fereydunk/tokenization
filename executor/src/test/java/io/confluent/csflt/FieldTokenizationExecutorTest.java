package io.confluent.csflt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.math.BigInteger;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests the type/length-preservation logic (pure, always runs) and a real
 * tokenize -> detokenize round trip against the Python mock vault started as a
 * subprocess (skipped gracefully if python3 or the vault is unavailable, so the
 * build never hard-fails on environment).
 */
class FieldTokenizationExecutorTest {

  // ---- pure type-preservation logic (the headline CSFLT property) ----------

  @Test
  void reconstructPreservesIntegerType() {
    Object out = FieldTokenizationExecutor.reconstruct("123456789", 987654321);
    assertInstanceOf(Integer.class, out);
    assertEquals(123456789, out);
  }

  @Test
  void reconstructPreservesLongType() {
    Object out = FieldTokenizationExecutor.reconstruct("4111111111111111", 4222222222222222L);
    assertInstanceOf(Long.class, out);
    assertEquals(4111111111111111L, out);
  }

  @Test
  void reconstructPreservesBigInteger() {
    Object out = FieldTokenizationExecutor.reconstruct("12345", new BigInteger("99999"));
    assertInstanceOf(BigInteger.class, out);
  }

  @Test
  void reconstructPassesStringThrough() {
    Object out = FieldTokenizationExecutor.reconstruct("987-65-4321", "123-45-6789");
    assertInstanceOf(String.class, out);
    assertEquals("987-65-4321", out);
  }

  // ---- real round trip against the mock vault ------------------------------

  private static Process vault;
  private static String baseUrl;
  private static String apiKey;
  private static final HttpClient HTTP = HttpClient.newHttpClient();

  @BeforeAll
  static void startVault() throws Exception {
    Path serverPy = Path.of("..", "mock-vault", "server.py").toAbsolutePath().normalize();
    if (!Files.exists(serverPy)) {
      return; // leaves baseUrl null -> integration tests skip
    }
    int port = 9399;
    Path dbDir = Files.createTempDirectory("csflt-vault-junit");
    ProcessBuilder pb = new ProcessBuilder("python3", serverPy.toString());
    pb.environment().put("VAULT_PORT", String.valueOf(port));
    pb.environment().put("VAULT_DB_PATH", dbDir.resolve("vault.db").toString());
    pb.redirectErrorStream(true);
    pb.redirectOutput(ProcessBuilder.Redirect.DISCARD);
    try {
      vault = pb.start();
    } catch (Exception e) {
      return; // no python3 -> skip
    }
    baseUrl = "http://127.0.0.1:" + port;

    // wait for health
    boolean up = false;
    for (int i = 0; i < 50; i++) {
      try {
        HttpResponse<String> r = HTTP.send(
            HttpRequest.newBuilder(URI.create(baseUrl + "/health")).GET().build(),
            HttpResponse.BodyHandlers.ofString());
        if (r.statusCode() == 200) { up = true; break; }
      } catch (Exception ignored) { }
      Thread.sleep(100);
    }
    if (!up) { baseUrl = null; return; }

    // create a token space + a full-access key
    post("/token-spaces", "{\"name\":\"junit-space\"}", null);
    String keyResp = post("/keys",
        "{\"can_tokenize\":true,\"can_detokenize\":true}", null);
    apiKey = keyResp.replaceAll(".*\"api_key\"\\s*:\\s*\"([^\"]+)\".*", "$1");
  }

  @AfterAll
  static void stopVault() {
    if (vault != null) {
      vault.destroy();
    }
  }

  private static String post(String path, String body, String key) throws Exception {
    HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(baseUrl + path))
        .timeout(Duration.ofSeconds(5))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(body));
    if (key != null) {
      b.header("X-Vault-Key", key);
    }
    return HTTP.send(b.build(), HttpResponse.BodyHandlers.ofString()).body();
  }

  @Test
  void intRoundTripPreservesTypeAndLength() throws Exception {
    assumeTrue(baseUrl != null && apiKey != null, "mock vault not available");
    MockVaultClient vaultClient = new MockVaultClient(baseUrl, apiKey);

    Long original = 123456789L;
    String token = vaultClient.tokenize("junit-space", String.valueOf(original), "LONG", true);
    assertEquals(String.valueOf(original).length(), token.length(), "same digit length");
    assertTrue(token.chars().allMatch(Character::isDigit), "token is all digits");
    assertNotEquals(String.valueOf(original), token, "value actually replaced");

    // type preserved when reconstructed into the record
    Object inRecord = FieldTokenizationExecutor.reconstruct(token, original);
    assertInstanceOf(Long.class, inRecord);

    // and it resolves back to the original
    String back = vaultClient.detokenize("junit-space", token);
    assertEquals(String.valueOf(original), back);
  }

  @Test
  void unauthorizedDetokenizeFails() throws Exception {
    assumeTrue(baseUrl != null && apiKey != null, "mock vault not available");
    // mint a tokenize-only key (no detokenize)
    String keyResp = post("/keys",
        "{\"can_tokenize\":true,\"can_detokenize\":false}", null);
    String tokOnly = keyResp.replaceAll(".*\"api_key\"\\s*:\\s*\"([^\"]+)\".*", "$1");

    MockVaultClient full = new MockVaultClient(baseUrl, apiKey);
    String token = full.tokenize("junit-space", "555", "INT", true);

    MockVaultClient restricted = new MockVaultClient(baseUrl, tokOnly);
    boolean threw = false;
    try {
      restricted.detokenize("junit-space", token);
    } catch (Exception e) {
      threw = true;
      assertTrue(e.getMessage().contains("403"), "should be a 403 from the vault");
    }
    assertTrue(threw, "unauthorized detokenize must throw");
  }
}
