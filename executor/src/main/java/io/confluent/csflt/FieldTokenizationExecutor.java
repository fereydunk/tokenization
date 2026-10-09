package io.confluent.csflt;

import io.confluent.kafka.schemaregistry.client.rest.entities.RuleMode;
import io.confluent.kafka.schemaregistry.rules.FieldRuleExecutor;
import io.confluent.kafka.schemaregistry.rules.FieldTransform;
import io.confluent.kafka.schemaregistry.rules.RuleContext;
import io.confluent.kafka.schemaregistry.rules.RuleException;

import java.math.BigInteger;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Client-Side Field Level Tokenization (CSFLT) rule executor.
 *
 * <p>This is the tokenization counterpart to Confluent's built-in CSFLE
 * {@code FieldEncryptionExecutor}. It is a new {@code TRANSFORM} rule of type
 * {@code TOKENIZE} in the existing Schema Registry data-contract rules
 * framework — same place in the contract, same tag-scoping, same client-side
 * execution mechanism as {@code ENCRYPT}. The only substantive differences:
 *
 * <ul>
 *   <li>it calls an external <b>vault</b> (via {@link VaultClient}) instead of
 *       doing crypto locally — Confluent never generates or stores tokens; and
 *   <li>it <b>preserves the field's type and length</b> (an integer tokenizes to
 *       an integer of the same digit length), which is exactly what CSFLE's
 *       byte/base64 encryption cannot do and is the headline reason CSFLT exists.
 * </ul>
 *
 * <p>The base {@link FieldRuleExecutor} handles tag matching and field walking;
 * this class only supplies {@link #type()} and the per-field {@link FieldTransform}.
 *
 * <p>Rule params (from the schema's {@code ruleSet.domainRules[].params}):
 * <pre>
 *   tokenize.vault.type     e.g. "mock-vault"            (selects the VaultClient impl)
 *   tokenize.vault.url      e.g. "http://localhost:9099"
 *   tokenize.token.space    e.g. "pii-ssn"               (per-tag namespace in the vault)
 *   tokenize.deterministic  "true" | "false"
 * </pre>
 */
public class FieldTokenizationExecutor extends FieldRuleExecutor {

  public static final String TYPE = "TOKENIZE";

  static final String PARAM_VAULT_TYPE = "tokenize.vault.type";
  static final String PARAM_VAULT_URL = "tokenize.vault.url";
  static final String PARAM_TOKEN_SPACE = "tokenize.token.space";
  static final String PARAM_DETERMINISTIC = "tokenize.deterministic";

  /** Cache one VaultClient per (type|url) so we don't rebuild it per message. */
  private final ConcurrentHashMap<String, VaultClient> clients = new ConcurrentHashMap<>();

  @Override
  public String type() {
    return TYPE;
  }

  @Override
  public FieldTransform newTransform(RuleContext ctx) throws RuleException {
    String vaultType = require(ctx, PARAM_VAULT_TYPE);
    String vaultUrl = require(ctx, PARAM_VAULT_URL);
    String tokenSpace = require(ctx, PARAM_TOKEN_SPACE);
    boolean deterministic = Boolean.parseBoolean(ctx.getParameter(PARAM_DETERMINISTIC));

    VaultClient vault = clients.computeIfAbsent(
        vaultType + "|" + vaultUrl, k -> newVaultClient(vaultType, vaultUrl));

    // The functional FieldTransform: called once per matching tagged field.
    return (RuleContext c, RuleContext.FieldContext fieldCtx, Object fieldValue) -> {
      if (fieldValue == null) {
        return null;
      }
      String fieldType = fieldCtx.getType().name();  // INT, LONG, STRING, …
      try {
        if (c.ruleMode() == RuleMode.WRITE) {
          String token = vault.tokenize(tokenSpace, String.valueOf(fieldValue),
              fieldType, deterministic);
          return reconstruct(token, fieldValue);     // keep original Java type
        } else if (c.ruleMode() == RuleMode.READ) {
          String value = vault.detokenize(tokenSpace, String.valueOf(fieldValue));
          return reconstruct(value, fieldValue);
        } else {
          throw new RuleException("unsupported rule mode: " + c.ruleMode());
        }
      } catch (RuleException re) {
        throw re;
      } catch (Exception e) {
        // Wrapped so the framework can apply the rule's onFailure policy.
        throw new RuleException(c.rule(), e);
      }
    };
  }

  /**
   * Return {@code raw} as the same Java type as {@code original}, so a tokenized
   * INT field stays an INT in the record (type/length preservation). The vault
   * guarantees a token of the same digit length, so no overflow vs. the original.
   */
  static Object reconstruct(String raw, Object original) {
    if (original instanceof Integer) {
      return Integer.valueOf(raw);
    }
    if (original instanceof Long) {
      return Long.valueOf(raw);
    }
    if (original instanceof Short) {
      return Short.valueOf(raw);
    }
    if (original instanceof BigInteger) {
      return new BigInteger(raw);
    }
    // STRING and anything else: pass the surrogate through unchanged.
    return raw;
  }

  private VaultClient newVaultClient(String vaultType, String vaultUrl) {
    if (MockVaultClient.TYPE.equals(vaultType)) {
      return new MockVaultClient(vaultUrl);
    }
    // The extension point: real Protegrity / HashiCorp Vault clients would be
    // selected here by their own tokenize.vault.type value.
    throw new IllegalArgumentException(
        "no VaultClient registered for tokenize.vault.type=" + vaultType);
  }

  private static String require(RuleContext ctx, String name) throws RuleException {
    String v = ctx.getParameter(name);
    if (v == null || v.isEmpty()) {
      throw new RuleException(ctx.rule(), "missing required tokenization rule param: " + name);
    }
    return v;
  }

  @Override
  public void close() throws RuleException {
    clients.values().forEach(c -> {
      try {
        c.close();
      } catch (Exception ignored) {
        // best-effort cleanup
      }
    });
    clients.clear();
  }
}
