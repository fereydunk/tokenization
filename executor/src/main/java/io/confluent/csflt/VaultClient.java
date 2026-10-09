package io.confluent.csflt;

/**
 * Service Provider Interface for a tokenization vault.
 *
 * <p>This is the pluggability point called for in the CSFLT product requirements
 * ("a well-defined SPI so customers can bring their own tokenization platform").
 * Confluent never generates or stores tokens itself — it only calls out to an
 * implementation of this interface, exactly as CSFLE calls out to a KMS driver.
 *
 * <p>The one implementation shipped in this demo is {@link MockVaultClient}.
 * Real Protegrity / HashiCorp Vault clients would be additional implementations
 * selected by the {@code tokenize.vault.type} rule parameter.
 */
public interface VaultClient extends AutoCloseable {

  /** The {@code tokenize.vault.type} value this client handles (e.g. "mock-vault"). */
  String type();

  /**
   * Replace a sensitive value with a vault-issued token, preserving the field's
   * type/length (the core CSFLT property that encryption cannot provide).
   *
   * @param tokenSpace    logical namespace the token lives in (per-tag scope)
   * @param value         the plaintext value, rendered as a string
   * @param fieldType     the Schema Registry field type name (INT, LONG, STRING, …)
   * @param deterministic if true, the same value always maps to the same token
   * @return the surrogate token, as a string
   */
  String tokenize(String tokenSpace, String value, String fieldType, boolean deterministic)
      throws Exception;

  /**
   * Resolve a token back to its original value. Throws if the caller is not
   * authorized to detokenize in this space (surfaced to the rule framework,
   * which applies the rule's onFailure policy).
   */
  String detokenize(String tokenSpace, String token) throws Exception;

  @Override
  default void close() {
    // no-op by default
  }
}
