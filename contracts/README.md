# Event contracts

`transaction.v1.schema.json` and `fraud-label.v1.schema.json` are the
authoritative FraudWatch event envelopes. Both producers and consumers must
preserve these rules:

- UTF-8 JSON objects contain exactly the fields declared by the schema.
- Encoded events are at most 65,536 bytes.
- IDs contain at least one non-whitespace character.
- Timestamps use the strict RFC3339 profile expressed by the schema and are
  normalized to UTC after validation.
- Transaction amounts are finite, non-negative JSON numbers; zero is valid.
- `transactions.v1` uses the UTF-8 `terminal_id` as its Kafka key.
- `fraud-labels.v1` uses the UTF-8 `transaction_id` as its Kafka key.

The files under `fixtures/` are language-neutral contract cases. Go ingestion
and Python consumer tests load the same transaction corpus so a contract change
cannot be accepted in only one implementation. Add or change fixtures before
changing a schema or validator.
