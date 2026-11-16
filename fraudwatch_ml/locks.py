"""PostgreSQL transaction-scoped locks shared by stream processors.

Every path that touches both identities must acquire the transaction lock first
and the terminal lock second.  Prefixing the lock keys keeps the two lock
namespaces independent even when an ID string happens to be identical.
"""


def lock_transaction(conn, transaction_id: str) -> None:
    conn.execute(
        """
        SELECT pg_advisory_xact_lock(
            hashtextextended('fraudwatch:transaction:' || %s, 0)
        )
        """,
        (transaction_id,),
    )


def lock_terminal(conn, terminal_id: str) -> None:
    conn.execute(
        """
        SELECT pg_advisory_xact_lock(
            hashtextextended('fraudwatch:terminal:' || %s, 0)
        )
        """,
        (terminal_id,),
    )
