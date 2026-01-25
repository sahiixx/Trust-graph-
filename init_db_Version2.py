#!/usr/bin/env python3
"""
Robust init_db.py

Features:
- Exponential backoff waiting for Neo4j connectivity
- File existence checks
- Safer statement execution using transactions (session.execute_write)
- Better logs and non-zero exit codes on failure
- Tolerant splitting of Cypher files into statements

Note: This splitter is pragmatic (splits on semicolons). For complex Cypher with semicolons
in strings or unusual formatting, consider keeping one statement per file or using a
proper Cypher migration tool.
"""
from __future__ import annotations
import os
import time
import logging
import sys
import re
from typing import List

try:
    from neo4j import GraphDatabase, Session
except Exception:
    raise RuntimeError("neo4j driver is required. Install with: pip install neo4j")

logging.basicConfig(
    level=os.getenv("INIT_DB_LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s",
)

logger = logging.getLogger("init_db")

def split_cypher_statements(text: str) -> List[str]:
    """
    Split Cypher text into individual statements.
    This function:
    - Removes block comments /* ... */
    - Removes single-line comments starting with //
    - Splits on semicolon characters that are followed by newline or EOF.
    """
    # Remove block comments
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.DOTALL)
    # Remove // comments
    text = re.sub(r"//.*?$", "", text, flags=re.MULTILINE)
    # Normalize newlines
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    # Split on semicolons that are followed by newline or end-of-text
    parts = re.split(r";\s*(?:\n|$)", text)
    statements = [p.strip() for p in parts if p and p.strip()]
    return statements

def wait_for_neo4j(driver, max_attempts: int = 8, initial_delay: float = 1.0) -> bool:
    """
    Exponential backoff waiting for Neo4j connectivity.
    Returns True on success, False otherwise.
    """
    delay = initial_delay
    for attempt in range(1, max_attempts + 1):
        try:
            driver.verify_connectivity()
            logger.info("✓ Connected to Neo4j")
            return True
        except Exception as exc:
            logger.debug("Neo4j connectivity attempt %d failed: %s", attempt, exc)
            logger.info("Waiting for Neo4j... (%d/%d) retrying in %.1fs", attempt, max_attempts, delay)
            time.sleep(delay)
            delay = min(delay * 2, 30.0)
    return False

def run_statements(session: Session, statements: List[str]) -> None:
    """
    Execute a list of Cypher statements inside write transactions.
    """
    for i, stmt in enumerate(statements, start=1):
        try:
            def _work(tx, q=stmt):
                tx.run(q)
            session.execute_write(_work)
            logger.info("✓ Executed statement %d/%d", i, len(statements))
        except Exception as e:
            logger.error("❌ Error executing statement %d: %s", i, e)
            raise

def load_cypher_file(path: str) -> List[str]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Cypher file not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        text = f.read()
    statements = split_cypher_statements(text)
    logger.info("Loaded %d statements from %s", len(statements), path)
    return statements

def init_database() -> bool:
    NEO4J_URI = os.getenv("NEO4J_URI")
    NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
    NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

    if not all([NEO4J_URI, NEO4J_PASSWORD]):
        logger.error("Missing Neo4j credentials (NEO4J_URI and NEO4J_PASSWORD are required)")
        return False

    logger.info("Connecting to %s as %s", NEO4J_URI, NEO4J_USER)
    driver = None
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    except Exception as e:
        logger.exception("Failed to create Neo4j driver: %s", e)
        return False

    try:
        if not wait_for_neo4j(driver):
            logger.error("Could not connect to Neo4j after retries")
            return False

        schema_path = os.getenv("SCHEMA_CYPHER_PATH", "graph/schema.cypher")
        seed_path = os.getenv("SEED_CYPHER_PATH", "graph/seed.cypher")

        with driver.session() as session:
            # Schema
            try:
                if os.path.exists(schema_path):
                    statements = load_cypher_file(schema_path)
                    if statements:
                        logger.info("Creating constraints and indexes...")
                        run_statements(session, statements)
                        logger.info("✓ Schema created")
                    else:
                        logger.info("No schema statements to run")
                else:
                    logger.warning("Schema file not found at %s, skipping schema creation", schema_path)
            except Exception:
                logger.exception("Error while applying schema")
                raise

            # Seed
            try:
                if os.path.exists(seed_path):
                    statements = load_cypher_file(seed_path)
                    if statements:
                        logger.info("Loading seed data...")
                        run_statements(session, statements)
                        logger.info("✓ Seed data loaded")
                    else:
                        logger.info("No seed statements to run")
                else:
                    logger.warning("Seed file not found at %s, skipping seed load", seed_path)
            except Exception:
                logger.exception("Error while loading seed data")
                raise

            # Verify
            try:
                result = session.execute_read(lambda tx: tx.run("MATCH (e:Entity) RETURN count(e) AS count").single())
                count = result["count"] if result and "count" in result else None
                logger.info("✓ Database initialized%s", f" with {count} entities" if count is not None else "")
            except Exception:
                logger.exception("Error while verifying database contents")
                raise

        return True

    except Exception as e:
        logger.error("❌ Error initializing database: %s", e)
        return False
    finally:
        try:
            if driver:
                driver.close()
        except Exception:
            logger.debug("Error closing driver", exc_info=True)

if __name__ == "__main__":
    logger.info("🚀 Trust Graph Database Initialization")
    logger.info("=" * 50)
    ok = init_database()
    logger.info("=" * 50)
    if ok:
        logger.info("✅ Database initialization complete!")
        sys.exit(0)
    else:
        logger.error("❌ Database initialization failed")
        sys.exit(1)