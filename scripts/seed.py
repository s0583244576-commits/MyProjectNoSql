"""
Seed script — loads data into all databases.

Usage:
    uv run python -m scripts.seed

Prerequisites:
    Run scripts.migrate first to create database structures.

What to implement in seed():
    Phase 1: Load products.json + customers.json into Postgres and MongoDB
    Phase 2: Initialize Redis inventory counters from Postgres product stock
    Phase 3: Build Neo4j co-purchase graph from historical_orders.json

Seed data files are in the seed_data/ directory.
"""

import os
from itertools import combinations
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

SEED_DIR = Path(__file__).parent.parent / "seed_data"


def seed(engine, mongo_db, redis_client=None, neo4j_driver=None):
    """Load seed data into all databases.

    Add your seeding logic here incrementally as you progress through phases.

    Args:
        engine: SQLAlchemy engine connected to Postgres
        mongo_db: pymongo Database instance
        redis_client: redis.Redis instance or None (Phase 2+)
        neo4j_driver: neo4j.Driver instance or None (Phase 3)

    Tip: Use json.load() to read the files in seed_data/:
        products = json.load(open(SEED_DIR / "products.json"))
        customers = json.load(open(SEED_DIR / "customers.json"))
        historical_orders = json.load(open(SEED_DIR / "historical_orders.json"))
    """
    import json
    from sqlalchemy.orm import Session
    from ecommerce_pipeline.postgres_models import Customer, Product
    customers = json.load(open(SEED_DIR / "customers.json"))
    products = json.load(open(SEED_DIR / "products.json"))

    with Session(engine) as session:
        # Customers → Postgres
        for c in customers:
            session.add(Customer(
                id=c["id"],
                name=c["name"],
                email=c["email"],
                address=c["address"],
            ))

        # Products → Postgres
        for p in products:
            session.add(Product(
                id=p["id"],
                name=p["name"],
                price=p["price"],
                stock_quantity=p["stock_quantity"],
                category=p["category"],
                description=p.get("description"),
                category_fields=p.get("category_fields"),
            ))

        session.commit()
    print("Postgres seeded.")

    # Products → MongoDB product_catalog
    mongo_db["product_catalog"].insert_many(products)
    # historical_orders → MongoDB order_snapshots (enriched)
    historical_orders = json.load(open(SEED_DIR / "historical_orders.json"))

    customers_by_id = {c["id"]: c for c in customers}
    products_by_id = {p["id"]: p for p in products}

    snapshots = []
    for order in historical_orders:
        customer = customers_by_id[order["customer_id"]]
        items = [
            {
                "product_id": pid,
                "product_name": products_by_id[pid]["name"],
                "quantity": 1,
                "unit_price": products_by_id[pid]["price"],
            }
            for pid in order["product_ids"]
        ]
        snapshots.append({
            "order_id": order["order_id"],
            "customer": {
                "id": customer["id"],
                "name": customer["name"],
                "email": customer["email"],
            },
            "items": items,
            "total_amount": round(sum(i["unit_price"] for i in items), 2),
            "status": "completed",
            "created_at": order["created_at"],
        })

    mongo_db["order_snapshots"].insert_many(snapshots)
    print("MongoDB seeded.")

    if redis_client:
        for p in products:
            redis_client.set(f"inventory:{p['id']}", p["stock_quantity"])
        print("Redis inventory counters initialized.")

    if neo4j_driver:
        with neo4j_driver.session() as neo4j_session:
            for order in historical_orders:
                for pid1, pid2 in combinations(sorted(order["product_ids"]), 2):
                    neo4j_session.run("""
                        MERGE (a:Product {id: $pid1})
                        MERGE (b:Product {id: $pid2})
                        MERGE (a)-[r:BOUGHT_TOGETHER]-(b)
                        ON CREATE SET r.weight = 1
                        ON MATCH SET r.weight = r.weight + 1
                    """, pid1=pid1, pid2=pid2)
        print("Neo4j co-purchase graph built.")
    
    


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _pg_url() -> str:
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = os.environ.get("POSTGRES_DB", "ecommerce")
    user = os.environ.get("POSTGRES_USER", "postgres")
    pwd = os.environ.get("POSTGRES_PASSWORD", "postgres")
    return f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"


def _mongo_db():
    from pymongo import MongoClient

    host = os.environ.get("MONGO_HOST", "localhost")
    port = int(os.environ.get("MONGO_PORT", "27017"))
    db = os.environ.get("MONGO_DB", "ecommerce")
    return MongoClient(host, port)[db]


def _redis_client():
    host = os.environ.get("REDIS_HOST")
    if not host:
        return None
    import redis

    port = int(os.environ.get("REDIS_PORT", "6379"))
    return redis.Redis(host=host, port=port, decode_responses=True)


def _neo4j_driver():
    host = os.environ.get("NEO4J_HOST")
    pwd = os.environ.get("NEO4J_PASSWORD")
    if not host or not pwd:
        return None
    from neo4j import GraphDatabase

    port = os.environ.get("NEO4J_BOLT_PORT", "7687")
    user = os.environ.get("NEO4J_USER", "neo4j")
    return GraphDatabase.driver(f"bolt://{host}:{port}", auth=(user, pwd))


def main():
    from sqlalchemy import create_engine

    engine = create_engine(_pg_url(), echo=False)
    mongo_db = _mongo_db()
    redis_client = _redis_client()
    neo4j_driver = _neo4j_driver()

    print("Seeding databases...")
    seed(engine, mongo_db, redis_client, neo4j_driver)
    print("Seeding complete.")

    if neo4j_driver:
        neo4j_driver.close()
    engine.dispose()


if __name__ == "__main__":
    main()
