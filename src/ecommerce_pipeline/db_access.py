"""
DBAccess — the data access layer.

This is one of the files you implement. The web API is already wired up;
every route calls one method on this class. Your job is to replace each
`raise NotImplementedError(...)` with a real implementation.

Work through the phases in order. Read the corresponding lesson file before
starting each phase.

You also implement scripts/migrate.py and scripts/seed.py alongside this file.
"""

from __future__ import annotations

import json
import logging
from itertools import combinations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import neo4j
    import redis as redis_lib
    from pymongo.database import Database as MongoDatabase
    from sqlalchemy.orm import sessionmaker

    from ecommerce_pipeline.models.requests import OrderItemRequest
    from ecommerce_pipeline.models.responses import (
        CategoryRevenueResponse,
        OrderCustomerEmbed,
        OrderItemResponse,
        OrderResponse,
        OrderSnapshotResponse,
        ProductResponse,
        RecommendationResponse,
    )

logger = logging.getLogger(__name__)


class DBAccess:
    def __init__(
        self,
        pg_session_factory: sessionmaker,
        mongo_db: MongoDatabase,
        redis_client: redis_lib.Redis | None = None,
        neo4j_driver: neo4j.Driver | None = None,
    ) -> None:
        self._pg_session_factory = pg_session_factory
        self._mongo_db = mongo_db
        self._redis = redis_client
        self._neo4j = neo4j_driver

    # ── Phase 1 ───────────────────────────────────────────────────────────────

    def create_order(self, customer_id: int, items: list[OrderItemRequest]) -> OrderResponse:
        from datetime import datetime
        from ecommerce_pipeline.postgres_models import Order, OrderItem, Product, Customer
        from ecommerce_pipeline.models.responses import OrderItemResponse, OrderCustomerEmbed, OrderResponse

        if self._redis:
            for item in items:
                stock = self._redis.get(f"inventory:{item.product_id}")
                if stock is not None and int(stock) < item.quantity:
                    raise ValueError(f"Insufficient stock for product {item.product_id}")

        with self._pg_session_factory() as session:
            # בדיקת מלאי לכל המוצרים לפני שמשנים כלום
            order_items_data = []
            total_amount = 0.0

            for item in items:
                product = session.get(Product, item.product_id, with_for_update=True)
                if product is None:
                    raise ValueError(f"Product {item.product_id} not found")
                if product.stock_quantity < item.quantity:
                    raise ValueError(
                        f"Insufficient stock for product {item.product_id}: "
                        f"requested {item.quantity}, available {product.stock_quantity}"
                    )
                order_items_data.append((product, item.quantity))
                total_amount += float(product.price) * item.quantity

            # יצירת ה-order ב-Postgres
            order = Order(
                customer_id=customer_id,
                status="confirmed",
                total_amount=total_amount,
            )
            session.add(order)
            session.flush()  # מקבלים order.id לפני commit

            response_items = []
            for product, quantity in order_items_data:
                session.add(OrderItem(
                    order_id=order.id,
                    product_id=product.id,
                    quantity=quantity,
                    unit_price=float(product.price),
                ))
                product.stock_quantity -= quantity
                response_items.append(OrderItemResponse(
                    product_id=product.id,
                    product_name=product.name,
                    quantity=quantity,
                    unit_price=float(product.price),
                ))

            session.commit()
            for product, quantity in order_items_data:
                self._mongo_db["product_catalog"].update_one(
                    {"id": product.id},
                    {"$inc": {"stock_quantity": -quantity}},
                )
                if self._redis:
                    self._redis.decrby(f"inventory:{product.id}", quantity)
                    self._redis.delete(f"product:{product.id}")
            order_id = order.id
            created_at = order.created_at.isoformat() if order.created_at else datetime.utcnow().isoformat()

            customer = session.get(Customer, customer_id)

        # Neo4j co-purchase edges (best-effort)
        if self._neo4j:
            product_ids = [p.id for p, _ in order_items_data]
            with self._neo4j.session() as neo4j_session:
                for pid1, pid2 in combinations(sorted(product_ids), 2):
                    neo4j_session.run("""
                        MERGE (a:Product {id: $pid1})
                        MERGE (b:Product {id: $pid2})
                        MERGE (a)-[r:BOUGHT_TOGETHER]-(b)
                        ON CREATE SET r.weight = 1
                        ON MATCH SET r.weight = r.weight + 1
                    """, pid1=pid1, pid2=pid2)

        # שמירת snapshot במונגו (best-effort)
        self.save_order_snapshot(
            order_id=order_id,
            customer=OrderCustomerEmbed(id=customer.id, name=customer.name, email=customer.email),
            items=response_items,
            total_amount=total_amount,
            status="completed",
            created_at=created_at,
        )
        return OrderResponse(
            order_id=order_id,
            customer_id=customer_id,
            status="completed",
            total_amount=total_amount,
            created_at=created_at,
            items=response_items,
        )


    def get_product(self, product_id: int) -> ProductResponse | None:
        from ecommerce_pipeline.models.responses import ProductResponse

        if self._redis:
            cached = self._redis.get(f"product:{product_id}")
            if cached:
                return ProductResponse(**json.loads(cached))

        doc = self._mongo_db["product_catalog"].find_one({"id": product_id})
        if doc is None:
            return None
        response = ProductResponse(
            id=doc["id"],
            name=doc["name"],
            price=float(doc["price"]),
            stock_quantity=doc["stock_quantity"],
            category=doc["category"],
            description=doc.get("description", ""),
            category_fields=doc.get("category_fields") or {},
        )

        if self._redis:
            self._redis.setex(f"product:{product_id}", 300, json.dumps(response.model_dump()))
        return response


    def search_products(
        self,
        category: str | None = None,
        q: str | None = None,
    ) -> list[ProductResponse]:
        from ecommerce_pipeline.models.responses import ProductResponse

        query = {}
        if category is not None:
            query["category"] = category
        if q is not None:
            query["name"] = {"$regex": q, "$options": "i"}

        docs = self._mongo_db["product_catalog"].find(query)
        return [
            ProductResponse(
                id=doc["id"],
                name=doc["name"],
                price=float(doc["price"]),
                stock_quantity=doc["stock_quantity"],
                category=doc["category"],
                description=doc.get("description", ""),
                category_fields=doc.get("category_fields") or {},
            )
            for doc in docs
        ]
            
    def save_order_snapshot(
        self,
        order_id: int,
        customer: OrderCustomerEmbed,
        items: list[OrderItemResponse],
        total_amount: float,
        status: str,
        created_at: str,
    ) -> str:
        doc = {
            "order_id": order_id,
            "customer": customer.model_dump(),
            "items": [item.model_dump() for item in items],
            "total_amount": total_amount,
            "status": status,
            "created_at": created_at,
        }
        result = self._mongo_db["order_snapshots"].replace_one(
            {"order_id": order_id}, doc, upsert=True
        )
        if result.upserted_id is not None:
            return str(result.upserted_id)
        found = self._mongo_db["order_snapshots"].find_one({"order_id": order_id}, {"_id": 1})
        return str(found["_id"]) if found else str(order_id)

    def get_order(self, order_id: int) -> OrderSnapshotResponse | None:
        from ecommerce_pipeline.models.responses import OrderSnapshotResponse

        doc = self._mongo_db["order_snapshots"].find_one({"order_id": order_id})
        if doc is None:
            return None
        return OrderSnapshotResponse(**{k: v for k, v in doc.items() if k != "_id"})

    def get_order_history(self, customer_id: int) -> list[OrderSnapshotResponse]:
        from ecommerce_pipeline.models.responses import OrderSnapshotResponse

        docs = self._mongo_db["order_snapshots"].find(
            {"customer.id": customer_id},
            sort=[("created_at", -1)],
        )
        return [
            OrderSnapshotResponse(**{k: v for k, v in doc.items() if k != "_id"})
            for doc in docs
        ]

    def revenue_by_category(self) -> list[CategoryRevenueResponse]:
        """Compute total revenue per product category, sorted by total_revenue descending.

        See CategoryRevenueResponse in models/responses.py for the return shape.
        """
        from sqlalchemy import select, func
        from ecommerce_pipeline.postgres_models import OrderItem, Product
        from ecommerce_pipeline.models.responses import CategoryRevenueResponse

        with self._pg_session_factory() as session:
            stmt = (
                select(
                    Product.category,
                    func.sum(OrderItem.unit_price * OrderItem.quantity).label("total_revenue"),
                )
                .join(Product, OrderItem.product_id == Product.id)
                .group_by(Product.category)
                .order_by(func.sum(OrderItem.unit_price * OrderItem.quantity).desc())
            )
            rows = session.execute(stmt).all()
            return [
                CategoryRevenueResponse(category=row.category, total_revenue=float(row.total_revenue))
                for row in rows
            ]

    # ── Phase 2 ───────────────────────────────────────────────────────────────
    #
    # In this phase you also need to:
    #   - Update create_order to DECR Redis inventory counters after the
    #     Postgres transaction succeeds.
    #   - Optionally, add a fast pre-check: before starting the Postgres
    #     transaction, check the Redis counter. If it shows insufficient
    #     stock, fail fast without hitting Postgres.
    #   - Update scripts/seed.py to initialize inventory counters in Redis.
    #   - Add cache-aside logic to get_product (check Redis first, populate
    #     on miss with a 300-second TTL).

    def invalidate_product_cache(self, product_id: int) -> None:
        """Remove a product's cached entry.

        Call this after updating a product's data so the next read fetches
        fresh data from the primary store. No-op if no entry exists.
        """
        if self._redis:
            self._redis.delete(f"product:{product_id}")

    def record_product_view(self, customer_id: int, product_id: int) -> None:
        """Record that a customer viewed a product.

        Maintains a bounded, ordered list of the customer's most recently
        viewed products (most recent first, capped at 10 entries).
        """
        if self._redis:
            key = f"recently_viewed:{customer_id}"
            self._redis.lrem(key, 0, product_id)
            self._redis.lpush(key, product_id)
            self._redis.ltrim(key, 0, 9)

    def get_recently_viewed(self, customer_id: int) -> list[int]:
        """Return up to 10 recently viewed product IDs for a customer.

        Returns IDs as integers, most recently viewed first.
        Returns an empty list if no views have been recorded.
        """
        if not self._redis:
            return []
        return [int(pid) for pid in self._redis.lrange(f"recently_viewed:{customer_id}", 0, 9)]

    # ── Phase 3 ───────────────────────────────────────────────────────────────
    #
    # In this phase you also need to:
    #   - Update create_order to MERGE co-purchase edges in Neo4j for every
    #     pair of products in the order, incrementing the edge weight.
    #   - Update scripts/migrate.py to create Neo4j constraints.
    #   - Update scripts/seed.py to build the co-purchase graph from
    #     seed_data/historical_orders.json.

    def get_recommendations(self, product_id: int, limit: int = 5) -> list[RecommendationResponse]:
        """Return product recommendations based on co-purchase patterns.

        See RecommendationResponse in models/responses.py for the return shape.
        Sorted by score descending. Returns an empty list if no co-purchase relationships exist.
        """
        from ecommerce_pipeline.models.responses import RecommendationResponse

        if not self._neo4j:
            return []
        with self._neo4j.session() as neo4j_session:
            result = neo4j_session.run("""
                MATCH (p:Product {id: $product_id})-[r:BOUGHT_TOGETHER]-(other:Product)
                RETURN other.id AS product_id, r.weight AS score
                ORDER BY score DESC
                LIMIT $limit
            """, product_id=product_id, limit=limit)
            return [
                RecommendationResponse(product_id=row["product_id"], name="", score=row["score"])
                for row in result
            ]
