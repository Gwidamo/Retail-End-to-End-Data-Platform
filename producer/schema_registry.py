from __future__ import annotations

import os
from dotenv import load_dotenv
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer


schema_str = open("sales_event_schema.avsc").read()
sr_client = SchemaRegistryClient({"url": os.environ["SCHEMA_REGISTRY_URL"]})


def _to_dict_sales_event(obj, ctx=None):
    """Convert a sales event object to a plain dict for Avro serialization.

    The producer builds events as plain dicts already, so this function simply
    validates / returns the input. AvroSerializer requires a callable with
    signature (obj, ctx) -> dict.
    """
    if obj is None:
        return None
    if isinstance(obj, dict):
        return obj
    # Fallback: try to use __dict__ if present
    try:
        return dict(obj)
    except Exception:
        raise TypeError("Cannot convert object to dict for Avro serialization")


avro_serializer = AvroSerializer(
    schema_registry_client=sr_client,
    schema_str=schema_str,
    to_dict=_to_dict_sales_event,
    conf={
        "auto.register.schemas": True,
        "normalize.schemas": True,
    }
)