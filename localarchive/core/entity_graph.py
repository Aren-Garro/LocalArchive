"""Entity relationship graph builder."""

from __future__ import annotations


def build_entity_graph(docs: list[dict], fields_by_doc: dict[int, list[dict]]) -> dict:
    doc_nodes: list[dict] = []
    entity_nodes: dict[str, dict] = {}
    edges: list[dict] = []

    for doc in docs:
        doc_id = int(doc["id"])
        doc_node_id = f"doc:{doc_id}"
        doc_nodes.append(
            {
                "id": doc_node_id,
                "kind": "document",
                "doc_id": doc_id,
                "label": str(doc.get("filename", f"document-{doc_id}")),
                "file_type": str(doc.get("file_type", "")),
                "status": str(doc.get("status", "")),
            }
        )
        fields = fields_by_doc.get(doc_id) or []
        seen_entities: set[tuple[str, str]] = set()
        for field in fields:
            field_type = str(field.get("field_type", "")).strip().lower()
            if not field_type.startswith("entity_"):
                continue
            value = str(field.get("value", "")).strip()
            if not value:
                continue
            key = (field_type, value.lower())
            if key in seen_entities:
                continue
            seen_entities.add(key)
            entity_node_id = f"entity:{field_type}:{value.lower()}"
            if entity_node_id not in entity_nodes:
                entity_nodes[entity_node_id] = {
                    "id": entity_node_id,
                    "kind": "entity",
                    "entity_type": field_type,
                    "label": value,
                }
            edges.append(
                {
                    "source": doc_node_id,
                    "target": entity_node_id,
                    "relation": "mentions",
                }
            )

    return {
        "nodes": doc_nodes + sorted(entity_nodes.values(), key=lambda x: str(x["id"])),
        "edges": edges,
    }
