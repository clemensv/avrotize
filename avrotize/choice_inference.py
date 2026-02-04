"""Discriminated Union (Choice) Inference for JSON data.

This module detects discriminated unions in JSON data, identifying:
1. Discriminator fields that correlate with schema variants
2. Nested discriminators in envelope patterns (e.g., CloudEvents with typed payload)
3. Sparse data vs distinct types

The inference uses Jaccard similarity clustering on field signatures,
then detects fields whose values correlate strongly with cluster membership.
"""

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple


@dataclass
class DocumentInfo:
    """Information about a single JSON document."""
    index: int
    data: Dict[str, Any]
    field_signature: frozenset  # Set of top-level field names
    field_values: Dict[str, str]  # String field values (potential discriminators)


@dataclass
class SchemaCluster:
    """A cluster of documents with similar schemas."""
    id: int
    documents: List[DocumentInfo] = field(default_factory=list)
    merged_signature: Set[str] = field(default_factory=set)
    required_fields: Set[str] = field(default_factory=set)
    
    def add_document(self, doc: DocumentInfo):
        self.documents.append(doc)
        if not self.merged_signature:
            self.merged_signature = set(doc.field_signature)
            self.required_fields = set(doc.field_signature)
        else:
            self.merged_signature |= doc.field_signature
            self.required_fields &= doc.field_signature


@dataclass
class DiscriminatorCandidate:
    """A potential discriminator field."""
    field_name: str
    values: Set[str]
    correlation: Dict[str, int]  # value -> cluster_id mapping
    correlation_score: float  # 0-1, how well values map to clusters


@dataclass 
class NestedDiscriminatorResult:
    """Result of nested discriminator analysis."""
    field_path: str  # e.g., "payload.type" or "data.kind"
    discriminator_field: str  # The actual discriminator within the nested object
    values: Set[str]  # The discriminator values
    nested_clusters: List[SchemaCluster]


@dataclass
class ChoiceInferenceResult:
    """Result of choice type inference."""
    is_choice: bool  # True if this is a discriminated union
    discriminator_field: Optional[str]  # Top-level discriminator field name
    discriminator_values: Set[str]  # The discriminator values
    clusters: List[SchemaCluster]  # Schema clusters (variants)
    nested_discriminator: Optional[NestedDiscriminatorResult]  # For envelope patterns
    recommendation: str  # Human-readable description


def jaccard_similarity(set1: frozenset, set2: frozenset) -> float:
    """Compute Jaccard similarity between two sets."""
    if not set1 and not set2:
        return 1.0
    intersection = len(set1 & set2)
    union = len(set1 | set2)
    return intersection / union if union > 0 else 0.0


def _extract_document_info(index: int, data: Dict[str, Any]) -> DocumentInfo:
    """Extract analysis info from a JSON document."""
    field_signature = frozenset(data.keys())
    
    # Extract potential discriminator values (string, number, boolean)
    field_values = {}
    for key, value in data.items():
        if isinstance(value, str):
            field_values[key] = value
        elif isinstance(value, (int, float)) and not isinstance(value, bool):
            field_values[key] = str(value)
        elif isinstance(value, bool):
            field_values[key] = str(value).lower()
    
    return DocumentInfo(
        index=index,
        data=data,
        field_signature=field_signature,
        field_values=field_values
    )


def _cluster_by_similarity(
    documents: List[DocumentInfo], 
    similarity_threshold: float = 0.5
) -> List[SchemaCluster]:
    """Cluster documents by field signature similarity.
    
    Uses a two-pass approach:
    1. Initial greedy clustering
    2. Refinement pass to reassign documents to better-matching clusters
    """
    clusters: List[SchemaCluster] = []
    
    # First pass: greedy clustering
    for doc in documents:
        best_cluster = None
        best_similarity = 0.0
        
        for cluster in clusters:
            if cluster.documents:
                rep_sig = cluster.documents[0].field_signature
                sim = jaccard_similarity(doc.field_signature, rep_sig)
                if sim >= similarity_threshold and sim > best_similarity:
                    best_cluster = cluster
                    best_similarity = sim
        
        if best_cluster:
            best_cluster.add_document(doc)
        else:
            new_cluster = SchemaCluster(id=len(clusters))
            new_cluster.add_document(doc)
            clusters.append(new_cluster)
    
    # Second pass: reassign documents to better-matching clusters
    changed = True
    max_iterations = 3
    iteration = 0
    
    while changed and iteration < max_iterations:
        changed = False
        iteration += 1
        
        for cluster in clusters:
            docs_to_move = []
            for doc in cluster.documents:
                best_cluster = cluster
                best_similarity = jaccard_similarity(doc.field_signature, 
                    cluster.documents[0].field_signature if cluster.documents else frozenset())
                
                for other_cluster in clusters:
                    if other_cluster.id == cluster.id or not other_cluster.documents:
                        continue
                    
                    rep_sig = other_cluster.documents[0].field_signature
                    sim = jaccard_similarity(doc.field_signature, rep_sig)
                    
                    if sim > best_similarity + 0.1:  # Must be significantly better
                        best_cluster = other_cluster
                        best_similarity = sim
                
                if best_cluster.id != cluster.id:
                    docs_to_move.append((doc, best_cluster))
            
            for doc, new_cluster in docs_to_move:
                cluster.documents.remove(doc)
                new_cluster.add_document(doc)
                changed = True
        
        # Remove empty clusters and renumber
        clusters = [c for c in clusters if c.documents]
        for i, c in enumerate(clusters):
            c.id = i
    
    return clusters


def _detect_discriminators(
    documents: List[DocumentInfo],
    clusters: List[SchemaCluster],
    min_correlation: float = 0.9
) -> List[DiscriminatorCandidate]:
    """Detect fields that correlate strongly with cluster membership."""
    doc_to_cluster: Dict[int, int] = {}
    for cluster in clusters:
        for doc in cluster.documents:
            doc_to_cluster[doc.index] = cluster.id
    
    field_presence: Dict[str, int] = defaultdict(int)
    field_values_by_doc: Dict[str, Dict[int, str]] = defaultdict(dict)
    
    for doc in documents:
        for field_name, value in doc.field_values.items():
            field_presence[field_name] += 1
            field_values_by_doc[field_name][doc.index] = value
    
    candidates = []
    total_docs = len(documents)
    num_clusters = len(clusters)
    
    for field_name, presence_count in field_presence.items():
        if presence_count < total_docs * 0.8:
            continue
        
        values = set(field_values_by_doc[field_name].values())
        
        # Skip likely unique IDs (>80% unique AND much more values than clusters)
        uniqueness_ratio = len(values) / total_docs
        values_vs_clusters = len(values) / num_clusters if num_clusters > 0 else len(values)
        if uniqueness_ratio > 0.8 and values_vs_clusters > 3:
            continue
        
        if len(values) < 2:
            continue
        
        # Single cluster with multiple values - check if values create distinct groups
        if len(clusters) == 1:
            value_to_docs: Dict[str, List[DocumentInfo]] = defaultdict(list)
            for doc in documents:
                if field_name in doc.field_values:
                    value_to_docs[doc.field_values[field_name]].append(doc)
            
            if len(value_to_docs) >= 2:
                all_values = list(value_to_docs.keys())
                inter_sims = []
                for i, v1 in enumerate(all_values):
                    for v2 in all_values[i+1:]:
                        docs1 = value_to_docs[v1]
                        docs2 = value_to_docs[v2]
                        if docs1 and docs2:
                            sim = jaccard_similarity(docs1[0].field_signature, docs2[0].field_signature)
                            inter_sims.append(sim)
                
                avg_inter_sim = sum(inter_sims) / len(inter_sims) if inter_sims else 1.0
                
                if avg_inter_sim < 0.7:
                    correlation = {v: i for i, v in enumerate(all_values)}
                    candidates.append(DiscriminatorCandidate(
                        field_name=field_name,
                        values=values,
                        correlation=correlation,
                        correlation_score=1.0 - avg_inter_sim
                    ))
            continue
        
        # Multiple clusters - check correlation
        value_to_clusters: Dict[str, Set[int]] = defaultdict(set)
        
        for doc_idx, value in field_values_by_doc[field_name].items():
            cluster_id = doc_to_cluster[doc_idx]
            value_to_clusters[value].add(cluster_id)
        
        if len(values) < len(clusters):
            continue
        
        perfect_mappings = sum(1 for v, c in value_to_clusters.items() if len(c) == 1)
        correlation_score = perfect_mappings / len(values) if values else 0
        
        if correlation_score >= min_correlation:
            correlation = {}
            for value, cluster_ids in value_to_clusters.items():
                correlation[value] = list(cluster_ids)[0]
            
            candidates.append(DiscriminatorCandidate(
                field_name=field_name,
                values=values,
                correlation=correlation,
                correlation_score=correlation_score
            ))
    
    candidates.sort(key=lambda c: c.correlation_score, reverse=True)
    return candidates


def _recluster_by_discriminator(
    documents: List[DocumentInfo],
    discriminator: DiscriminatorCandidate
) -> List[SchemaCluster]:
    """Re-cluster documents based on a detected discriminator field."""
    clusters_by_value: Dict[str, SchemaCluster] = {}
    
    for doc in documents:
        value = doc.field_values.get(discriminator.field_name)
        if value:
            if value not in clusters_by_value:
                clusters_by_value[value] = SchemaCluster(id=len(clusters_by_value))
            clusters_by_value[value].add_document(doc)
    
    return list(clusters_by_value.values())


def _detect_nested_discriminator(
    documents: List[DocumentInfo],
    max_depth: int = 2
) -> Optional[NestedDiscriminatorResult]:
    """Check if any nested object field contains a discriminated union."""
    if max_depth <= 0:
        return None
    
    total_docs = len(documents)
    field_objects: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    
    for doc in documents:
        for field_name, value in doc.data.items():
            if isinstance(value, dict) and value:
                field_objects[field_name].append(value)
    
    for field_name, nested_objects in field_objects.items():
        if len(nested_objects) < total_docs * 0.8:
            continue
        
        nested_docs = [_extract_document_info(i, obj) for i, obj in enumerate(nested_objects)]
        nested_clusters = _cluster_by_similarity(nested_docs, similarity_threshold=0.5)
        
        if len(nested_clusters) >= 2:
            nested_discriminators = _detect_discriminators(nested_docs, nested_clusters)
            if nested_discriminators:
                best = nested_discriminators[0]
                if best.correlation_score >= 0.8:
                    return NestedDiscriminatorResult(
                        field_path=f"{field_name}.{best.field_name}",
                        discriminator_field=best.field_name,
                        values=best.values,
                        nested_clusters=nested_clusters
                    )
        
        if len(nested_clusters) == 1:
            nested_discriminators = _detect_discriminators(nested_docs, nested_clusters)
            if nested_discriminators:
                best = nested_discriminators[0]
                if best.correlation_score > 0.3:
                    reclustered = _recluster_by_discriminator(nested_docs, best)
                    if len(reclustered) >= 2:
                        return NestedDiscriminatorResult(
                            field_path=f"{field_name}.{best.field_name}",
                            discriminator_field=best.field_name,
                            values=best.values,
                            nested_clusters=reclustered
                        )
        
        if max_depth > 1:
            deeper_result = _detect_nested_discriminator(nested_docs, max_depth - 1)
            if deeper_result:
                deeper_result.field_path = f"{field_name}.{deeper_result.field_path}"
                return deeper_result
    
    return None


def infer_choice_type(
    values: List[Dict[str, Any]],
    similarity_threshold: float = 0.5,
    detect_nested: bool = True
) -> ChoiceInferenceResult:
    """Analyze JSON values to detect discriminated unions (choice types).
    
    Args:
        values: List of JSON objects to analyze
        similarity_threshold: Jaccard similarity threshold for clustering (0-1)
        detect_nested: Whether to detect nested discriminators (e.g., in CloudEvents payload)
    
    Returns:
        ChoiceInferenceResult with analysis results
    """
    if not values:
        return ChoiceInferenceResult(
            is_choice=False,
            discriminator_field=None,
            discriminator_values=set(),
            clusters=[],
            nested_discriminator=None,
            recommendation="Empty input"
        )
    
    # Filter to dict values only
    dict_values = [v for v in values if isinstance(v, dict)]
    if not dict_values:
        return ChoiceInferenceResult(
            is_choice=False,
            discriminator_field=None,
            discriminator_values=set(),
            clusters=[],
            nested_discriminator=None,
            recommendation="No object values found"
        )
    
    documents = [_extract_document_info(i, v) for i, v in enumerate(dict_values)]
    clusters = _cluster_by_similarity(documents, similarity_threshold)
    discriminators = _detect_discriminators(documents, clusters)
    
    # Re-cluster by discriminator if found in single-cluster scenario
    if len(clusters) == 1 and discriminators:
        best = discriminators[0]
        if best.correlation_score > 0.3:
            clusters = _recluster_by_discriminator(documents, best)
    
    # Single cluster = check for nested discriminator or sparse data
    if len(clusters) == 1:
        nested_result = None
        if detect_nested:
            nested_result = _detect_nested_discriminator(documents, max_depth=2)
        
        if nested_result:
            return ChoiceInferenceResult(
                is_choice=True,
                discriminator_field=None,
                discriminator_values=set(),
                clusters=clusters,
                nested_discriminator=nested_result,
                recommendation=f"Envelope with nested discriminator at '{nested_result.field_path}'"
            )
        
        return ChoiceInferenceResult(
            is_choice=False,
            discriminator_field=None,
            discriminator_values=set(),
            clusters=clusters,
            nested_discriminator=None,
            recommendation="Single type with optional fields"
        )
    
    # Multiple clusters with discriminator = discriminated union
    if discriminators:
        best = discriminators[0]
        return ChoiceInferenceResult(
            is_choice=True,
            discriminator_field=best.field_name,
            discriminator_values=best.values,
            clusters=clusters,
            nested_discriminator=None,
            recommendation=f"Discriminated union on field '{best.field_name}' with {len(clusters)} variants"
        )
    
    # Multiple clusters without discriminator = undiscriminated union
    return ChoiceInferenceResult(
        is_choice=True,
        discriminator_field=None,
        discriminator_values=set(),
        clusters=clusters,
        nested_discriminator=None,
        recommendation=f"Undiscriminated union with {len(clusters)} distinct types"
    )
