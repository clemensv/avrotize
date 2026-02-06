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
        
        # Skip boolean-like string values - these are flags, not discriminators
        # A field with only "true"/"false" (or similar) values is not a type discriminator
        normalized_values = {v.lower() if isinstance(v, str) else str(v).lower() for v in values}
        boolean_values = {'true', 'false', 'yes', 'no', '0', '1'}
        if normalized_values <= boolean_values:
            continue
        
        # Skip numeric string values - these are data values, not discriminators
        # Discriminators are semantic type names like "Event", "PlayerTracking", not "1", "15", "2024"
        def is_numeric_string(s: str) -> bool:
            """Check if a string represents a number (int or float)."""
            try:
                float(s)
                return True
            except (ValueError, TypeError):
                return False
        
        if all(is_numeric_string(v) for v in values):
            continue
        
        # Single cluster with multiple values - check if values create distinct groups
        if len(clusters) == 1:
            value_to_docs: Dict[str, List[DocumentInfo]] = defaultdict(list)
            for doc in documents:
                if field_name in doc.field_values:
                    value_to_docs[doc.field_values[field_name]].append(doc)
            
            if len(value_to_docs) >= 2:
                # Check 1: Does each value map to a consistent signature?
                # A perfect discriminator has each value producing identical signatures
                value_to_sigs: Dict[str, Set[tuple]] = {}
                for val, val_docs in value_to_docs.items():
                    sigs = set(tuple(sorted(d.field_signature)) for d in val_docs)
                    value_to_sigs[val] = sigs
                
                # Count values with perfectly consistent signatures (all docs same sig)
                consistent_values = sum(1 for sigs in value_to_sigs.values() if len(sigs) == 1)
                consistency_ratio = consistent_values / len(value_to_sigs)
                
                # Check 2: Are signatures distinct across values?
                all_primary_sigs = [list(sigs)[0] for sigs in value_to_sigs.values() if sigs]
                distinct_sigs = set(all_primary_sigs)
                distinctness_ratio = len(distinct_sigs) / len(all_primary_sigs) if all_primary_sigs else 0
                
                # Check 3: Original inter-similarity check (relaxed to 0.85)
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
                
                # Check 4: Discriminator-field correlation (envelope pattern)
                # If discriminator value matches a unique payload field name, it's legitimate
                # e.g., _subtype: "play" -> has field "play" that only appears for this value
                discriminator_field_matches = 0
                unique_fields_per_value: Dict[str, Set[str]] = {}
                for disc_val, val_docs in value_to_docs.items():
                    if not val_docs:
                        continue
                    # Get fields unique to this discriminator value
                    this_sig = val_docs[0].field_signature
                    other_sigs = [d.field_signature for v, docs in value_to_docs.items() 
                                  if v != disc_val for d in docs[:1]]
                    if other_sigs:
                        common_with_others = this_sig.intersection(*other_sigs)
                        unique_fields = this_sig - common_with_others - {field_name}
                        unique_fields_per_value[disc_val] = unique_fields
                        # Check if discriminator value matches any unique field (case-insensitive)
                        disc_val_lower = disc_val.lower() if isinstance(disc_val, str) else str(disc_val).lower()
                        if any(uf.lower() == disc_val_lower for uf in unique_fields):
                            discriminator_field_matches += 1
                
                has_envelope_pattern = discriminator_field_matches >= len(value_to_docs) * 0.5
                
                # Check 5: Structural quality - detect sparse data false positives
                # If unique fields are very few AND overlap across variants, it's likely sparse data
                should_reject_sparse = False
                if not has_envelope_pattern and unique_fields_per_value:
                    all_unique_fields = [ufs for ufs in unique_fields_per_value.values() if ufs]
                    if all_unique_fields:
                        # Count total unique fields across all variants
                        total_unique = set().union(*all_unique_fields)
                        avg_unique = sum(len(ufs) for ufs in all_unique_fields) / len(all_unique_fields)
                        
                        # Check for "sparse optional field" pattern:
                        # - Very few unique fields per variant (1-2)
                        # - Moderate to high similarity (>0.6) 
                        # - No envelope pattern
                        # - Few total samples per variant (could be sample noise)
                        # - Only 2 variants (binary split is more likely to be accidental)
                        # - NOT a subset pattern (where one variant is just base + extras)
                        min_samples = min(len(docs) for docs in value_to_docs.values())
                        num_variants = len(value_to_docs)
                        
                        # Check for subset pattern (inheritance-like polymorphism)
                        # If one variant's signature is a subset of another, it's likely real
                        is_subset_pattern = False
                        all_sigs = [list(sigs)[0] for v, sigs in value_to_sigs.items() if sigs and len(sigs) == 1]
                        if len(all_sigs) >= 2:
                            for i, sig1 in enumerate(all_sigs):
                                for sig2 in all_sigs[i+1:]:
                                    # Check if one is subset of the other (ignoring discriminator)
                                    sig1_set = set(sig1) - {field_name}
                                    sig2_set = set(sig2) - {field_name}
                                    if sig1_set < sig2_set or sig2_set < sig1_set:
                                        is_subset_pattern = True
                                        break
                                if is_subset_pattern:
                                    break
                        
                        # Binary splits with few samples and minimal structural difference
                        # are most likely sparse data artifacts (unless it's a subset pattern)
                        if (num_variants == 2 and 
                            avg_unique <= 1.5 and 
                            avg_inter_sim > 0.6 and 
                            min_samples < 5 and
                            not is_subset_pattern):
                            should_reject_sparse = True
                
                if should_reject_sparse:
                    continue
                
                # Accept if: (a) low similarity, OR (b) high consistency + distinct signatures, 
                # OR (c) envelope pattern detected
                is_discriminator = (
                    avg_inter_sim < 0.7 or 
                    (consistency_ratio > 0.9 and distinctness_ratio > 0.9) or
                    has_envelope_pattern
                )
                
                if is_discriminator:
                    # Use distinctness as correlation score when similarity is high
                    # Boost score if envelope pattern detected
                    score = max(1.0 - avg_inter_sim, distinctness_ratio)
                    if has_envelope_pattern:
                        score = max(score, 0.95)
                    correlation = {v: i for i, v in enumerate(all_values)}
                    candidates.append(DiscriminatorCandidate(
                        field_name=field_name,
                        values=values,
                        correlation=correlation,
                        correlation_score=score
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
    
    # Fallback: if multi-cluster but no discriminator, try single-cluster analysis
    # This handles cases where clustering threshold merged distinct types
    if len(clusters) > 1 and not discriminators:
        # Treat all documents as one cluster for discriminator detection
        all_sigs = [d.field_signature for d in documents]
        merged_sig = set().union(*all_sigs) if all_sigs else set()
        required_sig = set(all_sigs[0]).intersection(*all_sigs[1:]) if len(all_sigs) > 1 else (set(all_sigs[0]) if all_sigs else set())
        single_cluster = [SchemaCluster(
            id=0,
            documents=documents,
            merged_signature=merged_sig,
            required_fields=required_sig
        )]
        fallback_discriminators = _detect_discriminators(documents, single_cluster)
        if fallback_discriminators and fallback_discriminators[0].correlation_score > 0.3:
            discriminators = fallback_discriminators
            clusters = _recluster_by_discriminator(documents, fallback_discriminators[0])
    
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
