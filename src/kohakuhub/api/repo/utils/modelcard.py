"""Model Card integration for KohakuHub.

Uses huggingface_hub.ModelCard to manage README metadata and evaluation metrics.
"""

import json
from typing import Dict, Any, Optional
from huggingface_hub import ModelCard, ModelCardData
from kohakuhub.db import Repository, File
from kohakuhub.logger import get_logger

logger = get_logger("MODEL_CARD")

def generate_default_card(repo: Repository, metrics: Optional[Dict[str, Any]] = None) -> str:
    """Generate a default ModelCard for a repository."""
    card_data = ModelCardData(
        language="en",
        license=repo.license or "unknown",
        library_name=repo.pipeline_tag,
        datasets=["versioned-dataset"], # Example link to datasets
        metrics=metrics or {}
    )
    
    card = ModelCard.from_template(
        card_data,
        model_id=repo.full_id,
        repo_type=repo.repo_type,
    )
    return str(card)

def update_card_with_metrics(content: str, metrics: Dict[str, Any]) -> str:
    """Update existing README content with new evaluation metrics."""
    try:
        card = ModelCard(content)
        if not card.data:
            card.data = ModelCardData()
        
        # Merge metrics
        current_metrics = getattr(card.data, "metrics", {})
        if not isinstance(current_metrics, dict):
            current_metrics = {}
            
        current_metrics.update(metrics)
        card.data.metrics = current_metrics
        return str(card)
    except Exception as e:
        logger.warning(f"Failed to parse model card for metric update: {e}")
        return content

def sync_card_to_db(repo: Repository, card_content: str):
    """Sync metadata from ModelCard back to repository database fields."""
    try:
        card = ModelCard(card_content)
        if card.data:
            from kohakuhub.api.repo.utils.metadata import update_repository_metadata
            # Reuse existing metadata updater
            update_repository_metadata(repo, card.data.to_dict())
    except Exception as e:
        logger.error(f"Failed to sync card to DB: {e}")
