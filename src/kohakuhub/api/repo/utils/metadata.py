"""Metadata extraction from repository files.

This module provides utilities to parse metadata (like tags, license, and
pipeline type) from special files like README.md (YAML frontmatter).
"""

import json
import yaml
from typing import Optional, Dict, Any

from kohakuhub.db import Repository
from kohakuhub.logger import get_logger

logger = get_logger("METADATA")

def parse_readme_metadata(content: str) -> Dict[str, Any]:
    """Parse YAML frontmatter from README.md content.
    
    Args:
        content: README.md file content
        
    Returns:
        Dictionary of extracted metadata
    """
    if not content.startswith("---"):
        return {}
        
    parts = content.split("---", 2)
    if len(parts) < 3:
        return {}
        
    yaml_text = parts[1]
    try:
        data = yaml.safe_load(yaml_text)
        if isinstance(data, dict):
            return data
    except Exception as e:
        logger.warning(f"Failed to parse YAML frontmatter: {e}")
        
    return {}

def update_repository_metadata(repo: Repository, metadata: Dict[str, Any]) -> bool:
    """Update repository fields based on extracted metadata.
    
    Args:
        repo: Repository database object
        metadata: Metadata dictionary (from README.md)
        
    Returns:
        True if changed, False otherwise
    """
    changed = False
    
    # 1. License
    license_val = metadata.get("license")
    if license_val and repo.license != license_val:
        repo.license = license_val
        changed = True
        
    # 2. Pipeline Tag
    pipeline_tag = metadata.get("pipeline_tag")
    if pipeline_tag and repo.pipeline_tag != pipeline_tag:
        repo.pipeline_tag = pipeline_tag
        changed = True
        
    # 3. Tags
    tags = metadata.get("tags", [])
    if isinstance(tags, list):
        tags_json = json.dumps(tags)
        if repo.tags != tags_json:
            repo.tags = tags_json
            changed = True
            
    if changed:
        repo.save()
        logger.info(f"Updated metadata for repository {repo.full_id}")
        
    return changed
