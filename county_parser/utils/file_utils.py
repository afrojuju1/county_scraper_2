"""File system utilities."""

import os
from pathlib import Path
from typing import Union


def get_file_size(file_path: Union[str, Path]) -> dict:
    """Get file size information."""
    
    path = Path(file_path)
    if not path.exists():
        return {"error": "File not found"}
    
    size_bytes = path.stat().st_size
    
    return {
        "size_bytes": size_bytes,
        "size_kb": round(size_bytes / 1024, 2),
        "size_mb": round(size_bytes / (1024 * 1024), 2),
        "size_gb": round(size_bytes / (1024 * 1024 * 1024), 2)
    }


def ensure_directory(dir_path: Union[str, Path]) -> Path:
    """Ensure directory exists, create if it doesn't."""
    
    path = Path(dir_path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_available_space(dir_path: Union[str, Path]) -> dict:
    """Get available disk space for a directory."""
    
    path = Path(dir_path)
    if not path.exists():
        path = path.parent
    
    stat = os.statvfs(path)
    
    # Available space
    available_bytes = stat.f_bavail * stat.f_frsize
    
    return {
        "available_bytes": available_bytes,
        "available_gb": round(available_bytes / (1024 * 1024 * 1024), 2)
    }
