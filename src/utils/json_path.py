from typing import Dict, Any, List, Tuple

def traverse_json(obj: Any, path: str = "") -> List[Tuple[str, Any]]:
    """Helper method to traverse JSON and find all paths"""
    paths = []
    if isinstance(obj, dict):
        for key, value in obj.items():
            new_path = f"{path}.{key}" if path else key
            paths.append((new_path, value))
            paths.extend(traverse_json(value, new_path))
    elif isinstance(obj, list) and obj:
        paths.extend(traverse_json(obj[0], path))
    return paths

def find_entity_path(data: Dict, entity_identifier: str) -> str:
    """Find the path to entities in a JSON structure"""
    base_paths = []
    for path, value in traverse_json(data):
        if isinstance(value, list) and value:
            first_item = value[0]
            if isinstance(first_item, dict) and entity_identifier in first_item:
                base_paths.append(path)
    return min(base_paths, key=len) if base_paths else "" 