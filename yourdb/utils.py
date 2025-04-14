def is_valid_entity_name(entity_name: str) -> bool:
    """
    Check if the entity name is valid.
    :param entity_name: Name of the entity to check.
    :return: True if valid, False otherwise.
    """
    return isinstance(entity_name, str) and entity_name.strip() != ""


def is_valid_schema(entity_schema: dict) -> bool:
    """
    Check if the entity schema is a valid dictionary.
    :param entity_schema: Schema of the entity to check.
    :return: True if valid, False otherwise.
    """
    return isinstance(entity_schema, dict) and bool(entity_schema)
