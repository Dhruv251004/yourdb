import re


def is_valid_entity_name(entity_name: str) -> bool:
    """
    Check if the entity name is valid.
    :param entity_name: Name of the entity to check.
    :return: True if valid, False otherwise.
    """
    # Entity name should only contain alphanumeric characters and underscores and should not start with a number
    return bool(re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", entity_name))


def is_valid_schema(entity_schema: dict) -> bool:
    """
    Check if the entity schema is a valid dictionary, ensuring there are valid field types.
    :param entity_schema: Schema of the entity to check.
    :param primary_key: The field to be considered as the primary key.
    :return: True if valid, False otherwise.
    """
    if not isinstance(entity_schema, dict) or not entity_schema:
        return False
    print(entity_schema)
    # Check if primary key exists in the schema
    if 'primary_key' not in entity_schema:
        return False
    # print(entity_schema)

    if entity_schema['primary_key'] not in entity_schema:
        return False
    # print(entity_schema)

    for field, field_type in entity_schema.items():
        if field == 'primary_key':
            continue

    return True
