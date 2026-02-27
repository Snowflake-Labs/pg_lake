"""JSON normalisation, read/write, and assertion utilities."""

import json

from deepdiff import DeepDiff


def normalize_json(value):
    """
    Recursively convert JSON string representations to dictionaries and handle escape characters in keys.
    """
    if isinstance(value, dict):
        new_dict = {}
        for k, v in value.items():
            # Handle escape characters in keys
            new_key = k.replace('\\"', '"')
            new_dict[new_key] = normalize_json(v)
        return new_dict
    elif isinstance(value, list):
        return [normalize_json(i) for i in value]
    elif isinstance(value, str):
        try:
            # Try to convert string to dictionary if it's valid JSON
            parsed_value = json.loads(value)
            if isinstance(parsed_value, (dict, list)):
                return normalize_json(parsed_value)
            return value
        except json.JSONDecodeError:
            return value
    else:
        return value


# Function to read JSON from a file
def read_json(file_path):
    with open(file_path, "r") as file:
        return json.load(file)


def read_binary(file_path):
    with open(file_path, "rb") as file:
        return file.read()


def write_to_file(file_path, content):
    with open(file_path, "w") as file:
        json.dump(content, file, indent=4)


def write_json_to_file(file_path, data):
    with open(file_path, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=4)


def assert_valid_json(result):

    # Check if the result is a dictionary, which means it's already a valid JSON object
    assert isinstance(result, dict), "The result is not a valid JSON object"

    # Check if the dictionary can be serialized back to JSON
    try:
        json_string = json.dumps(result)
        is_valid_json = True
    except ValueError:
        is_valid_json = False

    assert is_valid_json, "The result is not a valid JSON:" + result


# make sure two json objects have the exact same content, the order
# is not relevant
def assert_jsons_equivalent(json_obj1, json_obj2):

    # handle small escape char differences in representations
    normalized_1, normalized_2 = normalize_json(json_obj1), normalize_json(json_obj2)

    diff = DeepDiff(normalized_1, normalized_2, ignore_order=True)
    if diff:
        print("Differences found:")
        print(diff)
        assert False
    else:
        assert True
