__ALL__ = ["SPACEONE_TASK_SCHEMA"]

SPACEONE_TASK_SCHEMA = {
    "type": "object",
    "properties": {
        "name": {"type": "string"},
        "version": {"type": "string", "enum": ["v1"]},
        "executionEngine": {"type": "string"},
        "stages": {"type": "array", "items": {"$ref": "#/definitions/task"}}
    },
    "definitions": {
        "task": {
            "type": "object",
            "properties": {
                "locator": {"type": "string",
                            "enum": ["SERVICE", "MANAGER"]},
                "name": {"type": "string"},
                "metadata": {"type": "object"},
                "method": {"type": "string"},
                "params": {"type": "object"}
            },
            "required": ["locator", "name", "metadata", "method", "params"]
        }
    },
    "required": ["stages"]
}


"""
instance = {
    "name": "Publish",
    "version": "v1",
    "executionEngine": "BasePipelineWorker",
    "stages": [
        {
            "locator": "SERVICE",
            "name": "SupervisorService",
            "metadata": [["token", "1234"]],
            "method": "publish",
            "params": {"params": {"id": "aaaa"}}
        }
    ]
}

instance = {
    "name": 1
}

instance = {
    "name": "Publish",
    "version": "v1",
    "executionEngine": "BasePipelineWorker",
    "stages": [
        {
            "locator": "MANAGER",
            "name": "SupervisorService",
            "params": {"name": "aaaa", "metadata": 111}
        }
    ]
}


print(validate(instance, schema=SPACEONE_TASK_SCHEMA))
"""
