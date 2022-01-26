from fastavro import parse_schema, writer

'''
Question

How do you parse a manual avro schema?

'''

schema = parse_schema(
        {
            "type": "record",
            "name": "click_event",
            "namespace": "com.sbommireddy.lesson3.exercise",
            "fields": [
                {"name": "email", "type": "string"},
                {"name": "timestamp", "type": "string"},
                {"name": "uri", "type": "string"},
                {"name": "number", "type": "int"},
            ],
        }
    )

print(type(schema)) ## dict
## print(schema)
print(schema['fields'])

## /Users/sbommireddy/.pyenv/versions/3.9.0/bin/python3.9 -m pip install --upgrade pip
