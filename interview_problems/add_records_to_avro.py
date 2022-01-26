from fastavro import writer, parse_schema

schema = {
    'doc': 'A weather reading.',
    'name': 'Weather',
    'namespace': 'test',
    'type': 'record',
    'fields': [
        {'name': 'station', 'type': 'string'},
        {'name': 'time', 'type': 'long'},
        {'name': 'temp', 'type': 'int'},
    ],
}
parsed_schema = parse_schema(schema)

records = [
    {u'station': u'011990-99999', u'temp': 0, u'time': 1433269388},
    {u'station': u'011990-99999', u'temp': 22, u'time': 1433270389},
    {u'station': u'011990-99999', u'temp': -11, u'time': 1433273379},
    {u'station': u'012650-99999', u'temp': 111, u'time': 1433275478},
]

# Write initial records
with open('avrofiles/weather.avro', 'wb') as out:
    writer(out, parsed_schema, records)


more_records = [
    {u'station': u'011990-99998', u'temp': 0, u'time': 1533269388},
    {u'station': u'011990-99998', u'temp': 22, u'time': 1533270389},
    {u'station': u'011990-99999', u'temp': -10, u'time': 1533273379},
    {u'station': u'012650-99999', u'temp': 19, u'time': 1533275478},
]

# Write some more records
with open('avrofiles/weather.avro', 'a+b') as out:
    writer(out, parsed_schema, more_records)
