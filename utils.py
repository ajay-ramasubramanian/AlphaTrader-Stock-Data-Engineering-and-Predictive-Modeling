import avro

scope = "user-library-read \
         user-follow-read \
         playlist-read-private \
         playlist-read-collaborative \
         user-top-read \
         user-read-recently-played \
         playlist-modify-public \
         playlist-modify-private \
         user-read-private \
         user-read-email"


def load_schema(schema_path):
        with open(schema_path, "rb") as schema_file:
            return avro.schema.parse(schema_file.read())