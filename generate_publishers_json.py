import databento as db
import argparse
import json

parser = argparse.ArgumentParser(
    description="Extract publisher metadata from databento into a JSON file"
)

parser.add_argument(
    "--output",
    help="Name of the JSON file to generate",
    required=True
)

parser.add_argument(
    "--api-key",
    help="API Key to instantiate the client",
    required=True
)

args = parser.parse_args()


client = db.Historical(args.api_key)
publishers = client.metadata.list_publishers()
with open(args.output, 'w') as f:
    json.dump(publishers, f, indent=2)
