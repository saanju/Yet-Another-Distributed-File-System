import subprocess
import json

config = ""
with open("config.json", "r") as f:
    config = f.read()
config = json.loads(config)
