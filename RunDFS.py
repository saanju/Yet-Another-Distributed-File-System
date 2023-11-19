import subprocess
import json

config = ""
with open("config.json", "r") as f:
    config = f.read()
config = json.loads(config)


for datanode in config["containers"]:
    process = subprocess.Popen(
        f"docker exec -d -it {datanode['Name']} python3 /usr/files/DataNode.py", shell=True, stdout=subprocess.PIPE)
