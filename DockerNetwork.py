import subprocess
import ast
import json

process = subprocess.Popen(
    "docker network inspect yet-another-distributed-file-system_default", shell=True, stdout=subprocess.PIPE)

output, error = process.communicate()

network = output.decode('utf-8')
# network = ast.literal_eval(network)
network = json.loads(network)[0]

config_file = json.loads(open("config.json", "r").read())
config_file["containers"] = []

for container in network["Containers"].values():
    if (container["Name"].endswith("1")):
        config_file["namenode"] = container
    elif (container["Name"].endswith("2")):
        config_file["client"] = container
    else:
        config_file["containers"].append(container)

with open("config.json", "w") as f:
    f.write(json.dumps(config_file))
