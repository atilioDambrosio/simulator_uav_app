import json

def readJson(file_name=None):
    with open(file_name, "r") as file:
        data = json.load(file)
    return data

def write_json(file_path=None, data=None):
    json_object = json.dumps(data, indent=4)
    with open(file_path, "w") as outfile:
        outfile.write(json_object)
    outfile.close()