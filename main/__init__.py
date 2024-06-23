from . import getLM
import json

def read_conf(path):
        with open (path) as json_file:
            conf_dict = json.load(json_file)
            return conf_dict

path = "confs\cfg.json"
conf = read_conf(path)

getLM.SetConfig.set(getLM.SetConfig, conf)