import json
import urllib.request
from urllib.parse import urljoin


def load_json_config(url):
    config = json.load(urllib.request.urlopen(url))

    config_string = json.dumps(config)

    # replace "base" with the base path of where the config is located
    base = urljoin(url, './')
    config_string = config_string.replace('${base}/', base)
    config_string = config_string.replace('${base}', base)

    return json.loads(config_string)
