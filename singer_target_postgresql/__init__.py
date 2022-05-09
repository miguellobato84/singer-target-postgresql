#!/usr/bin/env python3

import argparse
import io
import os
import sys
import json
import threading
import http.client
import urllib
from datetime import datetime
import collections

import pkg_resources
from jsonschema.validators import Draft4Validator
import singer

logger = singer.get_logger()

def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        logger.debug('Emitting state {}'.format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()

def tosql(data, config):
    pks = config["keys"]
    pks_str = ", ".join(pks)
    entity = config["entity"]

    keys = data.keys()
    keys_nopk = [k for k in keys if k not in pks]
    keys_str = ", ".join(keys)
    values = [f"'{data[x]}'" for x in keys]
    values_str = ", ".join(values)
    values_upd = [f"{x}=EXCLUDED.{x}" for x in keys_nopk]
    values_upd_str = ", ".join(values_upd)
    return f"INSERT INTO {entity} ({keys_str}) VALUES({values_str}) ON CONFLICT ({pks_str}) DO UPDATE SET {values_upd_str};"
        
def persist_lines(config, lines):
    state = None
    schemas = {}
    key_properties = {}
    headers = {}
    validators = {}
    
    now = datetime.now().strftime('%Y%m%dT%H%M%S')

    # Loop over lines from stdin
    with open("data.sql", "w") as f:
        for line in lines:
            try:
                o = json.loads(line)
            except json.decoder.JSONDecodeError:
                logger.error("Unable to parse:\n{}".format(line))
                raise

            if 'type' not in o:
                raise Exception("Line is missing required key 'type': {}".format(line))
            t = o['type']

            if t == 'RECORD':
                if 'stream' not in o:
                    raise Exception("Line is missing required key 'stream': {}".format(line))
                if o['stream'] not in schemas:
                    raise Exception("A record for stream {} was encountered before a corresponding schema".format(o['stream']))

                # Get schema for this record's stream
                schema = schemas[o['stream']]

                # Validate record
                validators[o['stream']].validate(o['record'])

                # If the record needs to be flattened, uncomment this line
                # flattened_record = flatten(o['record'])
                
                f.write("{}\n".format(tosql(o['record'], config)))

                state = None
            elif t == 'STATE':
                logger.debug('Setting state to {}'.format(o['value']))
                state = o['value']
            elif t == 'SCHEMA':
                if 'stream' not in o:
                    raise Exception("Line is missing required key 'stream': {}".format(line))
                stream = o['stream']
                schemas[stream] = o['schema']
                validators[stream] = Draft4Validator(o['schema'])
                if 'key_properties' not in o:
                    raise Exception("key_properties field is required")
                key_properties[stream] = o['key_properties']
            else:
                raise Exception("Unknown message type {} in message {}"
                                .format(o['type'], o))
    
    return state


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file')
    args = parser.parse_args()


    if args.config:
        with open(args.config) as input:
            config = json.load(input)
    else:
        config = {}

    assert("keys" in config)
    assert("entity" in config)

    input = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    state = persist_lines(config, input)
        
    emit_state(state)
    logger.debug("Exiting normally")


if __name__ == '__main__':
    main()
