#!/usr/bin/env python3

import fiona
from flask import Flask
from geoalchemy2 import WKTElement, Geometry
import geojson
import geopandas as gpd
import glob
import json
import math
from multiprocessing import Process, Manager, Pool
import multiprocessing
import os
from osmdiff import OSMChange
import pandas as pd
import re
import requests
from shapely.geometry import Point, Polygon, shape
import subprocess
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert
import sys
import traceback

DEFAULT_CONCURRENT_JOB = 4
MAX_CONCURRENT_JOB = 6

app = Flask(__name__) # Needs defining at file global scope for thread-local sharing


gdf = None
AREA_CONST = 12436609510
engine=create_engine(
    "postgresql://openindoor-db-admin:admin123@openindoor-db:5432/openindoor-db")
unique_id = 'id'
chunksize = 100
update = 0
overpass_url='https://overpass-world.openindoor.io'
# pool = None

def json2geojson(data: json):
    result = subprocess.run(
        ["osmtogeojson"],
        text=True,
        input=json.dumps(data, indent=None, separators=(",",":")),
        capture_output=True
    )
    return geojson.loads(result.stdout)

    # https://songoku.openindoor.io/api/building/update/984911484
# curl http://localhost:8092/get-seq-number/minutely
@app.route("/get-seq-number/minutely")
def update_minutely_():
    osm_change = OSMChange()    
    osm_change.frequency = "minute"  # the default
    osm_change.get_state()  # retrieve current sequence ID
    sequence_number = osm_change.sequence_number
    response = app.response_class(
        response=json.dumps({"sequence_number": sequence_number}),
        status=200,
        mimetype='application/json'
    )
    return response
# curl http://localhost:8092/get-seq-number/hourly
@app.route("/get-seq-number/hourly")
def update_hourly_():
    osm_change = OSMChange()
    osm_change.frequency = "hour"  # the default
    osm_change.get_state()  # retrieve current sequence ID
    sequence_number = osm_change.sequence_number
    response = app.response_class(
        response=json.dumps({"sequence_number": sequence_number}),
        status=200,
        mimetype='application/json'
    )
    return response
# curl http://localhost:8092/get-seq-number/daily
@app.route("/get-seq-number/daily")
def update_daily_():
    osm_change = OSMChange()
    osm_change.frequency = "day"  # the default
    osm_change.get_state()  # retrieve current sequence ID
    sequence_number = osm_change.sequence_number
    response = app.response_class(
        response=json.dumps({"sequence_number": sequence_number}),
        status=200,
        mimetype='application/json'
    )
    return response

def chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))

def check_buildings(osm_change_, sequence_number_, proc = 4):
    created_buildings = [n for n in osm_change_.create if (n.tags is not None) and ('building' in n.tags) and (n.tags['building'] != 'no')]
    modified_buildings = [n for n in osm_change_.modify if (n.tags is not None) and ('building' in n.tags) and (n.tags['building'] != 'no')]
    building_ids = []
    for building in created_buildings:
        # building_id = int(building.attribs['id'])
        building_id = str(building).split()[0] + '/' + building.attribs['id']
        if building_id not in building_ids:
            building_ids.append(building_id)
    for building in modified_buildings:
        # building_id = int(building.attribs['id'])
        building_id = str(building).split()[0] + '/' + building.attribs['id']
        if building_id not in building_ids:
            building_ids.append(building_id)
    print(str(sequence_number_), '- number of buildings:', str(len(building_ids)), file=sys.stderr)
    # building_indoor_update
    # with Manager() as manager:
    #     manager = Manager()
        # building_indoor_update_ids = manager.list()
    building_indoor_ids = []
    print('proc:', str(proc), file=sys.stderr)
    # for id in building_ids:
    #     print('id:', id.split('/')[1])
    with Pool(processes=proc) as pool:
        # async_result = pool.starmap_async(
        #     func = check_building,
        #     iterable = ((id, sequence_number_) for id in building_ids)
        # )
        # print('building_ids:', building_ids)
        async_result = pool.map_async(
            func = check_building,
            iterable = building_ids,
        )
        async_result.wait(300)
        success = False
        try:
            success = async_result.successful()
            if success:
                # building_indoor_ids = [id for id in async_result.get() if id is not None]
                building_indoor_ids = update_buildings(
                    [id for id in async_result.get() if id is not None], sequence_number_
                )
        except ValueError:
            traceback.print_exc()

    # building_indoor_ids = update_buildings(building_indoor_ids)
    return {
        "success": success,
        "sequence_number": sequence_number_,
        "building_indoor_ids": building_indoor_ids
    }

def check_building(building_id_):
    # print(building_id_)
    print('check_building:', building_id_, file=sys.stderr)
    building_type = 'wr'
    match building_id_.split('/')[0]:
        case 'Way':
            building_type = "way"
        case 'Relation':
            building_type = "relation"

    building_id = building_id_.split('/')[1]
    building_indoor_query = '[out:json];' + building_type + '(id:' + building_id + ');map_to_area->.b;way(area.b)[indoor];(._;>;);out geom;'
    print('building_indoor_query:', building_indoor_query, file=sys.stderr)
    building_indoor_request = requests.get(overpass_url + '/api/interpreter?data=' + building_indoor_query)
    indoor_len = 0
    if building_indoor_request.status_code < 400:
        try:
            indoor = building_indoor_request.json()
            indoor_ways = [e for e in indoor["elements"] if e["type"] == "way"]
            indoor["elements"] = indoor_ways
            indoor_len = len(indoor["elements"])
        except Exception:
            print(traceback.format_exc())
    if (indoor_len > 5):
        print('building with', str(indoor_len), 'indoor data:', building_id_, file=sys.stderr)
        return building_id_
    else:
        return None

def update_buildings(building_ids, sequence_number_):
    # print('building_ids:', json.dumps(building_ids), file=sys.stderr)
    effective_building_ids = [id for id in building_ids if id is not None]
    print('effective_building_ids:', json.dumps(effective_building_ids), file=sys.stderr)
    for building_id_ in effective_building_ids:
        building_type = 'wr'
        match building_id_.split('/')[0]:
            case 'Way':
                building_type = "way"
            case 'Relation':
                building_type = "relation"

        building_id = building_id_.split('/')[1]

        building_indoor_request = requests.get(overpass_url + '/api/interpreter?data=[out:json];' + building_type + '(id:' + building_id + ');map_to_area->.b;way(area.b)[indoor];(._;>;);out geom;')
        indoor = building_indoor_request.json()
        indoor_ways = [e for e in indoor["elements"] if e["type"] == "way"]
        indoor["elements"] = indoor_ways
        indoor_len = len(indoor["elements"])
        if (indoor_len > 5):
            requests.get('https://songoku.openindoor.io/api/building/update/' + building_type + '/' + building_id + '/' + str(sequence_number_))
    return effective_building_ids
    # if building_id is None:
    #     return
    # building_indoor_request = requests.get(overpass_url + '/api/interpreter?data=[out:json];way(id:' + str(building_id) + ');map_to_area->.b;way(area.b)[indoor];(._;>;);out geom;')
    # indoor = building_indoor_request.json()
    # indoor_ways = [e for e in indoor["elements"] if e["type"] == "way"]
    # indoor["elements"] = indoor_ways
    # indoor_len = len(indoor["elements"])
    # if (indoor_len > 5):
    #     requests.get('https://songoku.openindoor.io/api/building/update/' + str(building_id))

# Building with 8 indoor data: 984911484
# curl http://localhost:8092/update/minutely/5026715
# curl https://pikachu.openindoor.io/update/minutely/5026715
@app.route("/update/minutely/<int:sequence_number>")
def update_minutely(sequence_number):
    return _update_minutely(sequence_number, proc = DEFAULT_CONCURRENT_JOB)

@app.route("/update/minutely/<int:sequence_number>/<int:proc>")
def update_minutely_proc(sequence_number, proc):
    return _update_minutely(sequence_number, proc = proc)

def _update_minutely(sequence_number, proc):
    osm_change = OSMChange()
    osm_change.frequency = "minute"  # the default
    if sequence_number is not None:
        osm_change.sequence_number = sequence_number
    osm_change.retrieve()
    # building_indoor_ids = check_buildings(osm_change, sequence_number)
    result = check_buildings(osm_change, sequence_number, proc = min(proc, MAX_CONCURRENT_JOB))
    response = app.response_class(
        # response=json.dumps({"building_indoor_update_ids": building_indoor_ids}),
        response=json.dumps(result),
        # response=json.dumps({"state": "OK"}),
        status=200,
        mimetype='application/json'
    )
    return response

# curl http://localhost:8092/update/daily/3510
@app.route("/update/daily/<int:sequence_number>")
def update_daily(sequence_number):
    print('Dealing with daily update sequence_number :', str(sequence_number), file=sys.stderr)
    osm_change = OSMChange()
    osm_change.frequency = "day"  # the default
    if sequence_number is not None:
        osm_change.sequence_number = sequence_number
    osm_change.retrieve()
    building_indoor_update_ids = check_buildings(osm_change, sequence_number)
    response = app.response_class(
        response=json.dumps({"building_indoor_update_ids": building_indoor_update_ids}),
        status=200,
        mimetype='application/json'
    )
    return response
# curl http://localhost:8092/update/hourly/84195
@app.route("/update/hourly/<int:sequence_number>")
def update_hourly(sequence_number):
    osm_change = OSMChange()
    osm_change.frequency = "hour"  # the default
    if sequence_number is not None:
        osm_change.sequence_number = sequence_number
    osm_change.retrieve()
    building_indoor_update_ids = check_buildings(osm_change, sequence_number)
    response = app.response_class(
        response=json.dumps({"building_indoor_update_ids": building_indoor_update_ids}),
        status=200,
        mimetype='application/json'
    )
    return response
if __name__ == '__main__':
    app.run(host="0.0.0.0")