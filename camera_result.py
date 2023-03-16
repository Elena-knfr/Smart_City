"""
This is the Newmarket Camera endpoint for the API
- `/newmarket_camera`: returns recent drone camera updates
- `/camfeed`: return the output picture of the camera
"""

from __future__ import absolute_import
from flask_restful import fields, marshal
from flask import abort, send_file
from .apiAuth import tokenAuth
from flask_restful_swagger import swagger
# import paho.mqtt.client as subscriber_client
# import paho.mqtt.publish as publisher_client
# The platform module is used in recognizing the host operating system.
# import platform
import time as clock
import thread
import os
import glob
from .utils import TimedResource, ObjectIdField,\
    FixedField
from os.path import basename
from elasticsearch import Elasticsearch
from datetime import datetime, timedelta
from pytz import timezone
import shutil


NEWMARKET_RESULT_CAM_FIELDS = {
    # "available_spots": fields.Integer,
    "timestamp": fields.String,
    "counts": fields.Integer
}

NEWMARKET_RESULT_CAM_FIELDS_GEOJSON = {
    "type": FixedField("Feature"),
    "properties": NEWMARKET_RESULT_CAM_FIELDS
}

CAMERA_COL = 'NEWMARKET_CAMERAS'




class NewmarketResultBaseResource(TimedResource):
    collection = CAMERA_COL
    # collection = ''
    def query(self):
        """find all entries for freeflow"""
        es = Elasticsearch([{'host': '10.12.6.34', 'port': 9200}])
        current_date = datetime.now(timezone('America/Toronto'))
        current_timestamp = int(current_date.strftime('%s'))
        current_date_in_string = current_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        day = current_date_in_string[8:10]
        month = current_date_in_string[5:7]
        year = current_date_in_string[0:4]
        date = year + month + day
        # collection = "newmarket_total" + date
        collection = 'newmarket_total_test'
        totalHits = es.search(index=collection, doc_type="data",
                              body={
                                  "query": {
                                      'match_all': {}
                                  }, "sort": {"timestamp": "desc"}})
        size = totalHits['hits']['total']
        if size != 0:
            totalHits = es.search(index=collection, doc_type="data",
                                  body={
                                      "query": {
                                          'match_all': {}
                                      }, "sort": {"timestamp": "desc"}}, size=10
                                  )
            docs = totalHits['hits']['hits']
            result = []
            i = 0
            for data in docs:
                del data["_id"]
                result.insert(i, data['_source'])
                i += 1
            return [doc for doc in result]


class NewmarketResultResource(NewmarketResultBaseResource):
    collection = CAMERA_COL


class marketResultCamera(NewmarketResultResource):
    """
    Returns info for newmart camera
    """
    # @tokenAuth.login_required
    def get(self):
        """Uses the super class method to get the data"""
        result = marshal(self.query(), NEWMARKET_RESULT_CAM_FIELDS_GEOJSON)
        geojson = {
            "type": "FeatureCollection",
            "features": result
        }
        return geojson, 200

'''
class MarketCameraFeed(NewmarketResource):
    """
        Send the picture of the given camera
    """
    @swagger.operation(
        notes='nonregistered user cannot access this data',
        nickname='getCameraFeed',
        responseMessages=[
            {
                "code": 200,
                "message": "Success"
            },
            {
                "code": 401,
                "message": "Only registered user can access this data"
            }
        ])
    @tokenAuth.login_required
    def get(self, file_name):
        PATH = "/home/ubuntu/cvst-images/NewmarketCamera/"
        files = [os.path.basename(x) for x in glob.glob(
            PATH + "/*.jpg")]
        files = sorted(files, reverse=True)
        file_name1 = max(glob.iglob("/home/ubuntu/cvst-images/NewmarketCameraSub/cam1/*.jpg"), key=os.path.getctime)
        file_name2 = max(glob.iglob("/home/ubuntu/cvst-images/NewmarketCameraSub/cam2/*.jpg"), key=os.path.getctime)
        out1 = os.path.splitext(basename(file_name1))[0]
        out2 = os.path.splitext(basename(file_name2))[0]
        shutil.copy2("/home/ubuntu/cvst-images/NewmarketCameraSub/cam1/%s.jpg" % out1, "/home/ubuntu/cvst-images/NewmarketCamera/out_modified0.jpg")
        shutil.copy2("/home/ubuntu/cvst-images/NewmarketCameraSub/cam2/%s.jpg" % out2, "/home/ubuntu/cvst-images/NewmarketCamera/out_modified1.jpg")
        if (file_name in files):  # getting <feed_num> timestamps
            return send_file(PATH + "/" + file_name, mimetype='image/jpg')
        else:
            abort(500, message="Internal error")
            '''
