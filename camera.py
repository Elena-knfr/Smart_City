from __future__ import absolute_import
from flask_restful import fields, marshal
from flask import abort, send_file
from .apiAuth import tokenAuth
from flask_restful_swagger import swagger
import paho.mqtt.client as subscriber_client
import paho.mqtt.publish as publisher_client
# The platform module is used in recognizing the host operating system.
import platform
import time as clock
from time import sleep
import thread
import os
import glob
from .utils import TimedResource, ObjectIdField,\
    FixedField
from os.path import basename
import shutil

ADDRESS = "142.150.199.252"
PORT = 1883
TOPIC1 = 'update/cam1'
TOPIC2 = 'update/cam2'


NEWMARKET_CAM_FIELDS = {
    "_id": ObjectIdField,
    # "coordinates": fields.RaW,
    "name": fields.String,
    "coordinates": fields.List(fields.Float, attribute="loc.coordinates"),
    # "number_of_subscribed_images_camera1": fields.Integer(attribute="c1"),
    # "number_of_subscribed_images_camera2": fields.Integer(attribute="c2"),
    "number_of_subscribed_images_camera": fields.Integer(attribute="count"),
    "timestamp": fields.Integer(attribute="timestamp"),
    "id": fields.Integer
}

NEWMARKET_CAM_FIELDS_GEOJSON = {
    "type": FixedField("Feature"),
    "geometry": {
        "type": FixedField("Point"),
        "coordinates": fields.List(fields.Float, attribute="loc.coordinates")
    },
    "properties": NEWMARKET_CAM_FIELDS
}

CAMERA_COL = 'NEWMARKET_CAMERAS'


class Subscriber:
    # The topic that publisher uses to transfer the images and subscriber needs to use for receiving it from the broker.
    subscriber_topic = None
    # Broker IP address.
    broker_address = None
    # The port that broker is listening on for incoming connections. The default is 1883 for Mosquitto broker.
    broker_port = None
    # Is used to have a counter for the number of images that have been received so far.
    counter = ''
    '''
        @input: address, port, topic, destination
        @role: Constructor method
    '''
    def __init__(self, address, port, topic):
        # Broker IP address - Our broker: 142.150.199.219
        self.broker_address = str(address)
        # Broker port number - Our broker: 1883
        self.broker_port = int(port)
        # Topic for subscription - Our example: update/cam1 or update/cam2
        self.subscriber_topic = str(topic)
    '''
        @input: client, userdate, flags, rc
        @role: Is being used to pair with the on_connect method on the broker to exchange the subscription topic.
    '''
    def on_connect(self, client, userdata, flags, rc):
        # Subscribing for contents with the specified topic.
        client.subscribe(self.subscriber_topic)
    '''
        @input: client, userdata, message
        @role: Is being used to pair with the on_message method on the broker to exchange the message with the topic of interest.
    '''
    def on_message(self, client, userdata, message):
        # Obtaining the message from broker.
        self.counter = str(message.payload)
        # Disconnecting from the broker.
        client.disconnect()
    '''
        @role: Subscribing to the broker and receiving the contents specified by the topics of interest.
    '''
    def subscribe(self):
        # Creating an MQTT client object.
        client = subscriber_client.Client()
        # An infinite loop to keep the subscriber alive.
        while True:
            # Connecting to the broker.
            client.connect(self.broker_address, self.broker_port, 60)
            # Receiving the message.
            client.on_message = self.on_message
            # Providing the topic of interest.
            client.on_connect = self.on_connect
            # Keep the subscriber running until the whole message is received.
            client.loop_forever()


class NewmarketBaseResource(TimedResource):
    collection = CAMERA_COL
    # collection = ''
    def query(self):
        """find all entries for freeflow"""
        result = self.mongo.db[self.collection].find(
                {}, {'_id': 0})
        return [doc for doc in result]


class NewmarketResource(NewmarketBaseResource):
    collection = CAMERA_COL


class marketCamera(NewmarketResource):
    """
    Returns info for newmart camera
    """
    # @tokenAuth.login_required
    def get(self):
        """Uses the super class method to get the data"""
        sc1 = Subscriber(ADDRESS, PORT, TOPIC1)
        sc2 = Subscriber(ADDRESS, PORT, TOPIC2)
        thread.start_new_thread(sc1.subscribe, ())
        thread.start_new_thread(sc2.subscribe, ())
        # c1 = sc1.counter
        # c2 = sc2.counter
        c1 = ''
        c2 = ''
        i=0
        if i < 3:
            if (c1, c2) == ('', ''):
                c1 = sc1.counter
                c2 = sc2.counter
                sleep(10)
                i +=1
        elif i >= 3 and (c1, c2) == ('', ''):
            c1 = 0
            c2 = 0
        else:
            pass
        # query = self.mongo.db[self.collection].find()
        # counts = {"c1": c1, "c2":c2}
        # data = dict(query, **counts)
        self.mongo.db[self.collection].update({"id": 1}, {"$set": {"count": c1}})
        self.mongo.db[self.collection].update({"id": 2}, {"$set": {"count": c2}})
        # db.NEWMARKET_CAMERAS.update({"id": 1}, {"$set": {"count44": c1}})
        result = marshal(self.query(), NEWMARKET_CAM_FIELDS_GEOJSON)
        geojson = {
            "type": "FeatureCollection",
            "features": result
        }
        return geojson, 200


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
        # os.chdir(PATH)
        # files = glob.glob(PATH + '/*.jpg')
        # files.sort(key=lambda fn: os.path.getctime(os.path.join(PATH, fn)))
        files = [os.path.basename(x) for x in glob.glob(PATH + "/*.jpg")]
        files = sorted(files, reverse=True)
        def cam1():
            PATH1 = "/home/ubuntu/cvst-images/NewmarketCameraSub/cam1"
            os.chdir(PATH1)
            files = glob.glob(PATH1 + '/*.jpg')
            files.sort(key=lambda fn: os.path.getctime(os.path.join(PATH1, fn)))
            while True:
                for file in files:
                    file_name1= file[49:]
                    out1 = os.path.splitext(basename(file_name1))[0]
                    shutil.copy2("/home/ubuntu/cvst-images/NewmarketCameraSub/cam1/%s.jpg" % out1,
                         "/home/ubuntu/cvst-images/NewmarketCamera/out_modified0.jpg")
                    sleep(10)
        def cam2():
            PATH2 = "/home/ubuntu/cvst-images/NewmarketCameraSub/cam2"
            os.chdir(PATH2)
            files = glob.glob(PATH2 + '/*.jpg')
            files.sort(key=lambda fn: os.path.getctime(os.path.join(PATH2, fn)))
            while True:
               for file in files:
                   file_name2 = file[49:]
                   out2 = os.path.splitext(basename(file_name2))[0]
                   shutil.copy2("/home/ubuntu/cvst-images/NewmarketCameraSub/cam2/%s.jpg" % out2,
                         "/home/ubuntu/cvst-images/NewmarketCamera/out_modified1.jpg")
                   sleep(10)
        thread.start_new_thread(cam1, ())
        thread.start_new_thread(cam2, ())
        if (file_name in files):  # getting <feed_num> timestamps
            return send_file(PATH + "/" + file_name, mimetype='image/jpg')
        else:
            abort(500, message="Internal error")
