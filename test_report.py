import json
import smtplib
import datetime
import schedule
from time import sleep
from email.message import Message
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from elasticsearch import Elasticsearch
from datetime import datetime, timedelta
from pytz import timezone
import numpy as np
import pytz
import os
import glob
from os.path import basename
TOTAL_SPOTS = 255

def send_email(title, Content, recipient):
    '''
    if not recipient or not isinstance(recipient, list):
        return False
    '''
    gmail_user = "" # my email
    gmail_pwd = ""
    TEXT = Content
    FROM = gmail_user
    TO = recipient = "" # email of newmarket
    SUBJECT = "CVST SERVER: " + title
    message ="\r\n".join([
       "FROM: ", # my email
       "TO: ", # my test email
       "SUBJECT: Just a message",
       "",
       TEXT
       ])
    try:
        server = smtplib.SMTP('smtp.gmail.com:587')
        server.ehlo()
        server.starttls()
        server.login(gmail_user, gmail_pwd)
        server.sendmail(FROM, TO, message)
        print("email is successfully sent")
        server.quit()
    except:
        print("unable to send email")

es = Elasticsearch([{'host': '', 'port': 9200}]) # set host
current_date = datetime.now(timezone('America/Toronto'))
current_timestamp = int(current_date.strftime('%s'))
current_date_in_string = current_date.strftime("%Y-%m-%dT%H:%M:%SZ")
day = current_date_in_string[8:10]
month = current_date_in_string[5:7]
year = current_date_in_string[0:4]
date = year + month + day
collection = 'newmarket_total_test' + date

collection1 = 'newmarket_data2'
collection2 = 'newmarket_cam2test'
collections =[collection1, collection2]

latest_timeEntry = es.search(index = "newmarket_total_test", doc_type="data",
                             body={
                                 "query": {
                                     'match_all': {}
                                 }, "sort": {"timestamp": "desc"}}, size=1
                             )

res = latest_timeEntry['hits']['hits']
if res is not None:
    for data in res:
        latest_pubTime = str(data['_source']["timestamp"])
        total_count = int(str(data['_source']["counts"]))
        latest_pubTime_total = datetime.strptime(latest_pubTime, '%Y-%m-%dT%H:%M:%S')
else:
    latest_pubTime = None
    latest_pubTime_total = None
    total_count = 0

pubTimes =[]
if latest_pubTime_total is not None:
    for i in np.arange(len(collections)):
        uncounted = es.search(index=collections[i], doc_type="data",
                              body={
                                  "query": {
                                      "range": {
                                          "timestamp": {"gte": latest_pubTime}}
                                  }, "sort": {"timestamp": "asc"}}, size=1
                              )
        res = uncounted['hits']['hits']
        if res is not None:
            for data in res:
                pubTime = str(data['_source']["timestamp"])
                # latest_pubTime = datetime.strptime(latest_pubTime, '%Y-%m-%dT%H:%M:%S')
                pubTimes.append(pubTime)

result1 = []
result2= []

if len(pubTimes) > 1:
    oldest_pubTime1 = datetime.strptime(pubTimes[0], '%Y-%m-%dT%H:%M:%S')
    oldest_pubTime2 = datetime.strptime(pubTimes[1], '%Y-%m-%dT%H:%M:%S')
    oldest_pubTime = max(pubTimes[0], pubTimes[1])
    totalHits = es.search(index="newmarket_data2", doc_type="data",
                          body={
                              "query": {
                                  "range": {
                                      "timestamp": {"gte": oldest_pubTime}}}, "sort": {"timestamp": "asc"}})
    size1 = totalHits['hits']['total']
    if size1 != 0:
        totalHits = es.search(index="newmarket_data2", doc_type="data",
                              body={
                                  "query": {
                                      "range": {
                                          "timestamp": {"gte": oldest_pubTime}}}, "sort": {"timestamp": "asc"}}, size = size1)
        docs = totalHits['hits']['hits']
        result1 = []
        i = 0
        for data in docs:
            del data["_id"]
            result1.insert(i, data['_source'])
            i += 1
    totalHits = es.search(index="newmarket_cam2test", doc_type="data",
                          body={
                              "query": {
                                  "range": {
                                      "timestamp": {"gte": pubTimes[0]}}}, "sort": {"timestamp": "asc"}})
    size2 = totalHits['hits']['total']
    if size2 != 0:
        totalHits = es.search(index="newmarket_data2", doc_type="data",
                              body={
                                  "query": {
                                      "range": {
                                          "timestamp": {"gte": pubTimes[0]}}}, "sort": {"timestamp": "asc"}},
                              size=size1)
        docs = totalHits['hits']['hits']
        result2 = []
        i = 0
        for data in docs:
            del data["_id"]
            result2.insert(i, data['_source'])
            i += 1

    if len(result1) > 0  and len(result2) > 0:
        for i in np.arange(min(len(result1), len(result2))):
            aggregated = []
            aggregated.append(result1[i]["counts"])
            aggregated.append(result1[i]["direction"])
            aggregated.append(result1[i]["timestamp"])
            aggregated.append(result2[i]["counts"])
            aggregated.append(result2[i]["direction"])
            aggregated.append(result2[i]["timestamp"])
            pubTime1 = datetime.strptime(aggregated[2], '%Y-%m-%dT%H:%M:%S')
            pubTime1_regular = pytz.timezone('EST').localize(pubTime1)
            pubTime2 = datetime.strptime(aggregated[5], '%Y-%m-%dT%H:%M:%S')
            pubTime2_regular = pytz.timezone('EST').localize(pubTime2)
            count_cam1 = aggregated[0]
            count_cam2 = aggregated[3]
            direction_cam1 = aggregated[1]
            direction_cam2 = aggregated[4]
            if (direction_cam1 == 'West' or direction_cam1 == 'North'):
                count_cam1 = count_cam1
            else:
                count_cam1 = -count_cam1
            if direction_cam2 == 'South' or direction_cam2 == 'West':
                count_cam2 = count_cam2
            else:
                count_cam2 = -count_cam2
            if abs((pubTime2_regular - pubTime1_regular).total_seconds()) < 3:
                if (direction_cam1 == 'East' and direction_cam2 == 'North') or (
                        direction_cam1 == 'South' and direction_cam2 == 'North'):
                    total_count += count_cam1 + count_cam2 - 1
                elif (direction_cam1 == 'West' and direction_cam2 == 'South') or (
                        direction_cam1 == 'North' and direction_cam2 == 'South'):
                    total_count += count_cam1 + count_cam2 - 1
                else:
                    total_count += count_cam1 + count_cam2
            else:
                total_count += count_cam1 + count_cam2
        data = {'timestamp': (max(pubTime2, pubTime1)).strftime("%Y-%m-%dT%H:%M:%S"), 'counts': abs(total_count), "available_spots": TOTAL_SPOTS - total_count}
        dataRecord = json.dumps(data)
        dataRecord = json.loads(dataRecord)
        with open('report.json', 'w') as outfile:
            json.dump(dataRecord, outfile)
        es.index(index="newmarket_total_test", doc_type='data', body=dataRecord)
        d = 'date: ' + (max(pubTime2, pubTime1)).strftime(
            "%Y-%m-%dT%H:%M:%SZ") + '' + '\n' + 'the number of cars:' + str(
            total_count)
