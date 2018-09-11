# -*- coding: utf-8 -*-
import cv2
import json
import requests
import os,sys
import base64
import argparse
import time
import uuid
from datetime import datetime
from kafka import KafkaProducer
import paho.mqtt.publish as publish
import paho.mqtt.client as mqtt
import threading

# mqtt config
MQTT_HOST = "172.24.11.164"
MQTT_PORT = 1883

# TOPIC
TOPIC = "video-stream-"

# kafka config
KAFKA_HOST = "172.24.11.164:9092"

class VideoCamera(object):
    def __init__(self, videonum):
        self.isstop = False
        self.videoname = "Camera-" + videonum
        self.videonum = videonum
        print(self.videonum)
        if self.videonum == "1":
            self.video = cv2.VideoCapture("rtsp://admin:admin1234@192.168.21.6:554/live2.sdp")
        elif self.videonum == "2":
            self.video = cv2.VideoCapture("rtsp://root:ChangeM1@172.24.10.60/onvif-media/media.amp?profile=profile_1_h264&sessiontimeout=60&streamtype=unicast")
        elif self.videonum == "3":
            self.video = cv2.VideoCapture("rtsp://admin:ChangeM1@cam-01.openlab.tw:554/live1.sdp")
        else:
            self.video = cv2.VideoCapture("../data/video_143513_x264.mp4")
    # delete connection
    def __del__(self):
        self.video.release()
    # 開始執行程式
    def start(self):
	# 把程式放進子執行緒，daemon=True 表示該執行緒會隨著主執行緒關閉而關閉。
        print('Start recognize person from ipcam ' + self.videoname)
        catch_Pic = threading.Thread(target=self.recognize_person, args=(args,))
        catch_Pic.setDaemon(True)
        catch_Pic.start()
    # stop ipcam
    def isstop(self):
        self.isstop = True
        print('ipcam stopped!')
    # 取得Frame
    def getframe(self):
	# 當有需要影像時，再回傳最新的影像。
        return self.image
    # recognize
    def recognize_person(self, args):
        # MQTT Client
        client = mqtt.Client()
        client.connect(MQTT_HOST, MQTT_PORT)
        # KAFKA producer
        #producer = KafkaProducer(bootstrap_servers=[KAFKA_HOST], api_version=(0, 10))
        if not self.video.isOpened():
            print('video' + self.videoname + ' open failed!')
            exit()
        while (not self.isstop):
            try:
                isread, self.image = self.video.read()
                if not isread:
                    print('read frame failed!')
                    #exit()
                    continue
                tick = cv2.getTickCount()
                _,img_detect = cv2.imencode('.jpg', self.image)
                s_img_encoded = base64.b64encode(img_detect).decode()
                detected_persons = []
                # detect person from agent detect api
                response = requests.post(args.agent_url + "/api/Detect",data=json.dumps({'image':s_img_encoded,'description':'Camera-A'}))
                if response.status_code != 200:
                    print('Detect Error occur, status = %d'%(response.status_code))
                    exit()
                tfps = cv2.getTickFrequency() / (cv2.getTickCount() - tick)
                pos = int(self.video.get(cv2.CAP_PROP_POS_FRAMES))
                cv2.putText(self.image, "Position=%d, FPS=%.2f"%(pos,tfps), (20, 70), 0, 0.5, (255,0,0))
                results = json.loads(response.content)
                if len(results['person']) == 0:
                    print('Position=%d, FPS=%.2f. No detections!'%(pos,tfps))
                else:
                    # recognize person from hub Rekognition api
                    response = requests.post(args.hub_url_1 + "/api/Rekognition", data=json.dumps(results))
                    if response.status_code != 200:
                        print('Rekognition Error occur, status=%d'%(response.status_code))
                        break
                    rek_results = json.loads(response.content)
                    personResult = rek_results['personResult']
                    person_bbox = []
                    for i,p in enumerate(results['person']):
                        person_bbox.append({"person_bbox": str(p['bbox'][0]) + str(p['bbox'][1]) + str(p['bbox'][2]) + str(p['bbox'][0]), "face_base64": p['face_image'], "body_base64": p['image'], "feature_base64": p['feature'], "confidence": p['confidence']})
                    for i,p in enumerate(personResult):
                        x,y,w,h = p['bbox']
                        text_color = (0, 255, 0)
                        name = p['user']
                        result_bbox = str(p['bbox'][0]) + str(p['bbox'][1]) + str(p['bbox'][2]) + str(p['bbox'][0])
                        face_image = ''
                        body_image = ''
                        body_feature = ''
                        person_confidence = ''
                        if p['user'] == "Unknown":
                            for index,val in enumerate(person_bbox):
                                if str(result_bbox) == str(val['person_bbox']):
                                    face_image = val['face_base64']
                                    body_image = val['body_base64']
                                    body_feature = val['feature_base64']
                                    person_confidence = val['confidence']
                            #name = p['remark'] # most likely person name
                            struuid = str(uuid.uuid4())
                            name = p['user'] + struuid[:8]
                            self.registerUnknown(name, face_image, body_image, body_feature, args)
                            text_color = (0, 0, 255)
                        self.draw_opencv(p, name, text_color)
                        detected_persons.append({'target_person_name': name, 'target_person_uuid': name, 'bbox': {'x': x, 'y': y, 'width': w, 'height': h}})
                    print('Position=%d, FPS=%.2f. Recognize %d persons!'%(pos,tfps,len(personResult)))
                if args.record is True:
                    timestamp = str(datetime.now())
                    _,img_base64 = cv2.imencode('.jpg', self.image)
                    producer_image = base64.b64encode(img_base64).decode()
                    producer_image = "data:image/jpg;base64," + producer_image
                    producerdata = {"timestamp": timestamp, "camera_name": 'cameraA', "camera_uuid": 'a123', "image_base64": producer_image}
                    for i,n in enumerate(detected_persons):
                        producerperson = {"timestamp": timestamp, "target_person_name": str(json.dumps(n['target_person_name'])).strip('"'), "target_person_uuid": str(json.dumps(n['target_person_name'])).strip('"'), "camera_name": "cameraA", "camera_uuid": "a123", "image_base64": producer_image, "bbox": json.dumps(n['bbox']), "confidence":0.90}
                        #client.publish('person-unknown', json.dumps(producerperson))
                        #producer.send('person-' + json.dumps(n['target_person_name']).strip('"'), json.dumps(producerperson))
                        #producer.send('person-unknown', json.dumps(producerperson))
                    producerdata.update({'detected_persons': detected_persons})
                    client.publish(TOPIC + self.videonum, json.dumps(producerdata))
                    #producer.send(TOPIC + self.videonum, json.dumps(producerdata))
                ret, jpeg = cv2.imencode('.jpg', self.image)
                #if args.record is True:
                    #out.write(image)
                #return jpeg.tobytes()
                if cv2.waitKey(1) & 0xff == 27:
                    self.video.release()
                    break
            except KeyboardInterrupt:
                self.video.release()
            except requests.ConnectionError:
                time.sleep(10)
                continue
            except:
                time.sleep(10)
                raise

    # draw opencv
    def draw_opencv(self, p, name, text_color):
        x,y,w,h = p['bbox']
        dist = round(p['distance'],2)
        prob = round(p['similarity']*100.0,2)
        cv2.rectangle(self.image, (x, y),(x+w, y+h),text_color, 1)
        cv2.putText(self.image, "%.f%%"%prob,(x,y), 0, 0.3, text_color, 1)
        cv2.putText(self.image, "%.2f"%dist,(x,y+int(h/2)), 0, 0.25, text_color, 1)
        cv2.putText(self.image, name,(x,y+int(h/4)), 0, 0.25, text_color, 1)
        cv2.putText(self.image, p['remark'],(x,y+int(h/1.3)), 0, 0.25, text_color, 1)

    # create Unknown
    def registerUnknown(self, name, face_image, body_feature, body_image, args):
        if face_image:
            print('face')
            data = {'name': name, 'faceimage': face_image}
            #response = requests.post(args.hub_url + "/api/Register", data=json.dumps({'name': name, 'faceimage': face_image, 'info': self.videoname}))
        elif body_feature:
            print('body')
            data = {'name': name, 'bodyfeature': body_feature, 'bodyimage': body_image}
            #response = requests.post(args.hub_url + "/api/Register", json.dumps({'name': name, 'bodyfeature': body_feature, 'bodyimage': body_image, 'info': self.videoname}))
        #if response.status_code != 200:
        #    print('Register Error occur, Error=%s'%(response.content.decode()))
        #print('Register success, response=%s'%(response.content.decode()))

def parse_arguments(argv):
    parser = argparse.ArgumentParser(description='Person register demo.')
    parser.add_argument('--hub_url', type=str, help='hub server url', default='http://localhost:3000')
    parser.add_argument('--agent_url', type=str, help='agent server url', default='http://localhost:8080')
    parser.add_argument('--hub_url_1', type=str, help='hub server url', default='http://172.19.2.199:3000')
    parser.add_argument('--agent_url_1', type=str, help='agent server url', default='http://172.19.2.199:8081')
    parser.add_argument('-v','--video', type=str, help='input video file', default='../data/video_143513_x264.mp4')
    parser.add_argument('--record', type=bool, help='record video', default=True)
    parser.add_argument('--videonum', type=str, help='video num', default='1')

    return parser.parse_args(argv)

if __name__ == '__main__':
    args = parse_arguments(sys.argv[1:])
    ipcam = VideoCamera(args.videonum)
    ipcam.start()
    time.sleep(10)
    while True:
    # 使用 getframe 取得最新的影像
        ipcam.getframe()
    
    if cv2.waitKey(1000) == 27:
        ipcam.stop()
