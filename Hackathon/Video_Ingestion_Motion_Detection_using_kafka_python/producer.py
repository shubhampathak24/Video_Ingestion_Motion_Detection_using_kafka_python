import sys
import time
import cv2
from kafka import KafkaProducer
import numpy as np
import os

os.system('start /MIN cmd /c python create_topic.py kafka_video')
fourcc = cv2.VideoWriter_fourcc(*'XVID')
out = cv2.VideoWriter('motiondetect.avi',fourcc,20.0,(640,480))

foreground = cv2.createBackgroundSubtractorMOG2(detectShadows=False)

topic = 'kafka_video'

def publish_video(video_file):
    producer = KafkaProducer(bootstrap_servers = 'localhost:9092')

    cap = cv2.VideoCapture(video_file)
    print('Publishing video...')
    i = 0
    while(cap.isOpened()):
        if i%3==0:
            success, camframe = cap.read()

            if not success:
                print('Bad read!')
                break

            ret, buffer = cv2.imencode('.jpg', camframe)

            producer.send(topic, buffer.tobytes())
        i+=1
        time.sleep(0.2)
    cap.release()
    cv2.destroyAllWindows()
    print('End of stream')

def publish_camera():
    producer = KafkaProducer(bootstrap_servers = 'localhost:9092')
    camera = cv2.VideoCapture(0,cv2.CAP_DSHOW)
    ret, frame1 = camera.read()
    ret, frame2 = camera.read()
    try:
        while camera.isOpened():
            diff = cv2.absdiff(frame1, frame2)
            gray = cv2.cvtColor(diff, cv2.COLOR_BGR2GRAY)
            blur = cv2.GaussianBlur(gray, (5,5),0)
            _, thresh = cv2.threshold(blur, 20,255, cv2.THRESH_BINARY)
            dilated = cv2.dilate(thresh, None, iterations = 3)
            contours, _ = cv2.findContours(dilated, cv2.RETR_TREE, cv2.CHAIN_APPROX_SIMPLE)

            for contour in contours:
                (x, y, w, h) = cv2.boundingRect(contour)

                if cv2.contourArea(contour) < 7000:
                    continue
                cv2.rectangle(frame1, (x,y), (x+w, y+h), (0,255,0),2)
                cv2.putText(frame1, "Status: {}".format('Movement'),(10,20), cv2.FONT_HERSHEY_SIMPLEX,1, (0, 0, 255), 3)
            
            out.write(frame1)
            cv2.imshow('Feed', frame1)
            ret, buffer = cv2.imencode('.jpg',frame1)
            producer.send(topic, buffer.tobytes())
            frame1 = frame2
            ret,frame2 = camera.read()

            if cv2.waitKey(40) == 27:
                break
        

    except:
        os.system('start /MIN cmd /c python minio_up.py')
        print('\nWe are done.')
        sys.exit(1)

    camera.release()
    out.release()
    cv2.destroyAllWindows()
    

if __name__ == '__main__':
    if(len(sys.argv)>1):
        video_path = sys.argv[1]
        publish_video(video_path)
    else:
        print('We are live!')
    publish_camera()
