import sys
import time
import cv2
from kafka import KafkaProducer
import numpy as np

fourcc = cv2.VideoWriter_fourcc(*'XVID')
out = cv2.VideoWriter('motiondetect.avi',fourcc,20.0,(640,480))

foreground = cv2.createBackgroundSubtractorMOG2(detectShadows=False)

topic = 'kafka_video'

def publish_video(video_file):
    producer = KafkaProducer(bootstrap_servers = 'localhost:9092')

    cap = cv2.VideoCapture(video_file)
    print('Publishing video...')
    while(cap.isOpened()):
        success, camframe = cap.read()

        if not success:
            print('Bad read!')
            break

        ret, buffer = cv2.imencode('.jpg', camframe)

        producer.send(topic, buffer.tobytes())
        time.sleep(0.2)
    cap.release()
    print('End of stream')

def publish_camera():
    producer = KafkaProducer(bootstrap_servers = 'localhost:9092')
    camera = cv2.VideoCapture(0,cv2.CAP_DSHOW)
    #camera.set(cv2.CAP_PROP_FRAME_WIDTH,1920)
    #camera.set(cv2.CAP_PROP_FRAME_HEIGHT,1080)
    #camera.set(cv2.CAP_PROP_FPS,6)

    try:
        while(True):
            success, camframe = camera.read()
            #grayframe = cv2.cvtColor(camframe, cv2.COLOR_BGR2GRAY)
            #blurframe = cv2.GaussianBlur(camframe, (5,5),0)

            motionframe = foreground.apply(camframe)
            detect = (np.sum(motionframe))//255
            if detect > 30:
                print("Object in motion size = ",detect)
                cv2.waitKey(1)
                out.write(camframe)
                cv2.imshow('grayframe',camframe)
            ret, buffer = cv2.imencode('.jpg',camframe)
            producer.send(topic, buffer.tobytes())
            time.sleep(0.2)
    except:
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