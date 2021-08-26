from threading import Thread
import cv2, time
from kafka import KafkaProducer
topic = 'kafka_video'
fourcc = cv2.VideoWriter_fourcc(*'XVID')
out = cv2.VideoWriter('motiondetect.avi',fourcc,20.0,(640,480))
class VideoStreamWidget(object):
    producer = KafkaProducer(bootstrap_servers = 'localhost:9092')
    def __init__(self, src=0):
        self.capture = cv2.VideoCapture(src)
        # Start the thread to read frames from the video stream
        self.thread = Thread(target=self.update, args=())
        self.thread.daemon = True
        self.thread.start()

    def update(self):
        # Read the next frame from the stream in a different thread
        i = 0
        while True:
            if self.capture.isOpened():
                self.ret, self.frame = self.capture.read()
                out.write(self.frame)
                i+=1
                #(self.status, self.frame) = self.capture.read()
                if i%3==0:
                    self.ret, self.frame = self.capture.read()
                    ret, buffer = cv2.imencode('.jpg', self.frame)
                    VideoStreamWidget.producer.send(topic, buffer.tobytes())
            out.release()
            #time.sleep(.01)

    def show_frame(self):
        # Display frames in main program
        cv2.imshow('frame', self.frame)
        key = cv2.waitKey(1)
        if key == ord('q'):
            self.capture.release()
            cv2.destroyAllWindows()
            exit(1)

if __name__ == '__main__':
    video_stream_widget = VideoStreamWidget(r'D:\Make-a-thon\Distributed-Multi-Video-Streaming-and-Processing-with-Kafka-master\videos\test.avi')
    while True:
        try:
            video_stream_widget.show_frame()
        except AttributeError:
            pass