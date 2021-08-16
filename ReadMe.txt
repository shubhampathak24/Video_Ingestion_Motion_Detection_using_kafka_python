1. Open cmd in MotionDetection folder
2. Run 'python import_libs.py'
3. Run your Kafka zookeeper and kafka server - Don't close there terminals.
4. Run 'python consumer.py' in MotionDetection folder.
5. Don't close the current terminal and open a new terminal. Run 'python producer.py'
6. Check you consumer terminal to get IP Address for video stream and append '/vidoe' in the address.
Example - http://192.168.29.28:5000/video