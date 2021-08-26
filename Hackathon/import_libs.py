import pip
import os

pip.main(['install','tweepy'])
pip.main(['install','kafka-python'])
pip.main(['install','pandas'])
pip.main(['install','matplotlib'])
pip.main(['install','flask'])
pip.main(['install','opencv-python'])
pip.main(['install','tensorflow'])
pip.main(['install','numpy'])
#pip.main(['install','pymongo'])


'''
import subprocess
subprocess.run(["python start_zookeeper.py","python start_kafka.py"], shell=True)
'''