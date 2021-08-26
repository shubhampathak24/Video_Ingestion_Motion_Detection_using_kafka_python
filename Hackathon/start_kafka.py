import os
os.chdir('kafka')
print('Starting Kafka Server')
os.system('bin\windows\kafka-server-start config\server.properties')
