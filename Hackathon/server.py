import os, webbrowser
from flask import Flask, render_template, request
app=Flask(__name__,template_folder="./")
cwd = os.getcwd()
@app.route("/")
def index():
    return render_template('landing.html')

@app.route("/action")
def my_link():
    i1=request.args.get("input1",type=str)
    i2=request.args.get("input2",type=str)
    os.chdir(cwd)
    os.chdir("Twitter")
    os.system("python get_tweets.py "+i1+" "+i2)

    return render_template("Twitter/index.html")

@app.route("/video")
def my_link1():
    i1=request.args.get("input1",type=str)
    print(i1)
    os.chdir(cwd)
    os.chdir("Video_Ingestion_Motion_Detection_using_kafka_python")
    os.system('start /MIN cmd /c python consumer.py')
    os.system('start /MIN cmd /c python producer.py '+i1)
    os.chdir("..\\")
    return render_template("returnTHP.html")
    

if __name__=="__main__":
    app.run(debug=True)