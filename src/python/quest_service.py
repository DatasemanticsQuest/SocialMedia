#!/usr/local/bin/python2.7

#Initializes logging
import logging

logging.basicConfig(filename='quest_service.log',level=logging.DEBUG,format='%(asctime)s %(message)s')

#To catch broken pipe issue
from signal import signal, SIGPIPE, SIG_DFL
signal(SIGPIPE,SIG_DFL) 

from flask import Flask, jsonify, abort, request, make_response, url_for
from flask_httpauth import HTTPBasicAuth
import subprocess
import sys
import master_producer_utils
import simplejson
import twitter_utils
import datetime
import time
import atexit

app = Flask(__name__, static_url_path = "")
auth = HTTPBasicAuth()

#Specifies authentication credentials
@auth.get_password
def get_password(username):
    if username == 'gunjan':
        return 'ds@123'
    return None

#Creates a bunch of error handlers
@auth.error_handler
def unauthorized():
    return make_response(jsonify( { 'error': 'Unauthorized access' } ), 403)
    # return 403 instead of 401 to prevent browsers from displaying the default auth dialog
    
@app.errorhandler(400)
def not_found(error):
    return make_response(jsonify( { 'error': 'Bad request' } ), 400)

@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify( { 'error': 'Not found' } ), 404)

task = {
        'id': 1001,
        'name': 'Daniel Craig',
	'type': 'Celebrity'
       }
#Defines the GET request
@app.route('/api/task', methods = ['GET'])
@auth.login_required
def get_task():
    return jsonify( { 'task': task } )

#Defines the POST request
@app.route('/api/task', methods = ['POST'])
@auth.login_required
def create_task():
#    if not request.json:
#        abort(400)
    print 'trying to print json'
    print simplejson.dumps(request.get_json(force=True))
    print 'here i am in post'
    print request.json
    print simplejson.dumps(request.json)
    task['id'] = request.json['id']
    task['name'] = request.json['name']
    task['type'] = request.json['type']
    print task
#Runs the search api once in the case of a new celebrity addition, to retrieve old tweets
    if task['type'] == 'celebrity':
	process3=subprocess.Popen(["/usr/local/bin/python2.7", "/root/quest/producer_search_api.py", task['name']])
    master_producer_utils.add_trend(task['type'], task['id'], task['name']) #input is a type, id and name. no return, simply performs the addition into the corresponding tables
#Restarts the event producer in the case of a new event being added
    if task['type'] == 'event':
	process4 = subprocess.Popen(["/usr/local/bin/python2.7", "/root/quest/restart.py", "porsche", "producer_streaming_api_event"])
    node_prod = master_producer_utils.get_node_name_and_producer_name(task['type'], task['id']) #input is a id, returns a dictionary of producer name and node name
    process1 = {}
    k = 0
#Creates subprocesses to restart each producer that the addition corresponds to
    for (prod,node) in node_prod.iteritems():
	process1[k]= subprocess.Popen(["/usr/local/bin/python2.7", "/root/quest/restart.py", node, prod])
	atexit.register(process1[k].terminate)
	k+=1
    return jsonify( { 'task': task } ), 201

#Defines the DELETE request
@app.route('/api/task', methods = ['DELETE'])
#@auth.login_required
def delete_task():
#    if not request.json:
#	abort(400)
    print 'here i am in delete' 
    print request.json
    print simplejson.dumps(request.json)
    task['id'] = request.json['id']
    task['name'] = request.json['name']
    task['type'] = request.json['type']
    node_prod = master_producer_utils.get_node_name_and_producer_name(task['type'], task['id']) #input is a id, returns a dictionary of producer name and node name
    master_producer_utils.del_trend(task['type'], task['id'])#input is a type and id. no return, simply performs the deletion from the corresponding tables
    process2 = {}
    l = 0
    for (prod,node) in node_prod.iteritems():
	process2[l]= subprocess.Popen(["/usr/local/bin/python2.7", "/root/quest/restart.py", node, prod])
        atexit.register(process2[l].terminate)
        l+=1
    return jsonify( { 'result': True } )

if __name__ == '__main__':
    print 'in main'
    app.run(host='0.0.0.0')
    app.run(debug = True)
