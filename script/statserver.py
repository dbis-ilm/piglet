#
# A simple server that accept HTTP requests with statistics
# information of a Piglet Spark job in JSON format and stores
# it in a small document database
#
# For REST server we use Flask: http://flask.pocoo.org/
# For Document DB we use TinyDB: https://pypi.python.org/pypi/tinydb
#
#

from flask import Flask, request, jsonify, make_response
from tinydb import TinyDB, where, Query
import argparse



# a new Flask app for REST service
app = Flask(__name__)

@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found'}), 404)

# a tiny "DB" to store values in a JSON document
# will be set in main
db = None

#@app.errorhandler(404)
#def notFound(error):
#	return make_response(jsonify({'error': 'Not Found'}),404)

#########################################
# Execution statistics for stages
#########################################

def saveExectimeToDB(exectime):
	return exectimes.insert(exectime)

def findExectimeInDB(lineage):
	entry = Query()
	res = exectimes.search(entry.lineage == lineage)
	return res

def getAllExectimesFromDB():
	return exectimes.all()

@app.route('/exectimes',methods=['POST'])
def addExectime():

	if not request.json:
		abort(404)

	exectime = {
		'appname': request.json['appname'],
		'stageid': request.json['stageid'],
		'stagename': request.json['stagename'],
		'lineage': request.json['lineage'],
		'stageduration': request.json['stageduration'],
		'progduration': request.json['progduration'],
		'submissiontime': request.json['submissiontime'],
		'completiontime': request.json['completiontime'],
		'size': request.json['size']
	}

	res = saveExectimeToDB(exectime)
	return str(res)

@app.route('/exectimes', methods=['GET'])
def getAllExectimes():
	return jsonify({'exectimes':getAllExectimesFromDB()})

@app.route('/exectimes/<string:lineage>', methods=['GET'])
def getExectime(lineage):
	res = findExectimeInDB(lineage)
	return jsonify({'exectimes':res})

@app.route('/times', methods=['POST'])
def addTiming():
  if not request.json:
    abort(404)

  timingData = {
    'lineage': request.json['lineage'],
    'partitionId': request.json['id'],
    'time': request.json['time']
  }

  print(timingData)
  return str("ok")


#########################################
# Materialization points for lineages
#########################################

def saveMaterializationToDB(materialization):
	return materializations.insert(materialization)

def findMaterialization(lineage):
	entry = Query()
	res = materializations.search(entry.lineage == lineage)
	return res[0] if res else ''

def getAllMaterializationsFromDB():
	return materializations.all()

# upload a new one
@app.route('/materializations',methods=['POST'])
def addMaterialization():
	mat = {
		'lineage':request.json['lineage'],
		'path': request.json['path']
	}

	res = saveMaterializationToDB(mat)

	return str(res)

@app.route('/materializations', methods=['GET'])
def getAllMaterializations():
	return jsonify({'materializations': getAllMaterializationsFromDB()})

# get path for Materialization of the given lineage
@app.route('/materializations/<string:lineage>', methods=['GET'])
def getMaterialization(lineage):
	mat = findMaterialization(lineage)
	return jsonify(mat)





@app.route('/',methods=['GET'])
def index():
	return "Hallo Welt"





if __name__ == '__main__':

	parser = argparse.ArgumentParser()
	parser.add_argument("-f", "--file", help="Filename to use for Database")
	parser.add_argument("-p", "--port", type=int, help="The port for the REST server to listen on")

	args = parser.parse_args()

	db = TinyDB(args.file)
	# create necessary tables
	exectimes = db.table("exectimes")
	materializations = db.table("materializations")

	app.run(host='0.0.0.0',debug=False,port=args.port)
