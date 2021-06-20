from flask import Flask,jsonify,request
from flask import render_template
import ast

app = Flask(__name__)

labels = []
values = []


@app.route("/")
def chart():
    global labels,values
    labels = []
    values = []
    return render_template('chart.html', values=values, labels=labels)


@app.route('/refreshData')
def refresh_graph_data():
    global labels, values
    print("labels now: " + str(labels))
    print("data now: " + str(values))
    return jsonify(sLabel=labels, sData=values)
    
accumlated={}
@app.route('/updateData', methods=['POST'])
def update_data_post():
    global labels, values
    if not request.form or 'data' not in request.form:
        return "error",400
    
    #read in the data send from spark
    info = ast.literal_eval(request.form['data'])

    #clear the list before every refresh so that the data does not append
    labels.clear()
    values.clear()
    for value in info:       
        labels.append(value[0])
        values.append(value[1])


    print("labels received: " + str(labels))
    print("data received: " + str(values))
    return "success",201


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5001)

