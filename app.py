from flask import Flask, render_template, request, jsonify, send_file, Response, send_from_directory
import pandas as pd
import sqlite3
import geopandas as gpd
import os
import random
from werkzeug.utils import secure_filename
import zipfile
import json
import fiona
from datetime import datetime


app = Flask(__name__, static_folder="build", template_folder="templates", static_url_path='/')

app.config['uploadsdb'] = 'build\\database\\uploadsdb'
app.config['convertgnss'] = 'build\\database\\convertgnss'
app.config['temp'] = 'build\\database\\temp'
app.config['database'] = 'build\\database'


def shp_to_zip(gds, filename, temp = 'temp', outf = 'temp'):
    output = os.path.join(app.config[temp], 'output.shp')
    gds.to_file(output)

    with zipfile.ZipFile(os.path.join(app.config[outf], f"{filename}.zip"), 'w', zipfile.ZIP_DEFLATED) as zip_file:
        for i in ['.shp', '.shx', '.prj', '.cpg', '.dbf']:
            filepaths = os.path.join(app.config[temp], f"output{i}")
            zip_file.write(filepaths, f"{filename}{i}")
            os.remove(filepaths)
    


@app.route('/')
def index():
    # return render_template('index.html')
    return send_from_directory(app.static_folder, 'index.html')
@app.route('/test')
def test():
    return render_template('index.html')


@app.route('/senddbflie', methods=['POST', 'GET'])
def senddbflie():
    if request.method == 'POST':
        if 'file' not in request.files:
            return jsonify({'error': 'No file select'})
        
        file = request.files['file']
        if 'db' not in file.filename:
            return jsonify({'error': 'select db file'})
        else:
            try:
                filename = secure_filename(file.filename)
                filepath = os.path.join(app.config['uploadsdb'], filename)
                file.save(filepath)
                con = sqlite3.connect(filepath)
                ds = pd.read_sql('SELECT * FROM surveypointbody', con)
                ds = ds[['dataSetName','code', 'localNehn', 'localNehe', 'localNehh']]
                gds = gpd.GeoDataFrame(ds, geometry=gpd.points_from_xy(ds['localNehe'], ds['localNehn']), crs=32648).to_crs('epsg:4326')
                output = os.path.join(app.config['temp'], 'output.shp')
                gds.to_file(output)
            
                with zipfile.ZipFile(os.path.join(app.config['uploadsdb'], filename.replace('.db','.zip')), 'w', zipfile.ZIP_DEFLATED) as zip_file:
                    for i in ['.shp', '.shx', '.prj', '.cpg', '.dbf']:
                        filepaths = os.path.join(app.config['temp'], f"output{i}")
                        zip_file.write(filepaths, f"{filename.replace('.db',i)}")
                        os.remove(filepaths)
                con.close()
                os.remove(filepath)

                existjson = json.load(open(os.path.join(app.config['database'], 'convertlist.json'),'r'))
                try:
                    gds = gds.to_crs('epsg:32648')
                    listcol = existjson[filename.replace('.db', '.zip')]['info']
                    
                    ds = gds[gds['code'] == listcol['main']['col']]
                    newds = ds[['localNehn', 'localNehe', 'localNehh', 'geometry']]
                    newds = newds.rename(columns = {'localNehn': "X", 'localNehe': "Y", 'localNehh': listcol['main']['newcol']})

                    for i in listcol.keys():
                        if i != 'main':
                            if listcol[i]['col'] != '':
                                ds = gds[gds['code'] == listcol[i]['col']]
                                
                                newdsa = gpd.sjoin_nearest(newds, ds, how='left', max_distance=float(listcol[i]['dis']), distance_col='distance')
                                newds[listcol[i]['newcol']] = newdsa['localNehh']
                                
                    shp_to_zip(newds, existjson[filename.replace('.db', '.zip')]['newfile'], outf='convertgnss')
                    
                    
                except:
                    print('No ')
                return jsonify({'success': 'read data complete'})
            except:
                return jsonify({'error', 'it is not data db'})

@app.route('/readdbfile', methods=['POST'])
def readdbfile():
    if request.method == 'POST':
        
        con = sqlite3.connect(f"uploadsdb/{request.json['filename']}")
        ds = pd.read_sql('SELECT * FROM surveypointbody', con)
        ds = ds[['dataSetName','code', 'localNehn', 'localNehe', 'localNehh']]
        gds = gpd.GeoDataFrame(ds, geometry=gpd.points_from_xy(ds['localNehn'], ds['localNehe']), crs='epsg:4326')
        return jsonify({'success': 'Get Data from Map'}) 
        


@app.route('/upload', methods=['POST'])
def upload_file():
    try:
        # Check if file is in the request
        if 'file' not in request.files:
            return 'No file part', 400
        file = request.files['file']
        
        if file.filename == '':
            return 'No selected file', 400
        
        if file and file.filename.endswith('.db'):
            
            # print(file)
            filename = secure_filename(file.filename)
            filepath = os.path.join(app.config['uploadsdb'], filename)
            file.save(filepath)
            return jsonify({'success': 'send file complete'})
    except:
        'error'


####################### GNSS ############################
@app.route('/GNSS/listdbfile', methods = ['GET', 'POST'])
def GNSSlistdbfile():
    if request.method == 'GET':
        listdata = [['', '']] + [[i.replace('.zip', ''), datetime.fromtimestamp(os.path.getmtime(os.path.join(app.config['uploadsdb'], i))).strftime('%d-%m-%Y %H:%M:%S')]  for i in os.listdir(app.config['uploadsdb'])]
        existjson = json.load(open(os.path.join(app.config['database'], 'convertlist.json'),'r'))
        ll = []
        for i in listdata:
            if f"{i[0]}.zip" in existjson.keys():
                i.append('active')
            else:
                i.append('')
            ll.append(i)        
        return jsonify({'success': ll})

    if request.method == 'POST':
        filename = request.form['filename']
        filepath = os.path.join(app.config['uploadsdb'], filename)
        gds = gpd.read_file(f"zip://{filepath}")
        
        random.seed(1)
        code = {}        
        for i in gds['code'].drop_duplicates().values:
            
            if i != "" or i == "None" :
                code[str(i)] = f"{random.randint(0, 16777215):06x}"


        return jsonify({'data': gds.to_json(), 'listcode':code,  'bounds': list(gds.total_bounds)})

@app.route('/GNSS/exportfile', methods = ['GET', 'POST'])
def GNSS_exportfile():
    if request.method == 'POST':
        filename =request.form['filename']
        filepath = os.path.join(app.config['uploadsdb'], filename)
        
        con = sqlite3.connect(filepath)
        ds = pd.read_sql('SELECT * FROM surveypointbody', con)
        ds = ds[['dataSetName','code', 'localNehn', 'localNehe', 'localNehh']]
        gds = gpd.GeoDataFrame(ds, geometry=gpd.points_from_xy(ds['localNehe'], ds['localNehn']), crs=32648).to_crs('epsg:4326')
        output = "build/static/Temp/output.shp"
        gds.to_file(output)
        with zipfile.ZipFile(f"static/Temp/{filename.replace('.db','.zip')}", 'w', zipfile.ZIP_DEFLATED) as zip_file:

            for i in ['.shp', '.shx', '.prj', '.cpg', '.dbf']:
                filepath = f"static/Temp/output{i}"
                zip_file.write(filepath, f"{filename.replace('.db',i)}")
                os.remove(filepath)
        return jsonify({'success':f"{request.host_url}static/Temp/{filename.replace('.db','.zip')}"})
    
@app.route('/GNSS/download', methods = ['POST'])
def downloadgnss():
    if request.method == 'POST':
        filename =request.form['filename']
        filepath = os.path.join(app.config['uploadsdb'], filename)
        gds = gpd.read_file(f"zip://{filepath}")
        typeexe = request.form['typedownload']
        if typeexe == 'kml':
            fiona.supported_drivers['KML'] = 'rw'
            gds.to_file(os.path.join(app.config['temp'], filename.replace('.zip', '.kml')), driver='KML')
        if typeexe == 'csv':
            gds.to_csv(os.path.join(app.config['temp'], filename.replace('.zip', '.csv')))
        if typeexe == 'xlsx':
            gds.to_excel(os.path.join(app.config['temp'], filename.replace('.zip', '.xlsx')))
        
        return jsonify({"success": "download complete"})
@app.route('/GNSS/removedb', methods = ['POST'])
def removedb():
    if request.method == 'POST':
        filename =request.form['filename']
        filepath = os.path.join(app.config['uploadsdb'], filename)
        os.remove(filepath)
        listdata = [['', '']] + [[i.replace('.zip', ''), datetime.fromtimestamp(os.path.getmtime(os.path.join(app.config['uploadsdb'], i))).strftime('%d-%m-%Y %H:%M:%S')]  for i in os.listdir(app.config['uploadsdb'])]
        return jsonify({'success': listdata})

@app.route('/GNSS/getcolumn', methods=['POST'])
def getcolumn():
    if request.method == 'POST':
        filename = request.form['filename']
        filepath = os.path.join(app.config['uploadsdb'], filename)
        gds = gpd.read_file(f"zip://{filepath}")

        existjson = json.load(open(os.path.join(app.config['database'], 'convertlist.json'),'r'))

        if filename in existjson.keys():
            col = existjson[filename]
        else:
            col = None
        
        return jsonify({'success': 'send complete', 'columns':[i for i in  gds['code'].drop_duplicates().values if i != None], "exitc": col})

@app.route('/GNSS/setnewdata', methods=["POST"])
def setnewdata():
    if request.method == 'POST':
        filename = request.form['filename']
        newfilename = request.form['newfilename']
        listcol = json.loads(request.form['columns'])
        filepath = os.path.join(app.config['uploadsdb'], filename)
        gds = gpd.read_file(f"zip://{filepath}")
        gds = gds.to_crs('epsg:32648')


        ds = gds[gds['code'] == listcol['main']['col']]
        newds = ds[['localNehn', 'localNehe', 'localNehh', 'geometry']]
        newds = newds.rename(columns = {'localNehn': "X", 'localNehe': "Y", 'localNehh': listcol['main']['newcol']})

        for i in listcol.keys():
            if i != 'main':
                if listcol[i]['col'] != '':
                    ds = gds[gds['code'] == listcol[i]['col']]
                    newdsa = gpd.sjoin_nearest(newds, ds, how='left', max_distance=float(listcol[i]['dis']), distance_col='distance')
                    newds[listcol[i]['newcol']] = newdsa['localNehh']


        existjson = json.load(open(os.path.join(app.config['database'], 'convertlist.json'),'r'))
        existjson[filename] = {"info": listcol, "newfile": newfilename}
        with open(os.path.join(app.config['database'], 'convertlist.json'),'w') as f:
            json.dump(existjson, f)
        
        shp_to_zip(newds, newfilename, outf='convertgnss')
        return jsonify({"success": 'setnew complete'})

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')