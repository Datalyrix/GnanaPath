###################################################################
#  GnanaPath Main App Server
#
#
#
#################################################################
import os
import sys
import logging
import pyspark
from pyspark.sql import SparkSession

curentDir = os.getcwd()
listDir = curentDir.rsplit('/', 1)[0]
sys.path.append(listDir)
###sys.path.append(listDir + '/gndatadis')
#import findspark

#findspark.init()

from gndatadis.gndd_csv_load import gndwdbDataUpload  # to upload Files.
from gnutils.get_config_file import get_config_neo4j_conninfo_file
from gndwdb.gndwdb_neo4j_fetchops import gndwdb_metarepo_nodes_fetch_api, gndwdb_metarepo_edges_fetch_api
from gnsearch.gnsrch_sql_srchops import gnsrch_sqlqry_api
from gndwdb.gndwdb_neo4j_conn import gndwdb_neo4j_conn_check_api, gndwdb_neo4j_parse_config
###from gngraph.ingest.gngraph_ingest_main import gngraph_init
from gndatadis.gndd_filedb_ops import gndd_filedb_insert_file_api, gndd_filedb_filelist_get
from gndatadis.gndd_filelist_table import GNFileLogResults
from gngraph.search.gngraph_search_main import gngrph_search_init, gngrph_srch_metarepo_nodes_edges_fetch, gngrph_srch_datarepo_qry_fetch
###from gngraph.gngraph_dbops.gngraph_pgresdbops import GNGraphPgresDBOps


import flask
from flask import request, jsonify, request, redirect, render_template, flash, url_for, session, Markup, abort
from werkzeug.utils import secure_filename
from flask_login import login_required, current_user, login_user, logout_user, LoginManager
import base64
import reg_users
from moz_sql_parser import parse
import json
import re
from connect_form import ConnectServerForm, LoginForm
from collections import OrderedDict
from gn_config import gn_config_init, gn_logging_init, gn_log, gn_log_err
from gn_config import GNGraphConfigModel, GNGraphDBConfigModel
from pathlib import Path
import json
import pathlib
from pyspark.sql import SparkSession


# Append system path


def dequote(s):
    """
    If a string has single or double quotes around it, remove them.
    Make sure the pair of quotes match.
    If a matching pair of quotes is not found, return the string unchanged.
    """
    if (s[0] == s[-1]) and s.startswith(("'", '"')):
        return s[1:-1]
    return s


app = flask.Flask(__name__)
app.config["DEBUG"] = True

app.secret_key = "f1eaff5ddef68e5025adef1db4cf7807"
app.config['MAX_CONTENT_LENGTH'] = 256 * 1024 * 1024
# app.config["JSONIFY_PRETTYPRINT_REGULAR"]=True
# Get current path
path = os.getcwd()

app.config["gnRootDir"] = path.rsplit('/', 1)[0]
### Intialize config directories and logging
gn_config_init(app)

###gnlogger = gn_logging_init(app, "GNPath")

# file Upload
###UPLOAD_FOLDER = os.path.join(path, 'uploads')
UPLOAD_FOLDER = app.config["gnUploadsFolder"]

# Make directory if uploads is not exists
if not os.path.isdir(UPLOAD_FOLDER):
    os.mkdir(UPLOAD_FOLDER)

    
###app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

#### Initialize Spark Session
##gnGraphCls = gngraph_init(app.config["gnRootDir"])

app_name="gngraph_spk"
#spark = SparkSession.builder().appName(app_name).getOrCreate()
##spark = SparkSession.builder.getOrCreate()

#spark.stop()
gn_log('GNPMain: Getting spark session' )
gnp_spark = SparkSession \
        .builder \
        .appName("gnp") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

gn_log(' Spark session acquired ')
#sc = pyspark.SparkContext(appName=app_name)

### Initialize GNGraph Sessions
gnsrch_ops = gngrph_search_init(gnp_spark,   app.config["gnDataFolder"],  app.config["gnGraphDBCredsFolder"],  app.config["gncfg_settings"])


# Allowed extension you can set your own
ALLOWED_EXTENSIONS = set(['csv', 'json', ])

login_manager = LoginManager()
login_manager.init_app(app)

usr = reg_users.User()
all_users = {"gnadmin": usr}


def allowed_file(filename):
    return '.' in filename and filename.rsplit(
        '.', 1)[1].lower() in ALLOWED_EXTENSIONS


@login_manager.user_loader
def load_user(user_id):
    return all_users.get(user_id)


@app.route('/', methods=['GET'])
def gn_home():
    if check_server_session() or request.args.get('disp_srch'):
        _srch = True
    else:
        _srch = False

    return render_template('base_layout.html', disp_srch=_srch)


@app.route('/upload', methods=['GET', 'POST'])
@login_required
def upload_file():
    _srch = True if check_server_session() else False
    if request.method == 'GET':
        fres = gndd_filedb_filelist_get(app.config["gnCfgDBFolder"]);
        flen = len(fres)
        ###fres_table = GNFileLogResults(items=fres)
        print('upload ')
        print(fres)
        return render_template('upload.html', disp_srch=_srch, file_res=fres, flen=flen)
    if request.method == 'POST':

        if 'fd' not in request.files:
            flash('No file part', 'danger')
            return redirect(request.url)

        files = request.files["fd"]
        print(files)

        fdesc = request.form["fdesc"];
        print(' upload fdesc '+fdesc);

        ftype = request.form["ftype"];
        print(' upload ftype '+ftype);

        fdelim = request.form['fdelim'];
        print(' upload fdelim '+fdelim);
        
        if not allowed_file(files.filename):
            flash('Please upload CSV or JSON file', 'danger')
            return redirect(request.url)
        elif files and allowed_file(files.filename):
            fname = secure_filename(files.filename)
            file_name,file_ext=fname.split(".")
            filename= re.sub(r'\W+','',file_name)+f'.{file_ext}'
            print(filename)
            print(app.config['gnUploadsFolder'])
            files.save(os.path.join(app.config['gnUploadsFolder'], filename))
            ### For timebeing disable csv file upload
            ###gndwdbDataUpload(app.config['gnUploadsFolder'], filename)
            ###fdelim = ','
            fp = Path(app.config['gnUploadsFolder']+"/"+filename)
            fsize = fp.stat().st_size
            gndd_filedb_insert_file_api(filename, fsize, ftype, fdesc, fdelim, app.config["gnCfgDBFolder"])
            fres = gndd_filedb_filelist_get(app.config["gnCfgDBFolder"]);
            flen = len(fres)
            #print(fres_table)
            ##fres_table = GNFileLogResults(items=fres)
            
        flash(f'File {filename} successfully uploaded', 'success')
        ####return redirect(url_for('gn_home', disp_srch=_srch))
        return render_template('upload.html', disp_srch=_srch, file_res=fres, flen=flen)


def neo4j_conn_check_api():
    verbose = 0
    cfg_file = get_config_neo4j_conninfo_file()
    if not cfg_file == "Error":
        try:
            res = gndwdb_neo4j_conn_check_api(cfg_file, verbose)
            return res
        except:
            return "Error"
    return "NoFile"


def check_server_session():
    return True if 'serverIP' in session else False


@app.route("/gdbconfig", methods=['GET', 'POST'])
@login_required
def  gdb_config_settings_page():

    gdbcfg = GNGraphConfigModel(app.config["gnGraphDBCredsFolder"])
    gdbcfg_settings = gdbcfg.get_op()
    if gdbcfg_settings is None:
       gdbcfg_settings = {} 
       gdbcfg_settings['sfmode'] = 1
       gdbcfg_settings['dbmode'] = 0
    print(gdbcfg_settings)
    #if (request.method == 'GET'):
        
    if (request.method == 'POST'):
        print('GB Config POST')
        in_vars = request.form.to_dict()
        print(in_vars)
        cfg_setting = gdbcfg_settings
        if ('sfmode' in in_vars):
            print('GNGraphDB: sfmode '+in_vars['sfmode'])
            if (in_vars['sfmode'] == 'on'):
                cfg_setting['sfmode'] = 1
            else:
                cfg_setting['sfmode'] = 0

        if ('dbmode' in in_vars):
            print('GNGraphDB: sfmode '+in_vars['sfmode'])
            if (in_vars['dbmode'] == 'on'):
                cfg_setting['dbmode'] = 1
            else:
                cfg_setting['dbmode'] = 0
                
        gdbcfg.insert_op(cfg_setting)
        return redirect(url_for('gn_home'))    
          
    return render_template('gdb_config.html', title='Gnana Graph Server Config', cfg=gdbcfg_settings)
   

@app.route("/connect", methods=['GET', 'POST'])
@login_required
def connect_server():

    form = ConnectServerForm()
    connect = GNGraphDBConfigModel(app.config["gnGraphDBCredsFolder"])
    conf_settings = GNGraphConfigModel(app.config["gnGraphDBCredsFolder"])
    conn = {}
    if (request.method == 'GET'):
        
       #if 'serverIP' in session and connect.search_res(session['serverIP']):
        conn = connect.get_op()
        print('app srv: ')
        print(conn)
        
        if conn :
          srv_ip_encode = base64.urlsafe_b64encode(
            conn['serverIP'].encode("utf-8"))
          srv_encode_str = str(srv_ip_encode, "utf-8")
          print('ConnectForm: session ')
          ##conn['serverIP'] = session['serverIP']
          ##conn['serverPort'] = session['serverPort']
          ##conn['user_id'] = session['_user_id']
          form.serverIP.data = conn['serverIP']
          form.serverPort.data = conn['serverPort']
          form.username.data = conn['username']
          form.dbname.data = conn['dbname']
          ##print(conn)
          #flash(Markup('Connected to db server {},Click <a href=/modify/{}\
          #     class="alert-link"> here</a> to modify'.format(session['serverIP'], srv_encode_str)), 'info')
          #return redirect(url_for('gn_home', disp_srch=True))

        return render_template('db_setup.html', title='Connect Graph Server', form=form, disp_srch=False, conninfo=conn)
    
    if (request.method == 'POST'):
      print('connect_server: POST method input vars')
      in_vars = request.form.to_dict()
      print(in_vars)
      if ( 'db_mode' in in_vars):
          print(' DB mode set: Postgres ')
      if ('sf_mode' in in_vars):
          print(' Static file mode: Static files ')
          
      
      if form.validate_on_submit():
        result = request.form.to_dict()
        req_dict = connect.req_fields_json(result)
        print('DBConfig: POST ')
        print(req_dict)
        
        connect.upsert_op(req_dict)
        ##res = neo4j_conn_check_api()
        res = "Success"
        if res == "Error":
            flash(
                f'Error connecting to db server {form.serverIP.data}',
                'danger')
            connect.delete_op(req_dict)
            return render_template(
                'db_setup.html', title='Connect Graph Server', form=form, disp_srch=False)
        else:
            session['serverIP'] = form.serverIP.data
            session['serverPort'] = form.serverPort.data
            flash(f'Connected to server {form.serverIP.data}!', 'success')
            return redirect(url_for('gn_home', disp_srch=True))
      else:
         print('connect_server: Error in form submit')
         flash(f'Error on submit ', form.errors)         
         return render_template('db_setup.html', title='Connect Graph Server', form=form,  disp_srch=False)
  
        
    ##return render_template(
      ##  'connect_db.html', title='Connect Graph Server', form=form, disp_srch=False)


@app.route("/modify/<serverIP>", methods=['GET', 'POST'])
@login_required
def modify_conn_details(serverIP):
    decoded_bytes = base64.b64decode(serverIP)
    servIP = str(decoded_bytes, "utf-8")
    form = ConnectServerForm()
    connect = ConnectModel(path)
    serv_details = connect.search_res(servIP)
    if not serv_details:
        flash(f"No connection details exist to modify", 'warning')
        session.pop('serverIP', None)
        return render_template(
            'connect.html', title='Connect Graph Server', form=form, disp_srch=False)
    if request.method == 'GET':
        form.serverIP.data = serv_details[0]['serverIP']
        form.username.data = serv_details[0]['username']
        form.password.data = serv_details[0]['password']
        form.connect.label.text = 'Update'
        return render_template(
            "connect.html", title='Modify connection details', form=form, disp_srch=True)
    if request.method == 'POST':
        if form.validate_on_submit():
            result = request.form.to_dict()
            req_dict = connect.req_fields_json(result)
            connect.update_op(servIP, req_dict)
            res = neo4j_conn_check_api()
            if res == "Error":
                flash(
                    f'Error connecting to db server {form.serverIP.data}',
                    'danger')
                form.connect.label.text = 'Update'
                connect.delete_op(req_dict)
                session.pop('serverIP', None)
                return redirect(url_for('gn_home', disp_srch=False))
            flash('Connection details modified successfully', 'success')

            return redirect(url_for('gn_home', disp_srch=True))
        else:
            flash('Error in  Server Connection parameters', 'danger')
            form.connect.label.text = 'Update'
            return render_template(
                'connect.html', title='Modify connection details', form=form, disp_srch=False)


@app.route("/logout/")
@login_required
def logout():
    logout_user()
    session.pop('serverIP', None)
    return redirect(url_for('user_login'))


@app.route("/login", methods=['GET', 'POST'])
def user_login():
    form = LoginForm()
    if request.method == 'GET':
        return render_template('login.html', title='Login ', form=form)

    username = request.form["username"]
    if username not in all_users:
        flash(f'Invalid  user', 'danger')
        return render_template("login.html", form=form)
    user = all_users[username]
    if not user.check_password(request.form["password"]):
        flash(f'Incorrect password', 'danger')
        return render_template("login.html", form=form)

    login_user(user)
    if neo4j_conn_check_api() == "NoFile":
        _srch = False
        flash("Please input the server config details", 'success')
        return redirect(url_for('connect_server', disp_srch=_srch))

    if neo4j_conn_check_api() == "Error":
        _srch = False
        connect = ConnectModel(path)
        connect._db.truncate()
        flash("Error connecting to db server", 'danger')
        return redirect(url_for('connect_server', disp_srch=_srch))
    _srch = True
    cfg_file = get_config_neo4j_conninfo_file()
    conn_param = gndwdb_neo4j_parse_config(1)
    session['serverIP'] = conn_param['serverIP']
    return redirect(url_for('connect_server', disp_srch=_srch))


@app.route('/gnsrchview', methods=['GET'])
@login_required
def gnview_cola_api():
    _srch = True if check_server_session() else False
    gn_log(' gnview cola is initiated')
    return render_template('gnview/gnsrchview.html', disp_srch=_srch)

@app.route('/api/v1/testdbconn', methods=['GET'])
@login_required
def  testdb_conn():

    dbIP=''
    dbPort=''
    dbUser=''
    dbPasswd=''
    dbname=''
    if 'dbIP' in request.args:
      dbIP = request.args['dbIP']
    
    if 'dbPort' in request.args:   
      dbPort = request.args['dbPort']
      
    if 'dbUser' in request.args:
      dbUser = request.args['dbUser']

    if 'dbPasswd' in request.args:  
      dbPasswd = request.args['dbPasswd']

    if 'dbname' in request.args:  
      dbname = request.args['dbname']
    print('Test DB Connection dbIP '+dbIP+"  port "+dbPort)
    if ((dbIP == '') or dbPort == '' or dbUser == '' or dbPasswd == '' or dbname == ''):
        print('DBTestConn: Invalid args ')
        rjson = {
            "status": "FAIL",
            "connect_status": 0,
            "statusmsg": "Invalid Input Arguments"
            }
        return jsonify(rjson)
      
    pgdb_cls = GNGraphPgresDBOps(dbIP, dbPort, dbUser, dbPasswd,  dbname, "metadb")
    is_connect = pgdb_cls._isconnected()
    rjson = {
        "status": "SUCCESS",
        "connect_status": is_connect
    }
    
    return jsonify(rjson)
    
@app.route('/api/v1/search', methods=['GET'])
@login_required
def gnsrch_api():
    verbose = 0
    srchqry = ''
    gn_log('gn search api initiated')
    # Get srchstring and then pass to search func
    if 'srchqry' in request.args:
        srchqry = request.args['srchqry']
        gn_log('GNSearch: search qry string:' + srchqry)
        # Remove "' begining and end
        srchqry_filtered = dequote(srchqry)
        slen = len(srchqry_filtered)

        # Let us invoke gnsrch api
        gn_log('GNPAppServer: search qry : ' + srchqry_filtered)

        # call gnsearch api
        #res = gngrph_srch_sqlqry_api(srchqry_filtered, verbose)
        gndata_folder=app.config["gnDataFolder"]
        gngraph_creds_folder=app.config["gnGraphDBCredsFolder"]
        ###res = gngrph_srch_metarepo_nodes_fetch(srchfilter, spark, gndata_folder, gngraph_creds_folder)

        
        res = gngrph_srch_datarepo_qry_fetch(gnsrch_ops, gnp_spark, srchqry_filtered)
        
        ###res_data = re.sub(r"(\w+):", r'"\1":', res)
        gn_log('GNPAppServer: Fetch Data Nodes with filter '+srchqry_filtered+' SUCCESS')
        gn_log('GNPAppServer: Fetch Data Nodes with filter '+srchqry_filtered+' SUCCESS')
        ##print(res)


        
        ##res = {}
        ##res_data = re.sub(r"(\w+):", r'"\1":', res)

        rjson = {
            "status": "SUCCESS",
            "gndata": res
        }
        return rjson

    else:
        errstr = {
            'status': 'ERROR',
            'errmsg': "No Search query provided "
        }
        # return jsonify('{status:"ERROR", errmsg:"No Search query provided
        # "}');
        return jsonify(errstr)


@app.route('/gnmetaview', methods=['GET'])
@login_required
def gnmetaview_cola_api():
    _srch = True if check_server_session() else False
    print('GnApp: gnview cola is initiated')
    return render_template('gnview/gnmetaview.html', disp_srch=_srch)


@app.route('/api/v1/metanodes', methods=['GET'])
@login_required
def gnmetanodes_fetch_api():

    verbose = 0
    print('GNPAppserver: Meta nodes search api ')
    # Get srchstring and then pass to search func
    if 'srchqry' in request.args:
        srchqry_raw = request.args['srchqry']

        # Remove "' begining and end
        srchqry = dequote(srchqry_raw)

        # Let us invoke gnsrch api
        gnlogger.info('GNPAppServer: search qry for metanodes : ' + srchqry)
    else:
        srchqry = ''

    # call gnmeta node search api Right now ignore searchqry arg

    ##res = gndwdb_metarepo_nodes_fetch_api(verbose)
    srchfilter=""
    res = gngrph_srch_metarepo_nodes_fetch(gnsrch_ops, gnp_spark, srchfilter) 
    ###res_data = re.sub(r"(\w+):", r'"\1":', res)
    gn_log('GNPAppServer:  metanode search  with filter '+srchfilter+'  SUCCESS : ')
    rjson = {
        "status": "SUCCESS",
        "data": res_data
    }

    # return json.JSONDecoder(object_pairs_hook=OrderedDict).decode()
    return res_data

    # return  json.dumps(rjson, indent=4, separators=(',', ': '))


@app.route('/api/v1/metaedges', methods=['GET'])
@login_required
def gnmetaedges_fetch_api():

    verbose = 0
    # Get srchstring and then pass to search func
    if 'srchqry' in request.args:
        srchqry_raw = request.args['srchqry']

        # Remove "' begining and end
        srchqry = dequote(srchqry_raw)

        # Let us invoke gnsrch api
        gn_log('GNPAppServer: search qry for metanodes : ' + srchqry)

    else:
        srchqry = ''

    # call gnmeta node search api Right now ignore searchqry arg

    ##res = gndwdb_metarepo_edges_fetch_api(srchqry, verbose)
    srchfilter=""

    res = gngrph_srch_metarepo_nodes_edges_fetch(gnsrch_ops, gnp_spark, srchfilter)
    
    ##res_data = re.sub(r"(\w+):", r'"\1":', res)
    gn_log('GNPAppServer: metanodes and edges with filter '+srchfilter+' SUCCESS ')
    
    rjson = {
        "status": "SUCCESS",
        "gndata": res
    }

    # return json.JSONDecoder(object_pairs_hook=OrderedDict).decode()
    return rjson


if __name__ == '__main__':
    
    app.run(host='0.0.0.0', port=5050, debug=True)
