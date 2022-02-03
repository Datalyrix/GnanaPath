###################################################################
#  GnanaPath Main App Server
#
#
#
#################################################################
import os
import sys
import time
import logging
import flask
from flask import request, jsonify, request, redirect, render_template, flash, url_for, session, Markup, abort
from werkzeug.utils import secure_filename
from flask_login import login_required, current_user, login_user, logout_user, LoginManager
from werkzeug.security import generate_password_hash
from auth.network_users import NetworkUser
import base64
import hashlib
from auth.reg_users import User
from moz_sql_parser import parse
import json
import re
from connect_form import ConnectServerForm, LoginForm
from collections import OrderedDict
from pathlib import Path
import json
import pathlib


### Add root directory to path
curentDir = os.getcwd()
listDir = curentDir.rsplit('/', 1)[0]
sys.path.append(listDir)

from gnutils.gn_log import gn_log, gn_log_err
from gn_config import gn_config_init, GNGraphConfigModel, GNGraphDBConfigModel, gn_pgresdb_getconfiguration

from gndatadis.gndd_filedb_ops import gndd_filedb_insert_file_api, gndd_filedb_filelist_get, gndd_filedb_fileinfo_bynode, gndd_filedb_filestate_set


from gngraph.search  import gngraph_search_client
from gngraph.ingest.gngraph_ingest_pd import gngraph_ingest_file_api
from gngraph.gngraph_dbops.gngraph_pgresdbops import GNGraphPgresDBOps
from gngraph.gngraph_dbops.gngraph_pgres_dbmgmtops import GNGraphPgresDBMgmtOps

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


gngraph_init_flag = 0
# set this flag to 1 if we want to use spark thread 
gnp_thread_flag=1

   
# Allowed extension you can set your own
ALLOWED_EXTENSIONS = set(['csv', 'json', ])

login_manager = LoginManager()
login_manager.init_app(app)

deploy_mode = os.getenv("GN_DEPLOY_MODE")

if deploy_mode == "Sandbox":
   app.config["deploy_mode"] = deploy_mode
   app.config["auth_server"] = os.getenv("GN_AUTH_SERVER")
   app.config["auth_server_port"] = os.getenv("GN_AUTH_SERVER_PORT")
   app.config["gn_acct_id"] =  os.getenv("GNACCTID")
   app.config["gn_serv_id"] =  os.getenv("GNSERVID")
   app.config["auth_server_login"] = 1
   all_users = {}
else:
   usr = User()
   all_users = {"gnadmin": usr}


def allowed_file(filename):
    return '.' in filename and filename.rsplit(
        '.', 1)[1].lower() in ALLOWED_EXTENSIONS


@login_manager.user_loader
def load_user(user_id):
    return all_users.get(user_id)

@app.before_first_request
def  init_components():
    print('GnAppSrv: Running Init prior to start app ')
    ###__gn_graph_init()

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
        return render_template('upload.html', disp_srch=_srch, file_res=fres, flen=flen)
    
    if request.method == 'POST':

        if 'fd' not in request.files:
            flash('No file part', 'danger')
            return redirect(request.url)

        files = request.files["fd"]
        fdesc = request.form["fdesc"]
        ftype = request.form["ftype"]
        fdelim = request.form["fdelim"]
        fbizdomain = request.form["bizdomain"]
        fnodename = request.form["nnameid"]
        
        if 'ingest_mode' in request.form:
            fingest = request.form['ingest_mode']
        else:
            fingest = 'off'
            
        ##if 'dataset_name' in request.form:
        ##    datasetname = request.form['dataset_name']
        ##    print(' Dataset name '+datasetname)
        ##else:
        ##    datasetname = ''

        ###bizdomain = request.form['bizdomain']
            
        
        if not allowed_file(files.filename):
            flash('Please upload CSV or JSON file', 'danger')
            return redirect(request.url)
        elif files and allowed_file(files.filename):
            fname = secure_filename(files.filename)
            file_name, file_ext = fname.split(".")
            filename= re.sub(r'\W+','', file_name)+f".{file_ext}"
            ####nodename, fext = filename.split(".")
            print('GnAppServ: file name '+filename+"   filenode: "+file_name)    
            files.save(os.path.join(app.config['gnUploadsFolder'], filename))
            gn_log("GnAppServ: uploaded file "+filename+" successfully ")
            if fnodename == "":
                fnodename = file_name

            if fbizdomain == "select":
               fbizdomain="other"
            ### For timebeing disable csv file upload
            ###gndwdbDataUpload(app.config['gnUploadsFolder'], filename)
            ###fdelim = ','
            fp = Path(app.config['gnUploadsFolder']+"/"+filename)
            fsize = fp.stat().st_size
            gndd_filedb_insert_file_api(filename, fsize, ftype, fbizdomain, fdesc, fdelim, fnodename, app.config["gnCfgDBFolder"])
            fres = gndd_filedb_filelist_get(app.config["gnCfgDBFolder"])
            flen = len(fres)
            
            if (fingest == 'on'):
                gn_log('GnAppServ: File ingestion is on ')
                #if (datasetname == ''):
                #    nodename, fext = filename.split(".")
                #    gn_log(' Getting node name '+nodename)
                #else:
                #    nodename = datasetname
                gn_log('GNAppSrv: Ingest file '+fnodename+ ' Business  Domain '+fbizdomain)    
                gngraph_ingest_file_api(filename, ftype, fdelim, fnodename, fbizdomain, app.config["gnDataFolder"], app.config["gnGraphDBCredsFolder"], app.config["gnCfgSettings"])
                
                gn_log('GnAppServ: Uploaded new file. Remap metarepo ')
                res = gngraph_search_client.gnsrch_meta_remap_request()
                
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
    gn_log('Current GnDBmode settings ')
    gn_log(gdbcfg_settings)

    pgres_conf = gn_pgresdb_getconfiguration(app.config["gnGraphDBCredsFolder"])
    gn_log(' GnDB Config settings ')
    gn_log(pgres_conf)
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
                ## dbmode is turned on
                #gngraph_datacopy_sf_to_pgres()
            else:
                cfg_setting['dbmode'] = 0

        ##gdbcfg.insert_op(cfg_setting)
        gdbcfg.upsert_op(cfg_setting)
        return redirect(url_for('gn_home'))    
          
    return render_template('gdb_config.html', title='Gnana Graph Server Config', cfg=gdbcfg_settings, pgres_conf=pgres_conf)
   

@app.route("/pgresdb_conf", methods=['GET', 'POST'])
@login_required
def pgresdb_config():

    form = ConnectServerForm()
    pgres_conf = GNGraphDBConfigModel(app.config["gnGraphDBCredsFolder"])
    conf_settings = GNGraphConfigModel(app.config["gnGraphDBCredsFolder"])
    conn = {}
    if (request.method == 'GET'):
        
       #if 'serverIP' in session and connect.search_res(session['serverIP']):
        conn = pgres_conf.get_op()
        print('app srv: ')
        print(conn)
        
        if conn :
          srv_ip_encode = base64.urlsafe_b64encode(
            conn['serverIP'].encode("utf-8"))
          srv_encode_str = str(srv_ip_encode, "utf-8")
          print('pgresConfForm: session ')
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

        return render_template('pgres_db_setup.html', title='Connect Graph Server', form=form, disp_srch=False, cfg=conn)
    
    if (request.method == 'POST'):
      print('pgres_db_conf: POST method input vars')
      in_vars = request.form.to_dict()
      print(in_vars)
      
      if 'newdbchk' in in_vars:
          newdbchk = in_vars['newdbchk']
          if (newdbchk == 'y'):
               print('new db check '+newdbchk)
      else:
          newdbchk = 'n'
          
      if 1 or form.validate_on_submit():
        result = request.form.to_dict()
        req_dict = pgres_conf.req_fields_json(result)
        print('DBConfig: POST ')
        print(req_dict)

        if (newdbchk == 'y'):        
           pgresdb_ops = GNGraphPgresDBMgmtOps(req_dict['serverIP'], req_dict['serverPort'], req_dict['username'], req_dict['password'],  req_dict['dbname'], '')
           iscreatedb = 1
           res = pgresdb_ops.gngraph_db_initialize(req_dict['dbname'], iscreatedb)
           if (res < 0):
               gn_log('GNPAppSrv: GNGraph DB Initialization failed ')
               flash(f'Error Initializing GN Graph Database Please look into log file for errors','danger')
               return render_template('pgres_db_setup.html', title='Graph DB Settings', form=form, disp_srch=False)
           

           gn_log('GNPAppSrv: GNGraph database initialized SUCCESS')
           
        pgres_conf.upsert_op(req_dict)
        res = "Success"
        if res == "Error":
            flash(
                f'Error connecting to db server {form.serverIP.data}',
                'danger')
            pgres_conf.delete_op(req_dict)
            return render_template(
                'pgres_db_setup.html', title='Graph DB Settings', form=form, disp_srch=False)
        else:
            session['serverIP'] = form.serverIP.data

            session['serverPort'] = form.serverPort.data
            flash(f'GNGraph database settings has been saved', 'success')
            return redirect(url_for('gdb_config_settings_page', disp_srch=True))
      else:
         print('Pgres Conf: Error in form submit')
         flash(f'Error on submit ', form.errors)         
         return render_template('pgres_db_setup.html', title='Connect Graph Server', form=form,  disp_srch=False)
        
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
    password = request.form["password"]

    if "auth_server_login" in app.config and app.config["auth_server_login"] == 1:
        
        auth_user = NetworkUser(app.config["auth_server"], app.config["auth_server_port"])
        encd = password.encode()
        res = hashlib.sha256(encd)
        ##passw_hash = generate_password_hash(password)
        passw_hash = res.hexdigest()
        status = auth_user.verify_user_pwhash(username, passw_hash, app.config["gn_acct_id"])
        if status == 1:
            all_users[username] = auth_user
            login_user(auth_user)
            return redirect(url_for('gn_home'))
        else:
            flash(f'Authentication Error  user', 'danger')
            return render_template("login.html", form=form)
    
    if username not in all_users:
        flash(f'Invalid  user', 'danger')
        return render_template("login.html", form=form)
    user = all_users[username]
    if not user.check_password(request.form["password"]):
        flash(f'Incorrect password', 'danger')
        return render_template("login.html", form=form)

    login_user(user)
    return redirect(url_for('gn_home'))

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

    if 'newdbchk' in request.args:
        newdbchk = request.args['newdbchk']
        if (newdbchk == 'true'):
            newdb = 1
        else:
            newdb = 0
    else:
        newdb = 0
        
    gn_log('new db val '+str(newdb))
    gn_log('Test DB Connection dbIP '+dbIP+"  port "+dbPort)
    if ((dbIP == '') or dbPort == '' or dbUser == '' or dbPasswd == ''):
           gn_log('GNPAppSrv: DB TestConn: Invalid args ')
           rjson = {
              "status": "FAIL",
               "connect_status": 0,
               "statusmsg": "Invalid Input Arguments"
              }
           return jsonify(rjson)

    if (newdb == 1):        
        pgdb_cls = GNGraphPgresDBOps(dbIP, dbPort, dbUser, dbPasswd,  "postgres", "")
    else:
        if (dbname):
           pgdb_cls = GNGraphPgresDBOps(dbIP, dbPort, dbUser, dbPasswd,  dbname, "") 
        else:
           gn_log('TestConn: dbname not provided for testing ')
           rjson = {
              "status": "FAIL",
               "connect_status": 0,
               "statusmsg": "Database name for testing not provided"
              }
           return jsonify(rjson)

    is_connect = pgdb_cls._isconnected()
    rjson = {
        "status": "SUCCESS",
        "connect_status": is_connect
    }
    
    return jsonify(rjson)
    
@app.route('/api/v1/search', methods=['GET'])
@login_required
def gnsrch_api():

    srchqry = ''
    gn_log('GnAppSrv: request for datanodes searching received')
    
    # Get srchstring and then pass to search func
    if 'srchqry' in request.args:
        srchqry = request.args['srchqry']
 
        if 'lnodes' in request.args:
            lnodes = request.args['lnodes']
        else:
            lnodes = 10

        if 'nodemode' in request.args:
            nodemode = int(request.args['nodemode'])
        else:
            nodemode = 1
                    
        # Remove "' begining and end
        srchqry_filtered = dequote(srchqry)
        slen = len(srchqry_filtered)

        gn_log('GnAppSrv: searching datanode with qry : ' + srchqry_filtered)

        ##Nodemode: 1 Nodes only, 2 Nodes+Edges, 3 Nodes+Edges+Derived nodes
        res = gngraph_search_client.gnsrch_dataqry_request(srchqry_filtered, nodemode, lnodes)
        
        gn_log('GnAppSrv: fetched datanodes with filter '+srchqry_filtered+' SUCCESS')

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

    gn_log('GnAppSrv:  request received for meta nodes search')
    # Get srchstring and then pass to search func
    if 'srchqry' in request.args:
        srchqry_raw = request.args['srchqry']

        # Remove "' begining and end
        srchqry = dequote(srchqry_raw)

        gn_log('GnAppSrv: search qry for metanodes : ' + srchqry)
    else:
        srchqry = ''


    srchfilter=""
    res = gngraph_search_client.gnsrch_metanodes_request()

    gn_log('GnAppSrv:  metanode search  with filter '+srchfilter+'  SUCCESS : ')
    if (res["status"] == "SUCCESS"):
        rjson = {
           "status": "SUCCESS",
           "gndata": res["data"]
        }
        
    else:
        rjson = {
            "status": "ERROR",
            "gndata": res["data"]
            }
              
    return rjson



@app.route('/api/v1/metaedges', methods=['GET'])
@login_required
def gnmetaedges_fetch_api():

    # Get srchstring and then pass to search func
    if 'srchqry' in request.args:
        srchqry_raw = request.args['srchqry']

        # Remove "' begining and end
        srchqry = dequote(srchqry_raw)

        # Let us invoke gnsrch api
        gn_log('GNPAppServer: search qry for metanodes : ' + srchqry)

    else:
        srchqry = ''

    srchfilter=""
    res = gngraph_search_client.gnsrch_metaqry_request(srchfilter)
    
    ##res_data = re.sub(r"(\w+):", r'"\1":', res)
    gn_log('GnAppSrv: metanodes and edges with filter '+srchfilter+' SUCCESS ')
    
    rjson = {
        "status": "SUCCESS",
        "gndata": res
    }

    return rjson

@app.route('/gnlog')
def gn_log_stream():
    def generate():
        with open(app.config["gnLogFilePath"]) as f:
            content= f.read()
            ##while True:
            ##    yield f.read()
            ##    time.sleep(10)
        return content    
    return app.response_class(generate(), mimetype='text/plain')


@app.route('/api/v1/ingnode', methods=['GET'])
@login_required
def  ingest_node_intodb():

    nodename= ''

    if 'node' in request.args:
        nodename = request.args['node']

    if (nodename == ''):
        print('GnAppSrv: Ingest node with: Invalid args ')
        rjson = {
            "status": "FAIL",
            "statusmsg": "Invalid node name ",
            }
        return jsonify(rjson)
    
    print('GnAppSrv: Ingesting node '+nodename+' into graphdb ')


    fres = gndd_filedb_fileinfo_bynode(nodename,app.config["gnCfgDBFolder"])

    if not bool(fres):
        print('GnAppSrv: Ingest node with: Invalid file ')
        rjson = {
            "status": "FAIL",
            "statusmsg": "Invalid node file ",
            }
        return jsonify(rjson)
        
    
    print('Filelog ')
    print(fres)
    gngraph_ingest_file_api(fres["filename"], fres["filetype"], fres["filedelim"], nodename, fres["bizdomain"], app.config["gnDataFolder"], app.config["gnGraphDBCredsFolder"], app.config["gnCfgSettings"])

    print('GnAppSrv: Node '+nodename+' is ingested ')
    ### Set the state 
    gndd_filedb_filestate_set(nodename, "INGESTED", app.config["gnCfgDBFolder"])
                            
    
    gn_log('GnAppSrv: Uploaded new node. Remap metarepo ')
    res = gngraph_search_client.gnsrch_meta_remap_request()
    
    rjson = {
        "status": "SUCCESS",
        "statusmsg": "Ingested node into database ",
        "node": nodename
    }
    return rjson

if __name__ == '__main__':
    
    gn_log('GnAppSrv: Starting GnSearch thread ')
    gngraph_search_client.__gn_graph_init(gnp_thread_flag, app.config["gnRootDir"])    
    
    gn_log('GnAppSrv: Running Flask App ')
    if (gnp_thread_flag == 0):
        __gn_graph_init()
    app.run(host='0.0.0.0', port=5050, debug=True)
    gn_log('GnAppSrv:  Started Flask App ')
