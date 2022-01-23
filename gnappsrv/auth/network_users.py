import requests
import os
import sys
from flask_login import UserMixin

currentDir = os.getcwd()
listDir = currentDir.rsplit('/', 1)[0]
sys.path.append(listDir)
parentDir = currentDir.rsplit('/', 1)[0]
sys.path.append(parentDir)


from gnutils.gn_log import gn_log, gn_log_err

class NetworkUser(UserMixin):

    def __init__(self, auth_srv, auth_port):
        
        #self.username = userdb.SimpleDBUser().get_username()
        #self.password = userdb.SimpleDBUser().get_password()
        self.auth_server = auth_srv
        self.auth_server_port = auth_port
        
    def verify_user_pwhash(self, user_login, passwhash, acctid):
        
        ###http://gnanapath.com:4000/gnaccount/login?email=vycgs2013@gmail.com&password=esdsdssddss

        if self.auth_server_port is None:
            server_url = "http://"+self.auth_server
        else:
            server_url = "http://"+self.auth_server+":"+self.auth_server_port
            
        user_params = {'email': user_login, 'password': passwhash}
        server_url = server_url+"/gnaccount/login"
        gn_log("UserAuth: sending url "+server_url)
        resp = requests.get(url = server_url, params=user_params)

        if resp.status_code != 200:
            gn_log('UserAuth: Error in getting response status_code:'+str(resp.status_code))
            return -1
        
        user_resp = resp.json()
        ###status = data['results'][0]['geometry']['location']['lat']
        status = user_resp['status']

        if (status == "SUCCESS"):
            self.gn_user = user_resp["user"]
            ## verify that accountid matches
            gn_acct_id = self.gn_user["id"]
            if gn_acct_id != acctid:
                gn_log('UserAuth: User credentials didnt match with account registered')
                return -1
            self.username = user_login
            self.password = passwhash
            gn_log('UserAuth: User credentials verified ')
            return 1
        else:
            return -1
    def check_password(self, password):
        return userdb.check_password_hash(self.password, password)

    def get_id(self):
        return self.username
