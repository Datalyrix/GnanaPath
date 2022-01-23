from tinydb import TinyDB, Query
from werkzeug.security import generate_password_hash, check_password_hash

class SimpleDBUser():
   def __init__(self):
       self.db=TinyDB('user_list.json')
       self.set_password('gnana')
   
   def set_password(self,password):
       self.password_hash = generate_password_hash(password)
       
     
   def check_password(self,password):
       return check_password_hash(self.password_hash,password)
  
   def create_user(self):
       if not self.get_user_details(): 
           pwd= self.password_hash
           user1={'username':'gnadmin','password':pwd}
           self.db.insert(user1)
       else:
          print('User already exists')

   def get_user_details(self):
       User=Query();
       user_details = self.db.search(User.username == 'gnadmin')
       return user_details
   
   def get_username(self):
       if not self.get_user_details():
            self.create_user()
       uname=self.get_user_details()
       return uname[0]['username']

   def get_password(self):
       if not self.get_user_details():
            self.create_user()
       upwd = self.get_user_details()
       return  upwd[0]['password']

