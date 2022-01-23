from . import userdb
from flask_login import UserMixin


class User(UserMixin):

    def __init__(self):

        self.username = userdb.SimpleDBUser().get_username()
        self.password = userdb.SimpleDBUser().get_password()

    def check_password(self, password):
        return userdb.check_password_hash(self.password, password)

    def get_id(self):
        return self.username
