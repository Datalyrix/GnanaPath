from flask_wtf import FlaskForm
from wtforms import StringField, PasswordField, SubmitField
from wtforms.validators import DataRequired, InputRequired, ValidationError
import re


class ConnectServerForm(FlaskForm):
    serverIP = StringField('Graph DB server IP : Port',
                           validators=[DataRequired()])
    username = StringField('Username',
                           validators=[DataRequired()])
    password = PasswordField('Password', validators=[DataRequired()])
    connect = SubmitField('Connect')

    def validate_serverIP(form, field):
        ipvalue = field.data
        regexp = r'\d{1,3}\.\d{1,3}\.\d{1,3}.\d{1,3}$'
        try:
            server_ip, srv_port = ipvalue.split(":")
            srv_port = int(srv_port)
        except ValueError:
            raise ValidationError(
                "Invalid input, Please provide a valid server ip/port")
        if not re.search(regexp, server_ip):
            raise ValidationError("Invalid IP, Please provide a valid IP")
        if not int(srv_port) in range(1, 65535):
            raise ValidationError(
                "Invalid port, Please provide a valid port no")


class LoginForm(FlaskForm):
    username = StringField('Username',
                           [DataRequired()])

    def validate_username(form, field):
        print(field.data)
        if field.data != "gnadmin":
            raise ValidationError(
                "Invalid username, Please check the username entered")

    password = PasswordField('Password', [DataRequired()])

    def validate_password(form, field):
        print(field.data)
        if field.data != "gnana":
            raise ValidationError(
                "Incorrect password, Please check the password entered")

    login = SubmitField('Login')
