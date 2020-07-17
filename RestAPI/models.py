from configparser import ConfigParser
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.orm import relationship
from sqlalchemy import Column, Integer, String, DateTime, Numeric, ForeignKey
from flask_marshmallow import Marshmallow
from sqlalchemy.ext.declarative import declarative_base
import os


#data models orm (abstraction layer)
Base = declarative_base()
parser = ConfigParser()
parser.read('config.ini')
RS_PORT = parser.get('Redshift', 'port')
RS_USER = parser.get('Redshift', 'username')
RS_PASSWORD = os.environ['API_READER_PASSWORD']
DATABASE = parser.get('Redshift', 'database_name')
RS_HOST = parser.get('Redshift', 'url')
SSL_MODE = parser.get('Redshift', 'sslmode')

app = Flask(__name__)
app.config[
    'SQLALCHEMY_DATABASE_URI'] = "postgresql://{DB_USER}:{DB_PASS}@{DB_ADDR}:{DB_PORT}/{DB_NAME}?sslmode={SSL_MODE}".format(
    DB_USER=RS_USER, DB_PASS=RS_PASSWORD, DB_ADDR=RS_HOST, DB_PORT=RS_PORT, DB_NAME=DATABASE, SSL_MODE=SSL_MODE)
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)
ma = Marshmallow(app)


# database models
class PaymentMethod(db.Model):
    __tablename__ = 'dim_payment_method'
    __table_args__ = {"schema": "merchants"}
    payment_method_pk = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    payment_method_name = Column(String(25), nullable=False)

    payments = relationship("Payments", backref="PaymentMethod", lazy='dynamic')

    def __repr__(self):
        return '<PaymentMethod {0}>'.format(self.payment_method_name)


class PaymentStatus(db.Model):
    __tablename__ = 'dim_payment_status'
    __table_args__ = {"schema": "merchants"}
    payment_status_pk = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    payment_status_name = Column(String(25), nullable=False)

    payments = relationship("Payments", backref="PaymentStatus", lazy='dynamic')

    def __repr__(self):
        return '<PaymentStatus {0}>'.format(self.payment_status_name)


class Merchant(db.Model):
    __tablename__ = 'dim_merchant'
    __table_args__ = {"schema": "merchants"}
    merchant_pk = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    merchant_id = Column(String(100), nullable=False)
    merchant_name = Column(String(50))
    address = Column(String(100))
    phone_number = Column(String(30))
    email = Column(String(200))

    payments = relationship("Payments", backref="Merchant", lazy='dynamic')


class Payments(db.Model):
    __tablename__ = 'fact_payments'
    __table_args__ = {"schema": "merchants"}
    payment_id = Column(Integer, primary_key=True, nullable=False)
    merchant_fk = Column(Integer, ForeignKey('merchants.dim_merchant.merchant_pk'), nullable=False)
    payment_method_fk = Column(Integer, ForeignKey('merchants.dim_payment_method.payment_method_pk'), nullable=False)
    payment_status_fk = Column(Integer, ForeignKey('merchants.dim_payment_status.payment_status_pk'), nullable=False)
    payment_date = Column(DateTime, nullable=False)
    payment_amount = Column(Numeric(10, 2), nullable=False)
