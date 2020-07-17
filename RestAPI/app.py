from configparser import ConfigParser

from flask import Flask, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_marshmallow import Marshmallow
from sqlalchemy import func
from sqlalchemy.ext.declarative import declarative_base
from RestAPI.models import Merchant, Payments, PaymentStatus
import os

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
    'SQLALCHEMY_DATABASE_URI'] = "postgresql://{DB_USER}:{DB_PASS}@{DB_ADDR}:{DB_PORT}/{DB_NAME}?sslmode={SSL_MODE}" \
    .format(
    DB_USER=RS_USER, DB_PASS=RS_PASSWORD, DB_ADDR=RS_HOST, DB_PORT=RS_PORT, DB_NAME=DATABASE, SSL_MODE=SSL_MODE)
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)
ma = Marshmallow(app)

# to create requirements file pip freeze > requirements.txt

requests = [
    {
        'description': u'Get number of rejected payments for a given merchant over the past 7 days',
        'request': u'/get_merchant_rejected_payments_7_lastdays/merchant_id'
    },
    {
        'description': u'Get number of payments for a given merchant over the past 7 days',
        'request': u'/get_merchant_payments_7_lastdays/merchant_id'
    },
    {
        'description': u'Get amount of rejected payments for a given merchant over the past 7 days',
        'request': u'/get_merchant_rejected_payments_amount_7_lastdays/merchant_id'
    },
    {
        'description': u'Get amount of payments for a given merchant over the past 7 days',
        'request': u'/get_merchant_payments_amount_7_lastdays/merchant_id'
    }
]


@app.route('/')
def root():
    return jsonify({'requests': requests})


@app.route('/get_merchant_rejected_payments_7_lastdays/<string:merchant_id>', methods=['GET'])
def retrieve_merchant_rejects(merchant_id: str):
    merchant = Merchant.query.filter(Merchant.merchant_id == merchant_id).first()
    if merchant:
        rejected = PaymentStatus.query.filter(PaymentStatus.payment_status_name == 'Rejected').first().payment_status_pk
        result = Payments.query.filter(
            db.and_(Payments.payment_status_fk == rejected, Payments.payment_date >= func.current_date() - 7
                    , Payments.merchant_fk == merchant.merchant_pk)).count()
        return jsonify(rejected_payments_count=result)
    else:
        return jsonify("Merchant with " + merchant_id + ' id does not exists'), 404


@app.route('/get_merchant_payments_7_lastdays/<string:merchant_id>', methods=['GET'])
def retrieve_merchant_payments(merchant_id: str):
    merchant = Merchant.query.filter(Merchant.merchant_id == merchant_id).first()
    if merchant:
        result = Payments.query.filter(db.and_(Payments.payment_date >= func.current_date() - 7,
                                               Payments.merchant_fk == merchant.merchant_pk)).count()
        return jsonify(payments_count=result)
    else:
        return jsonify("Merchant with " + merchant_id + ' id does not exists'), 404


@app.route('/get_merchant_rejected_payments_amount_7_lastdays/<string:merchant_id>', methods=['GET'])
def retrieve_merchant_rejected_amount(merchant_id: str):
    merchant = Merchant.query.filter(Merchant.merchant_id == merchant_id).first()
    if merchant:
        rejected = PaymentStatus.query.filter(PaymentStatus.payment_status_name == 'Rejected').first().payment_status_pk
        result = \
            db.session.query(func.sum(Payments.payment_amount)) \
                .filter(db.and_(Payments.payment_status_fk == rejected,
                                Payments.payment_date >= func.current_date() - 7,
                                Payments.merchant_fk == merchant.merchant_pk)).first()[0]
        return jsonify(rejected_payments_amount=str(0 if result is None else result))
    else:
        return jsonify("Merchant with " + merchant_id + ' id does not exists'), 404


@app.route('/get_merchant_payments_amount_7_lastdays/<string:merchant_id>', methods=['GET'])
def retrieve_merchant_amount(merchant_id: str):
    merchant = Merchant.query.filter(Merchant.merchant_id == merchant_id).first()
    if merchant:
        result = db.session.query(func.sum(Payments.payment_amount)).filter(
            db.and_(Payments.payment_date >= func.current_date() - 7,
                    Payments.merchant_fk == merchant.merchant_pk)).first()[0]
        return jsonify(payments_amount=str(0 if result is None else result))
    else:
        return jsonify("Merchant with " + merchant_id + ' id does not exists'), 404


if __name__ == '__main__':
    app.run(port=5000, debug=True)
