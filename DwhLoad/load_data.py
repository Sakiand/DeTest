from configparser import ConfigParser
from DwhLoad.db_pool import CursorFromConnectionFromPool, Database
import os

parser = ConfigParser()
parser.read('config.ini')

# database init
Database.initialize(database = parser.get('Redshift', 'database_name'),
                    user = parser.get('Redshift', 'username'),
                    password = os.environ['RS_PASSWORD'],
                    port = parser.get('Redshift', 'port'),
                    host = parser.get('Redshift', 'url')
                    )

#load from s3 bucket source data
def load_to_mrr_from_s3(table, file):
    # get connection from pool
    with CursorFromConnectionFromPool() as cur:
        cur.execute("set search_path = mrr_merchants;")
        cur.execute("truncate table {table};".format(table=table))
        copy_query = "copy {table} from 's3://a4a-data/data/{file}' credentials 'aws_access_key_id={aws_access_key_id};" \
                     "aws_secret_access_key={aws_secret_access_key}' csv delimiter ',' ignoreheader 1 dateformat 'auto' region 'us-west-2' NULL AS 'NULL' EMPTYASNULL;" \
            .format(aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID'], aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY'], file=file,
                    table=table)
        cur.execute(copy_query)
        print("copy {table} comleted succesfully!".format(table=table))

#get last ingestion timestamp for payments table before loading
def get_from_ts(table):
    with CursorFromConnectionFromPool() as cur:
        with open('resources/get_from_ts.sql') as f:
            sql_statement = f.read().format(tableName=table)
            cur.execute(sql_statement)
            from_ts = cur.fetchone()[0].strftime("%Y-%m-%d")
            return from_ts

#get last ingestion timestamp for payments table after loading
def update_manage_source(table, to_ts):
    with CursorFromConnectionFromPool() as cur:
        with open('resources/update_manage_source.sql') as f:
            sql_statement = f.read().format(tableName=table, to_ts=to_ts)
            cur.execute(sql_statement)
            if cur.rowcount > 0:
                print('update_manage_source for ' + table + ' updated')

#update attribute dimensions data in case new payment methods, statuses or merchants added
def load_dim_data():
    print("loading from " + get_from_ts('merchants.payments'))
    with CursorFromConnectionFromPool() as cursor:
        cursor.execute("set search_path = merchants;")
        cursor.execute('insert into merchants.dim_payment_method(payment_method_name) ' \
                       'select distinct payment_method from mrr_merchants.payments' \
                       ' where ingestion_date > \'{from_ts}\' ' \
                       ' and not exists (select 1 from merchants.dim_payment_method ' \
                       ' where payments.payment_method = dim_payment_method.payment_method_name); ' \
                       .format(from_ts=get_from_ts('merchants.payments')) \
                       )
        print(str(cursor.rowcount) + ' rows loaded to dim_payment_method')

        cursor.execute('insert into merchants.dim_payment_status(payment_status_name) ' \
                       'select distinct status from mrr_merchants.payments' \
                       ' where ingestion_date  > \'{from_ts}\' ' \
                       ' and not exists (select 1 from merchants.dim_payment_status '
                       'where payments.status = dim_payment_status.payment_status_name); ' \
                       .format(from_ts=get_from_ts('merchants.payments')) \
                       )
        print(str(cursor.rowcount) + ' rows loaded to dim_payment_status')

        # SCD Implemented (tracks merchants private data changes)
        cursor.execute('create temp table stg_dim_merchant as ' \
                       'select  merchant_id, merchant_name, address, phone_number, email, ingestion_date as from_date, ' \
                       'md5(\'(\'||merchant_name||\',|,\'||address||\',|,\'||phone_number||\',|,\'||email||\')\') as details, ' \
                       'row_number() over (partition by merchant_id order by ingestion_date desc) as rn ' \
                       'from mrr_merchants.merchants; ' \
 \
                       'update merchants.dim_merchant  ' \
                       'set to_date = stg.from_date - 1, data_modified = current_timestamp ' \
                       'from stg_dim_merchant stg ' \
                       'where dim_merchant.merchant_id = stg.merchant_id and ' \
                       'md5(\'(\'||dim_merchant.merchant_name||\',|,\'||dim_merchant.address||\',|,\'||dim_merchant.phone_number||\',|,\'||dim_merchant.email||\')\') != details;' \
 \
                       'insert into merchants.dim_merchant(merchant_id, merchant_name, address, phone_number, email, from_date, to_date) ' \
                       'select merchant_id, merchant_name, address, phone_number, email, from_date, \'29991231\'::date as to_date ' \
                       'from stg_dim_merchant ' \
                       'where not exists(select 1 from dim_merchant where dim_merchant.merchant_id = stg_dim_merchant.merchant_id);' \
 \
                       'insert into merchants.dim_merchant(merchant_id, merchant_name, address, phone_number, email, from_date, to_date) ' \
                       'select merchant_id, merchant_name, address, phone_number, email, from_date, \'29991231\'::date as to_date ' \
                       'from stg_dim_merchant ' \
                       'where exists(select 1 from dim_merchant dm where dm.merchant_id = stg_dim_merchant.merchant_id ' \
                       'and md5(\'(\'||dm.merchant_name||\',|,\'||dm.address||\',|,\'||dm.phone_number||\',|,\'||dm.email||\')\') != details); ' \
 \
                       )
        print("dimension tables updated succesfully!")

#load payments data
def load_payments_data():
    with CursorFromConnectionFromPool() as cursor:

        from_ts = get_from_ts('merchants.payments')

        #get all payment dates that changes after last data load
        cursor.execute(
            'select distinct payment_date from mrr_merchants.payments where ingestion_date > \'{from_ts}\';'.format(
                from_ts=from_ts))
        payment_dates_to_load = tuple([d[0].strftime("%Y-%m-%d") for d in cursor.fetchall()])

        #check if new data ingected
        if len(payment_dates_to_load) > 0:
            cursor.execute(
                'create temp table stg_fact_payments as ' \
                'select payment_id, coalesce(dm.merchant_pk,-1) as merchant_fk, dpm.payment_method_pk, dps.payment_status_pk, payment_date, payment_amount, ingestion_date, fp.merchant_id  ' \
                'from mrr_merchants.payments fp ' \
                'left join merchants.dim_payment_method dpm on fp.payment_method = dpm.payment_method_name ' \
                'left join merchants.dim_payment_status dps on fp.status = dps.payment_status_name ' \
                'left join merchants.dim_merchant dm on fp.merchant_id = dm.merchant_id and fp.payment_date >= dm.from_date and fp.payment_date <= dm.to_date ' \
                'where payment_date in {dates};'.format(dates=payment_dates_to_load) \
                )

            #for late arriving merchants attribute data rows in merchants dimension added
            cursor.execute('insert into merchants.dim_merchant(merchant_id, merchant_name, address, phone_number, email, from_date, to_date) ' \
                           'select  merchant_id, \'unknown\', \'unknown\', \'unknown\', \'unknown\',min(ingestion_date), max(ingestion_date)' \
                           'from stg_fact_payments where merchant_fk = -1 ' \
                           'group by merchant_id;'
                           )

            payment_dates_to_load = str(payment_dates_to_load)

            #before load delete same dates data
            delete_statement = 'delete from merchants.fact_payments where payment_date in {dates};'.format(
                dates=payment_dates_to_load)
            print(delete_statement)
            cursor.execute(delete_statement)
            print(str(cursor.rowcount) + ' rows deleted')

            #load payments data
            cursor.execute(
                'insert into merchants.fact_payments(payment_id, merchant_fk, payment_method_fk, payment_status_fk, payment_date, payment_amount)  ' \
                'select payment_id, merchant_pk, payment_method_pk, payment_status_pk, payment_date, payment_amount ' \
                'from stg_fact_payments fp ' \
                'join merchants.dim_merchant dm on fp.merchant_id = dm.merchant_id and '
                'fp.payment_date >= dm.from_date and fp.payment_date <= dm.to_date;' \
                )

            print("payments table loaded succesfully with " + str(cursor.rowcount) + ' rows')

            #get max ingestion date in current batch
            cursor.execute('select distinct max(ingestion_date) from mrr_merchants.payments '
                           'where ingestion_date > \'{from_ts}\';'.format(from_ts=get_from_ts('merchants.payments')))
            to_ts = cursor.fetchone()[0].strftime("%Y-%m-%d")

            update_manage_source('merchants.payments', to_ts)
            print("data load process finished succesfully")
        else:
            print('No new data in sources')
