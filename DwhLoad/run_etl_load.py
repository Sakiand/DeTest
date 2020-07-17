from DwhLoad.dwh_init import create_dwh_tables, create_mrr_tables
from DwhLoad.load_data import load_to_mrr_from_s3, load_dim_data, load_payments_data

if __name__ == '__main__':
    # create mrr tables
    create_mrr_tables()

    # create dwh structure if not exists
    create_dwh_tables()

    # load mrr data
    load_to_mrr_from_s3(table='mrr_merchants.merchants', file='merchants.csv')
    load_to_mrr_from_s3(table='mrr_merchants.payments', file='payments.csv')

    # update dimensions data
    load_dim_data()

    # load payments data
    load_payments_data()
