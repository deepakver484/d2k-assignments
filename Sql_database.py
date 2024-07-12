import sqlite3
import logging



def database_creation(name):
    con = sqlite3.connect(f"{name}")
    logging.info('database created')
    cur = con.cursor()


    # cur.execute('''CREATE TABLE IF NOT EXISTS green_taxi(VendorID, 
    #             lpep_pickup_datetime, 
    #             lpep_dropoff_datetime, 
    #             passenger_count,
    #             trip_distance,
    #             RatecodeID,
    #             store_and_fwd_flag,
    #             PULocationID,
    #             DOLocationID,
    #             payment_type,
    #             fare_amount,
    #             extra,
    #             mta_tax,
    #             tip_amount,
    #             tolls_amount,
    #             improvement_surcharge,
    #             total_amount,
    #             trip_type,
    #             congestion_surcharge,
    #             trip_duration,
    #             average_speed)''')

    # logging.info('green_taxi table created')
    # cur.execute('''CREATE TABLE IF NOT EXISTS yellow_taxi(VendorID, 
    #             tpep_pickup_datetime, 
    #             tpep_dropoff_datetime, 
    #             passenger_count,
    #             trip_distance,
    #             RatecodeID,
    #             store_and_fwd_flag,
    #             PULocationID,
    #             DOLocationID,
    #             payment_type,
    #             fare_amount,
    #             extra,
    #             mta_tax,
    #             tip_amount,
    #             tolls_amount,
    #             improvement_surcharge,
    #             total_amount,
    #             congestion_surcharge,
    #             trip_duration,
    #             average_speed)''')

    # logging.info('Yellow_taxi table created')

    cur.execute('''CREATE TABLE IF NOT EXISTS yellow_taxi_aggregate_data(
                Date,
                total_trip,
                avg_fare_amount 
                )''')

    logging.info('yellow_taxi_aggregate_data table created')

    cur.execute('''CREATE TABLE IF NOT EXISTS green_taxi_aggregate_data(
                Date,
                total_trip,
                avg_fare_amount 
                )''')

    logging.info('green_taxi_aggregate_data table created')

    return name