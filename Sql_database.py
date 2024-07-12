import sqlite3
con = sqlite3.connect("d2k_test.db")

cur = con.cursor()


cur.execute('''CREATE TABLE green_taxi(VendorID, 
            lpep_pickup_datetime, 
            lpep_dropoff_datetime, 
            passenger_count,
            trip_distance,
            RatecodeID,
            store_and_fwd_flag,
            PULocationID,
            DOLocationID,
            payment_type,
            fare_amount,
            extra,
            mta_tax,
            tip_amount,
            tolls_amount,
            improvement_surcharge,
            total_amount,
            trip_type,
            congestion_surcharge,
            trip_duration,
            average_speed)''')

cur.execute('''CREATE TABLE yellow_taxi(VendorID, 
            tpep_pickup_datetime, 
            tpep_dropoff_datetime, 
            passenger_count,
            trip_distance,
            RatecodeID,
            store_and_fwd_flag,
            PULocationID,
            DOLocationID,
            payment_type,
            fare_amount,
            extra,
            mta_tax,
            tip_amount,
            tolls_amount,
            improvement_surcharge,
            total_amount,
            congestion_surcharge,
            trip_duration,
            average_speed)''')


