import json
import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()

def lambda_handler(event, context):
    print("Starting Lambda function...")

    try:
        print("Connecting to the database...")
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD")
        )
        print("Connected successfully!")

        cur = conn.cursor()

        # Query to fetch all shows
        query = "SELECT id, name, dj_name, start_time, end_time, current_dotw, show_dates, first_show_date, last_show_date, alternates, playlist_image_url FROM shows;"
        
        print(f"Executing query: {query}")
        cur.execute(query)

        # Fetch all results
        shows = cur.fetchall()

        show_list = []
        for show in shows:
            show_dict = {
                "id": show[0],
                "name": show[1],
                "dj_name": show[2],
                "start_time": show[3].strftime('%H:%M:%S'),
                "end_time": show[4].strftime('%H:%M:%S'),
                "current_dotw": show[5],
                "dates": show[6],
                "first_show_date": show[7].strftime('%Y-%m-%d'),
                "last_show_date": show[8].strftime('%Y-%m-%d'),
                "alternates": show[9],
                "playlist_image_url": show[10],
            }
            show_list.append(show_dict)

        return {
            'statusCode': 200,
            'body': json.dumps(show_list)
        }

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        return {
            'statusCode': 500,
            'body': f"Error: {str(e)}"
        }

    finally:
        if conn:
            cur.close()
            conn.close()
            print("Database connection closed.")

print(lambda_handler(None, None))