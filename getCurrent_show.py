import json
import os
from dotenv import load_dotenv
import psycopg2
from datetime import datetime

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

        # Get the current date and time
        now = datetime.now()
        current_time = now.strftime('%H:%M:%S')
        current_date = now.strftime('%Y-%m-%d')

        # Query to fetch shows that are happening right now
        query = """
        SET TIMEZONE TO 'America/Los_Angeles';
        SELECT id, name, dj_name, start_time, end_time, current_dotw, show_dates, 
            first_show_date, last_show_date, alternates, playlist_image_url 
        FROM shows
        WHERE (
                -- Case 1: Show starts and ends on the same day
                (start_time <= CURRENT_TIME AND end_time >= CURRENT_TIME AND CURRENT_DATE = ANY (show_dates::date[]))
                -- Case 2: Show spans midnight, and we are on the starting day
                OR (start_time <= CURRENT_TIME AND end_time < start_time AND CURRENT_DATE = ANY (show_dates::date[]))
                -- Case 3: Show spans midnight, and we are on the ending day
                OR (start_time > end_time AND CURRENT_TIME < end_time AND CURRENT_DATE - INTERVAL '1 day' = ANY (show_dates::date[]))
            )
        LIMIT 1;
        """

        
        print(f"Executing query: {query}")
        cur.execute(query, (current_date, current_time, current_time))

        # Fetch the result (only the first show)
        show = cur.fetchone()

        if show:
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

            # Return the first show found
            return {
                'statusCode': 200,
                'body': json.dumps(show_dict)
            }

        # If no show is found, return a not found message
        return {
            'statusCode': 404,
            'body': "No show found."
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