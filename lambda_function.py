import json
import os
from dotenv import load_dotenv
import psycopg2
from datetime import datetime
import pytz

# Load environment variables
load_dotenv()

def lambda_handler(event=None, context=None):
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
        query = "SELECT id, name, dj_name, start_time, end_time, current_dotw, show_dates, first_show_date, last_show_date FROM shows;"
        
        print(f"Executing query: {query}")
        cur.execute(query)

        # Fetch all results
        shows = cur.fetchall()

        # Get current date and time in PST
        pst = pytz.timezone('US/Pacific')
        now = datetime.now(pst)
        today_dotw = now.strftime('%A')  # Today's day of the week
        today_date = now.strftime('%Y-%m-%d')  # Today's date

        print(f"Today's DOTW: {today_dotw}")
        print(f"Today's date: {today_date}")

        matching_show = None

        for show in shows:
            show_dict = {
                "id": show[0],
                "name": show[1],
                "dj_name": show[2],
                "start_time": show[3].strftime('%H:%M:%S'),
                "end_time": show[4].strftime('%H:%M:%S'),
                "current": show[5],
                "dates": show[6],
                "first_show_date": show[7].strftime('%Y-%m-%d'),
                "last_show_date": show[8].strftime('%Y-%m-%d'),
            }

            # Convert start_time and end_time to datetime objects
            start_time = datetime.combine(now.date(), show[3])  # Combine today's date with the time
            end_time = datetime.combine(now.date(), show[4])  # Combine today's date with the time

            # Localize start_time and end_time to PST
            start_time = pytz.timezone('US/Pacific').localize(start_time)
            end_time = pytz.timezone('US/Pacific').localize(end_time)

            # Check if the show matches today's DOTW and if the time is right now
            if show[5] == today_dotw and today_date in show[6]:
                # Check if current time is between start_time and end_time
                if start_time <= now <= end_time:
                    matching_show = show_dict
                    break

        if matching_show:
            return {
                'statusCode': 200,
                'body': json.dumps(matching_show)
            }
        else:
            return {
                'statusCode': 404,
                'body': "No matching show found."
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
            
# Local testing entry point
if __name__ == "__main__":
    response = lambda_handler()
    print(f"Function Response: {response}")
