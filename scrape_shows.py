import asyncio
from datetime import datetime
from pyppeteer import launch
import os
import psycopg2
import boto3

from dotenv import load_dotenv


DAY_OF_WEEK_MAP = {
    1: 'Sun',
    2: 'Mon',
    3: 'Tue',
    4: 'Wed',
    5: 'Thu',
    6: 'Fri',
    7: 'Sat',
}

NEXT_WEEK_BUTTON_SELECTOR = (
    'button.flex.justify-center.items-center.w-8.h-full.'
    'border-2.border-kdvsblack.bg-kdvswhite.absolute.-right-1.translate-x-full'
)

def download_chromium():
    s3_client = boto3.client('s3')
    bucket_name = 'serverless-chromium'
    object_key = 'chromium'

    chromium_path = '/tmp/headless-chromium'

    try:
        print(f"Downloading Chromium binary from s3://{bucket_name}/{object_key} to {chromium_path}")
        s3_client.download_file(bucket_name, object_key, chromium_path)
        os.chmod('/tmp/headless-chromium', 0o755)
        print("Chromium binary downloaded successfully.")
    except Exception as e:
        print(f"Error downloading Chromium: {e}")
        raise e  # Re-raise the exception to halt the function if download fails

async def close_browser(browser):
    for page in await browser.pages():
        await page.close()
    await browser.close()

def fallback_to_pkill():
    try:
        print("Executing pkill to terminate Chrome...")
        os.system("pkill -f chrome")
        print("Chrome processes terminated with pkill.")
    except Exception as e:
        print(f"Error while executing pkill: {e}")

async def scrape_showSite(browser, show_url):
    if not show_url:
        return "Unknown", False

    page = await browser.newPage()
    try:
        print(f"Visiting show URL: {show_url}")
        await page.goto(show_url, timeout=30000)

        # Wait for the DJ name element to load
        await page.waitForSelector('.dj-name', {'timeout': 10000})
        dj_name = await page.evaluate('''() => {
            const djElement = document.querySelector('.dj-name');
            if (!djElement) return "Unknown";

            const djLink = djElement.querySelector('a');
            return djLink ? djLink.innerText.trim() : djElement.innerText.trim();
        }''')

        # Check the first <li> element in the <ul> with class "timeslot show-schedule"
        alternates = await page.evaluate('''() => {
            const timeslotUl = document.querySelector('ul.timeslot.show-schedule');
            if (!timeslotUl) return false;

            const firstLi = timeslotUl.querySelector('li');
            if (!firstLi) return false;

            const text = firstLi.innerText || "";
            return text.includes("Every other week") ? true : false;
        }''')

        return dj_name or "Unknown", alternates

    except Exception as e:
        print(f"Error scraping DJ name for URL {show_url}: {e}")
        return "Unknown", False

    finally:
        await page.close()

async def scrape_schedule(page, todays_date):
    print("Scraping schedule...")
    try:
        await page.waitForFunction(
            '''() => document.querySelectorAll('a[href^="https://spinitron.com/KDVS/show/"]').length >= 225''',
            {'timeout': 10000}
        )
    except asyncio.TimeoutError:
        print("Timeout occurred while waiting for the schedule to load. Attempting to scrape remaining <a> tags.")

    dates = await page.evaluate('''() => {
        const dateElements = document.querySelectorAll('.grid-cols-7 .text-center p span');
        const daysOfWeek = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
        const dateMap = {};
        
        dateElements.forEach((span, index) => {
            if (daysOfWeek[index]) {
                dateMap[daysOfWeek[index]] = span.innerText;
            }
        });
        return dateMap;
    }''')

    days_with_years = {}
    for day, date_str in dates.items():
        month, day_num = map(int, date_str.split('/'))
        today_year = todays_date.year
        today_month = todays_date.month

        if month == 12 and today_month == 1:
            year = today_year - 1
        elif month == 1 and today_month == 12:
            year = today_year + 1
        else:
            year = today_year

        days_with_years[day] = f"{month}/{day_num}/{year}"

    shows = await page.evaluate('''(DAY_OF_WEEK_MAP, days_with_years) => {
        return Array.from(document.querySelectorAll('a')).map(link => {
            let showName = link.querySelector('span:nth-child(1)')?.innerText || '';
            showName = showName.replace(/^\\[✔\\]\\s*/, '').trim();

            if (!showName || showName.trim() === '') {
                return null;
            }

            const timeElements = link.querySelectorAll('span');
            let startTime = 'Unknown';
            let endTime = 'Unknown';
            if (timeElements.length > 1) {
                const timeRange = timeElements[1]?.innerText || '';
                const timeParts = timeRange.split('-');
                if (timeParts.length == 2) {
                    startTime = timeParts[0].trim();
                    endTime = timeParts[1].trim();
                }
            }

            const reformatTime = (time) => {
                if (!time || time.trim() === '') {
                    return 'Unknown';
                }

                let normalizedTime = time.replace(/([APap][Mm])$/, ' $1').trim();
                const match = normalizedTime.match(/^(\\d{1,2})(?::(\\d{2}))?\\s*([APap][Mm])$/);
                if (!match) {
                    return 'Unknown';
                }

                let [_, hour, minute = '00', period] = match;
                hour = parseInt(hour);

                if (hour < 1 || hour > 12) {
                    return 'Unknown';
                }

                return `${hour}:${minute} ${period.toUpperCase()}`;
            };

            startTime = reformatTime(startTime);
            endTime = reformatTime(endTime);

            if (startTime === 'Unknown' || endTime === 'Unknown') {
                return null;
            }

            const style = link.getAttribute('style');
            const gridAreaMatch = style?.match(/grid-area:\\s*\\d+\\s*\\/\\s*(\\d+)\\s*\\//);
            const columnPosition = gridAreaMatch ? parseInt(gridAreaMatch[1]) : null;

            const dayOfWeek = columnPosition ? DAY_OF_WEEK_MAP[columnPosition] : 'Unknown';
            const currentDotw = columnPosition ? 
                new Intl.DateTimeFormat('en-US', { weekday: 'long' }).format(new Date(days_with_years[dayOfWeek])) : 
                'Unknown';

            if (dayOfWeek === 'Unknown') {
                return null;
            }

            const dateWithYear = days_with_years[dayOfWeek] || 'Unknown';
            const objectElement = link.querySelector('object.w-full.h-full.object-cover');
            const imageUrl = objectElement?.getAttribute('data') || null;
            const showURL = link.getAttribute('href') || null;

            return {
                showName,
                startTime,
                endTime,
                dayOfWeek,
                current_dotw: currentDotw, 
                date: dateWithYear,
                image_url: imageUrl,
                show_url: showURL,
            };
        }).filter(show => show !== null);
    }''', DAY_OF_WEEK_MAP, days_with_years)

    if not shows:
        print("No more valid <a> tags found. Finishing early.")
    return shows

async def scrape_all_schedules():
    current_day = datetime.now()

    # download_chromium()
    
    # chromium_path = '/tmp/headless-chromium'
    # if os.path.exists(chromium_path):
    #     print("Chromium binary found. Launching browser...")
    # else:
    #     print(f"Error: Chromium binary not found at {chromium_path}")

    browser = await launch(
        headless=True, 
        args=[
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-gpu',
        ],
        # executablePath="/tmp/headless-chromium",
        # userDataDir="/tmp",
    )
    page = await browser.newPage()

    try:
        print("Navigating to the KDVS schedule page...")
        await page.goto('https://kdvs.org/programming', timeout=60000)

        all_shows = []
        shows = await scrape_schedule(page, current_day)

        for show in shows:
            existing = next((s for s in all_shows if s['showName'] == show['showName'] and 
                            s['startTime'] == show['startTime'] and 
                            s['endTime'] == show['endTime']), None)
            if existing:
                existing['dates'].append(show['date'])
            else:
                all_shows.append({
                    'showName': show['showName'],
                    'startTime': show['startTime'],
                    'endTime': show['endTime'],
                    'dates': [show['date']],
                    'image_url': show['image_url'],
                    'current_dotw': show['current_dotw'],
                    'show_url': show['show_url']
                })

        for week in range(9):
            print(f"Processing week {week + 1}...")
            await page.waitForSelector(NEXT_WEEK_BUTTON_SELECTOR, {'timeout': 10000, 'visible': True})
            print("Found Selector: clicking...")
            await page.evaluate(f'document.querySelector("{NEXT_WEEK_BUTTON_SELECTOR}").click()')
            print("Clicked and waiting for schedule to load...")

            next_week_shows = await scrape_schedule(page, current_day)

            for show in next_week_shows:
                existing = next((s for s in all_shows if s['showName'] == show['showName'] and 
                                s['startTime'] == show['startTime'] and 
                                s['endTime'] == show['endTime']), None)
                if existing:
                    existing['dates'].append(show['date'])
                else:
                    all_shows.append({
                        'showName': show['showName'],
                        'startTime': show['startTime'],
                        'endTime': show['endTime'],
                        'dates': [show['date']],
                        'image_url': show['image_url'],
                        'current_dotw': show['current_dotw'],
                        'show_url': show['show_url']
                    })
        
        # Scrape DJ names+alternates for each show
        for show in all_shows:
            if show['show_url']:
                show['dj_name'], show['alternates'] = await scrape_showSite(browser, show['show_url'])


    except Exception as e:
        print(f"Error during scraping: {e}")

    finally:
        print("Shutting down...")
        try:
            await asyncio.wait_for(close_browser(browser), timeout=5)
            print("Browser closed successfully.")
        except asyncio.TimeoutError:
            print("Graceful shutdown timed out. Falling back to pkill...")
            fallback_to_pkill()
        except Exception as e:
            print(f"Unexpected error during shutdown: {e}")
            print("Falling back to pkill...")
            fallback_to_pkill()
            
    return all_shows

def save_to_database(shows):
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

        cursor = conn.cursor()

        cursor.execute("DROP TABLE IF EXISTS temp_shows;")
        cursor.execute("""
            CREATE TABLE temp_shows (LIKE shows INCLUDING ALL);
        """)

        for show in shows:
            formatted_dates = '{' + ','.join([f'"{date}"' for date in show['dates']]) + '}'
            cursor.execute(
                """
                INSERT INTO temp_shows (name, dj_name, start_time, end_time, show_dates, playlist_image_url, current_dotw, alternates)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    show['showName'],
                    show['dj_name'],
                    show['startTime'],
                    show['endTime'],
                    formatted_dates,
                    show['image_url'], 
                    show['current_dotw'],
                    show['alternates']
                )
            )

        cursor.execute("BEGIN;")
        cursor.execute("DROP TABLE shows;")
        cursor.execute("ALTER TABLE temp_shows RENAME TO shows;")
        cursor.execute("COMMIT;")

        conn.commit()
        print("Database updated successfully!")
    except Exception as e:
        print(f"Error saving to database: {e}")
        if conn:
            conn.rollback()
    finally:
        if 'conn' in locals() and conn:
            conn.close()

def lambda_handler(event, context):
    print("Starting scraping process...")
    shows = asyncio.run(scrape_all_schedules())
    print("Total Shows Scraped:", shows)
    save_to_database(shows)

    return {
        'statusCode': 200,
        'body': f"Successfully saved {len(shows)} shows to the database."
    }

load_dotenv()
lambda_handler(None, None)