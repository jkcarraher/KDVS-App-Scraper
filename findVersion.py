import pyppeteer
import asyncio

async def findVersion():
    browser = await pyppeteer.launch();
    print(await browser.version())
    await browser.close()

asyncio.run(findVersion())