import asyncio
from playwright.async_api import async_playwright
import re 

async def capture_firestore_responses(url="https://quinzaine.org", output_file="db/raw/raw.txt"):
    captured_bodies = [] # List to store response body text

    async with async_playwright() as p:
        try:
            browser = await p.firefox.launch()
            page = await browser.new_page()

            async def handle_response(response):
                if "firestore.googleapis.com" in response.url and "Listen/channel" in response.url:
                    print(f"Intercepting response from: {response.url} (Status: {response.status})")
                    try:
                        body_text = await response.text()
                        if body_text:
                            captured_bodies.append(body_text)
                            print(f"Captured response body chunk (length: {len(body_text)}).")
                        else:
                            print("Response body was empty.")
                    except Exception as e:
                        print(f"Error reading response body for {response.url}: {e}")

            page.on("response", handle_response)

            print(f"Navigating to {url}...")
            await page.goto(url, wait_until="networkidle", timeout=100000)
            print("Initial navigation complete. Waiting a bit longer for potential stream data...")
            await page.wait_for_timeout(10000)

            await browser.close()
            print("Browser closed.")

        except Exception as e:
            print(f"An error occurred during browser operation: {e}")
            if 'browser' in locals() and browser.is_connected():
                await browser.close()
            return

    if not captured_bodies:
        print("No Firestore response bodies were captured.")
        return

    print(f"\nCaptured {len(captured_bodies)} response body chunks.")
    try:
        with open(output_file, "w", encoding="utf-8") as f:
            separator = "\n--- SEPARATOR ---\n"
            full_content = separator.join(captured_bodies)
            f.write(full_content)
        print(f"Captured Firestore response bodies saved to {output_file}")
    except Exception as e:
        print(f"Error saving content to file: {e}")

async def main():
    await capture_firestore_responses()

if __name__ == "__main__":
    asyncio.run(main())