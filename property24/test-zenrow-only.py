import os
import time
import requests
import traceback
import concurrent.futures
import random
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from pydub import AudioSegment
from speech_recognition import Recognizer, AudioFile, UnknownValueError, RequestError
from bs4 import BeautifulSoup
from seleniumwire import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver import ActionChains
from dotenv import load_dotenv

# ---------------------- Massive User-Agent Pool ------------------------------
USER_AGENTS = [
    # Desktop Chrome
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_2_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    # Desktop Firefox
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:118.0) Gecko/20100101 Firefox/118.0",
    # Desktop Edge
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0",
    # Desktop Safari
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_0_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.4 Safari/605.1.15",
    # Mobile Chrome
    "Mozilla/5.0 (Linux; Android 14; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 13; Pixel 6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Mobile Safari/537.36",
    # Mobile Safari
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPad; CPU OS 16_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.4 Mobile/15E148 Safari/604.1",
    # Mobile Firefox
    "Mozilla/5.0 (Android 14; Mobile; rv:124.0) Gecko/124.0 Firefox/124.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) FxiOS/124.0 Mobile/15E148 Safari/605.1.15",
    # More recent Chrome/Edge/Safari (for rotation)
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.2 Safari/605.1.15",
    # ... add even more as needed!
]
# Randomly pick a UA every browser session
def get_random_user_agent():
    return random.choice(USER_AGENTS)

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
load_dotenv(override=True)

URLS = [
    "https://www.property24.com/for-sale/bergville/bergville/kwazulu-natal/5659/115453952",
    "https://www.property24.com/for-sale/cliffdale/hammarsdale/kwazulu-natal/7119/115863578",
    "https://www.property24.com/for-sale/summer-place-estate/bronkhorstspruit/gauteng/33410/115964231",
    "https://www.property24.com/for-sale/orange-farm/evaton/gauteng/3738/115927880",
]

ZENROWS_PROXY = "http://GtRCPB8ZCqJS:YTXv6KQEw4vU@superproxy.zenrows.com:1337"
SELENIUMWIRE_OPTIONS = {
    'proxy': {
        'http': ZENROWS_PROXY,
        'https': ZENROWS_PROXY,
        'no_proxy': 'localhost,127.0.0.1'
    }
}

def get_stealth_chrome_options():
    options = Options()
    ua = get_random_user_agent()
    options.add_argument(f"--user-agent={ua}")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_argument("--disable-webrtc")
    options.add_argument("--disable-blink-features=AutomationControlled")
    # options.add_argument("--headless=new")  # Uncomment if needed
    options.add_argument("--disable-infobars")
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--window-size=1280,900")
    return options

def human_sleep(a=0.7, b=2.2):
    t = random.uniform(a, b)
    time.sleep(t)

def human_scroll(driver):
    total_scroll = random.randint(400, 1000)
    for _ in range(3):
        step = random.randint(50, 200)
        driver.execute_script(f"window.scrollBy(0, {step});")
        human_sleep(0.2, 0.7)

def audio_to_text(mp3_path, wav_path, retries=2):
    recognizer = Recognizer()
    audio = AudioSegment.from_mp3(mp3_path)
    audio.export(wav_path, format="wav")
    with AudioFile(wav_path) as source:
        audio_data = recognizer.record(source)
    for attempt in range(retries + 1):
        try:
            return recognizer.recognize_google(audio_data)
        except UnknownValueError:
            if attempt < retries:
                print(f"  ⚠️ Could not transcribe, retry {attempt + 1}")
                time.sleep(1)
                continue
            else:
                print("  ❌ Google Speech Recognition could not understand audio.")
                return ""
        except RequestError as e:
            print(f"  ❌ Could not request results from Google Speech Recognition: {e}")
            return ""
        except Exception as e:
            print(f"  ❌ Unexpected error in audio_to_text: {e}")
            return ""

def process_url(url, idx, total_attempts=2):
    for attempt in range(1, total_attempts + 1):
        print(f"[{idx}] Attempt {attempt} for URL: {url}")

        opts = get_stealth_chrome_options()
        driver = None
        mp3_file = f"audio_{idx}.mp3"
        wav_file = f"audio_{idx}.wav"
        try:
            driver = webdriver.Chrome(options=opts, seleniumwire_options=SELENIUMWIRE_OPTIONS)
            # Patch JS properties for stealth
            driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
                "source": """
                    Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                    window.navigator.chrome = { runtime: {} };
                    Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
                    Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
                """
            })
            driver.get(url)
            print(f"[{idx}] Loaded page.")
            human_sleep(1, 3)
            human_scroll(driver)

            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "a.js_contactNumbersLink"))
            )

            numbers_selector = ".p24_sidebarAgentContactNumber"
            buttons = driver.find_elements(By.CSS_SELECTOR, "a.js_contactNumbersLink")

            if not buttons:
                print(f"[{idx}] No ‘Show Contact Numbers’ button.")
                continue

            first_btn = buttons[0]
            if first_btn.is_displayed() and first_btn.size['height'] > 0 and first_btn.size['width'] > 0:
                ActionChains(driver).move_to_element(first_btn).perform()
                human_sleep(0.5, 1.5)
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", first_btn)
            else:
                print(f"[{idx}] Button not interactable, will try JavaScript click.")
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", first_btn)
                human_sleep(0.5, 1.5)

            bframe = None
            numbers_shown = False

            for click_attempt in range(1, 4):
                print(f"[{idx}] Click attempt {click_attempt}…")
                try:
                    first_btn.click()
                except WebDriverException:
                    driver.execute_script("arguments[0].click();", first_btn)
                human_sleep(1, 2)
                if driver.find_elements(By.CSS_SELECTOR, numbers_selector):
                    print(f"[{idx}] ✅ Numbers appeared.")
                    numbers_shown = True
                    break
                for iframe in driver.find_elements(By.TAG_NAME, "iframe"):
                    src = iframe.get_attribute("src") or ""
                    if "recaptcha/api2/bframe" in src:
                        bframe = iframe
                        print(f"[{idx}] → Found reCAPTCHA iframe.")
                        break
                if bframe:
                    break

            if bframe and not numbers_shown:
                for audio_try in range(1, 4):
                    print(f"[{idx}] Audio challenge attempt {audio_try}…")
                    try:
                        driver.switch_to.default_content()
                        frames = driver.find_elements(By.TAG_NAME, "iframe")
                        bframe = next(
                            (f for f in frames if (f.get_attribute("src") or "").startswith("https://www.google.com/recaptcha/api2/bframe")),
                            None
                        )
                        if not bframe:
                            print(f"[{idx}] ❌ Couldn’t re-find the audio iframe—aborting.")
                            break
                        driver.switch_to.frame(bframe)

                        if audio_try == 1:
                            WebDriverWait(driver, 5).until(
                                EC.element_to_be_clickable((By.ID, "recaptcha-audio-button"))
                            ).click()
                        else:
                            WebDriverWait(driver, 5).until(
                                EC.element_to_be_clickable((By.ID, "recaptcha-reload-button"))
                            ).click()
                            WebDriverWait(driver, 5).until(
                                EC.element_to_be_clickable((By.ID, "recaptcha-audio-button"))
                            ).click()
                    except TimeoutException:
                        print(f"[{idx}] ⚠️ Audio toggle/reload not found.")
                        continue

                    try:
                        download_el = WebDriverWait(driver, 10).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, "a.rc-audiochallenge-tdownload-link"))
                        )
                        audio_url = download_el.get_attribute("href")
                    except TimeoutException:
                        print(f"[{idx}] ⚠️ Audio download link not found.")
                        continue

                    for fn in (mp3_file, wav_file):
                        if os.path.exists(fn):
                            os.remove(fn)

                    with open(mp3_file, "wb") as f:
                        f.write(requests.get(audio_url).content)
                    print(f"[{idx}] 🔊 Downloaded {mp3_file}")

                    transcript = audio_to_text(mp3_file, wav_file)
                    if not transcript:
                        print(f"[{idx}] 🛑 Failed to transcribe audio, skipping this attempt…")
                        continue

                    print(f"[{idx}] 📝 Transcribed: {transcript}")
                    try:
                        WebDriverWait(driver, 5).until(
                            EC.presence_of_element_located((By.ID, "audio-response"))
                        ).send_keys(transcript)
                        driver.execute_script("document.getElementById('recaptcha-verify-button').click();")
                        print(f"[{idx}] ✅ Submitted response")
                    except Exception as e:
                        print(f"[{idx}] ⚠️ Error submitting CAPTCHA: {e}")
                        continue

                    human_sleep(2, 4)
                    driver.switch_to.default_content()

                    if driver.find_elements(By.CSS_SELECTOR, numbers_selector):
                        print(f"[{idx}] 🎉 Numbers appeared—done.")
                        numbers_shown = True
                        break
                    else:
                        print(f"[{idx}] ❌ No numbers yet—retrying…")

            driver.switch_to.default_content()
            human_sleep(1, 2)
            soup = BeautifulSoup(driver.page_source, "html.parser")
            phones = [el.get_text(strip=True) for el in soup.select(".js_contactNumbersDiv .p24_sidebarAgentContactNumber")]
            if phones:
                print(f"[{idx}] Extracted phone numbers:", phones)
                return  # SUCCESS, don't retry anymore
            else:
                print(f"[{idx}] ❌ No phone numbers found. Will retry if attempts left.")

        except Exception as e:
            print(f"[{idx}] 💥 Unexpected error: {e}")
            traceback.print_exc()
        finally:
            for fn in (mp3_file, wav_file):
                if os.path.exists(fn):
                    os.remove(fn)
            if driver:
                driver.quit()
            print(f"[{idx}] Cleaned up audio files and closed browser.")

    print(f"[{idx}] 🚨 Failed to extract numbers after {total_attempts} attempts: {url}")

def main():
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = [
            executor.submit(process_url, url, idx)
            for idx, url in enumerate(URLS)
        ]
        concurrent.futures.wait(futures)

if __name__ == "__main__":
    main()
