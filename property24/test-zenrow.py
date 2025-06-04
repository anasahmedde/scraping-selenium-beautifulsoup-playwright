import os
import time
import requests
import traceback
import concurrent.futures
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from pydub import AudioSegment
from speech_recognition import Recognizer, AudioFile
from bs4 import BeautifulSoup
from undetected_chromedriver import Chrome, ChromeOptions
from speech_recognition import Recognizer, AudioFile, UnknownValueError, RequestError

from dotenv import load_dotenv

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

ZENROWS_API_KEY = os.getenv("ZENROWS_API_KEY")
ZENROWS_PROXY = f"http://apikey:{ZENROWS_API_KEY}@gw.zenrows.com:8001"

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

        opts = ChromeOptions()
        # Use ZenRows as proxy
        opts.add_argument(f"--proxy-server={ZENROWS_PROXY}")
        opts.add_argument(
            "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/114.0.5735.199 Safari/537.36"
        )
        # opts.add_argument("--headless")  # Uncomment for headless operation

        driver = None
        mp3_file = f"audio_{idx}.mp3"
        wav_file = f"audio_{idx}.wav"
        try:
            driver = Chrome(options=opts, headless=False)
            driver.get(url)
            print(f"[{idx}] Loaded page.")

            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "a.js_contactNumbersLink"))
            )

            numbers_selector = ".p24_sidebarAgentContactNumber"
            buttons = driver.find_elements(By.CSS_SELECTOR, "a.js_contactNumbersLink")

            if not buttons:
                print(f"[{idx}] No ‘Show Contact Numbers’ button.")
                continue

            first_btn = buttons[0]
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", first_btn)

            bframe = None
            numbers_shown = False

            for click_attempt in range(1, 4):
                print(f"[{idx}] Click attempt {click_attempt}…")
                try:
                    first_btn.click()
                except WebDriverException:
                    driver.execute_script("arguments[0].click();", first_btn)
                time.sleep(5)
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

                    time.sleep(3)
                    driver.switch_to.default_content()

                    if driver.find_elements(By.CSS_SELECTOR, numbers_selector):
                        print(f"[{idx}] 🎉 Numbers appeared—done.")
                        numbers_shown = True
                        break
                    else:
                        print(f"[{idx}] ❌ No numbers yet—retrying…")

            driver.switch_to.default_content()
            time.sleep(2)
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
