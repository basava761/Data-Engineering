"""
=============================================================
  Naukri Resume Upload DAG
  - Runs at: 9 AM, 11 AM, 1 PM, 4 PM, 5 PM, 8 PM (IST)
  - Uploads resume from current directory to Naukri portal
  - Max 10 uploads per day (Airflow handles schedule)
  - Uses Selenium for browser automation
=============================================================

SETUP INSTRUCTIONS:
-------------------
1. Install dependencies:
      pip install apache-airflow selenium webdriver-manager

2. Place this DAG file in your Airflow DAGs folder:
      $AIRFLOW_HOME/dags/naukri_resume_upload_dag.py

3. Place your resume PDF in the folder defined by RESUME_DIR below.

4. Fill in the placeholders marked with  <-- FILL THIS
"""

import os
import glob
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# ─────────────────────────────────────────────
#  USER CONFIG — Fill these placeholders
# ─────────────────────────────────────────────

NAUKRI_EMAIL    = "basavaraj761@gmail.com"       # <-- FILL THIS
NAUKRI_PASSWORD = "Aa@1234567"           # <-- FILL THIS

# Folder where your resume PDF lives
RESUME_DIR      = "/home/patil/Downloads/spark/int/resumes"

# Resume filename
RESUME_FILENAME = "Basavaraj_DataEngineer_Resume (1).pdf"

# ─────────────────────────────────────────────
#  DAG DEFAULT ARGS
# ─────────────────────────────────────────────

default_args = {
    "owner"           : "basavaraj",
    "depends_on_past" : False,
    "email_on_failure": False,
    "email_on_retry"  : False,
    "retries"         : 1,
    "retry_delay"     : timedelta(minutes=5),
}

# ─────────────────────────────────────────────
#  SCHEDULE
#  Cron runs at 9,11 AM  and  1,4,5,8 PM IST
#  IST = UTC+5:30  →  subtract 5h30m for UTC cron
#
#  IST  → UTC
#  09:00 → 03:30
#  11:00 → 05:30
#  13:00 → 07:30
#  16:00 → 10:30
#  17:00 → 11:30
#  20:00 → 14:30
# ─────────────────────────────────────────────

# Using MultiCronTrigger-style string (Airflow 2.4+ supports multiple crons)
# If your Airflow version is < 2.4, use the timetable approach below.
SCHEDULE = "30 3,5,7,10,11,14 * * *"


# ─────────────────────────────────────────────
#  HELPER: Resolve resume file path
# ─────────────────────────────────────────────

def get_resume_path() -> str:
    """Return the resume file path. Auto-picks latest PDF if filename not set."""
    if RESUME_FILENAME:
        path = os.path.join(RESUME_DIR, RESUME_FILENAME)
        if not os.path.exists(path):
            raise FileNotFoundError(f"Resume not found at: {path}")
        return path

    # Auto-detect: pick the most recently modified PDF in RESUME_DIR
    pdfs = glob.glob(os.path.join(RESUME_DIR, "*.pdf"))
    if not pdfs:
        raise FileNotFoundError(f"No PDF files found in: {RESUME_DIR}")
    latest = max(pdfs, key=os.path.getmtime)
    logging.info(f"Auto-selected resume: {latest}")
    return latest


# ─────────────────────────────────────────────
#  CORE TASK: Upload resume to Naukri
# ─────────────────────────────────────────────

def upload_resume_to_naukri(**context):
    """
    Selenium automation to:
      1. Log in to Naukri.com
      2. Navigate to profile resume section
      3. Upload the resume PDF
    """
    import time
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from webdriver_manager.chrome import ChromeDriverManager

    resume_path = get_resume_path()
    logging.info(f"Uploading resume: {resume_path}")

    # ── Browser Options ──────────────────────
    options = Options()
    options.add_argument("--headless")           # Run without GUI (server/Airflow)
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )
    wait = WebDriverWait(driver, 30)

    try:
        # ── Step 1: Open Naukri Login Page ────
        logging.info("Opening Naukri login page...")
        driver.get("https://www.naukri.com/nlogin/login")
        time.sleep(3)

        # ── Step 2: Enter Email ────────────────
        email_field = wait.until(
            EC.presence_of_element_located((By.XPATH, "//input[@placeholder='Enter your active Email ID / Username']"))
        )
        email_field.clear()
        email_field.send_keys(NAUKRI_EMAIL)

        # ── Step 3: Enter Password ─────────────
        password_field = driver.find_element(By.XPATH, "//input[@placeholder='Enter your password']")
        password_field.clear()
        password_field.send_keys(NAUKRI_PASSWORD)

        # ── Step 4: Click Login ────────────────
        login_btn = driver.find_element(By.XPATH, "//button[@type='submit' and contains(text(),'Login')]")
        login_btn.click()
        time.sleep(4)

        logging.info(f"Logged in. Current URL: {driver.current_url}")

        # ── Step 5: Go to Profile Page ─────────
        driver.get("https://www.naukri.com/mnjuser/profile")
        time.sleep(4)

        # ── Step 6: Click 'Update Resume' ─────
        update_resume_btn = wait.until(
            EC.element_to_be_clickable(
                (By.XPATH, "//input[@type='file' and contains(@id,'attachCV')]")
            )
        )

        # Send file path directly to the hidden file input
        update_resume_btn.send_keys(resume_path)
        time.sleep(5)

        # ── Step 7: Confirm upload success ────
        try:
            success_msg = wait.until(
                EC.presence_of_element_located(
                    (By.XPATH, "//*[contains(text(),'Resume updated') or contains(text(),'successfully')]")
                )
            )
            logging.info(f"Upload successful! Message: {success_msg.text}")
        except Exception:
            logging.warning("Could not confirm success message — please verify manually.")

        logging.info(
            f"Resume upload task completed at "
            f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S IST')}"
        )

    except Exception as e:
        logging.error(f"Resume upload failed: {e}")
        driver.save_screenshot("/tmp/naukri_error_screenshot.png")
        logging.info("Screenshot saved to /tmp/naukri_error_screenshot.png")
        raise

    finally:
        driver.quit()


# ─────────────────────────────────────────────
#  DAG DEFINITION
# ─────────────────────────────────────────────

with DAG(
    dag_id          = "naukri_resume_upload",
    default_args    = default_args,
    description     = "Auto-upload resume to Naukri at 9,11AM | 1,4,5,8PM IST",
    schedule_interval = SCHEDULE,
    start_date      = datetime(2025, 1, 1),
    catchup         = False,
    max_active_runs = 1,
    tags            = ["naukri", "resume", "job-search"],
) as dag:

    upload_task = PythonOperator(
        task_id         = "upload_resume_to_naukri",
        python_callable = upload_resume_to_naukri,
        provide_context = True,
    )

    upload_task