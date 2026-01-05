import os
import pymysql
import requests
from datetime import date, timedelta
import time

# =====================
# CONFIG (FROM ENVIRONMENT)
# =====================
DB_CONFIG = {
    "host": os.environ["DB_HOST"],
    "user": os.environ["DB_USER"],
    "password": os.environ["DB_PASSWORD"],
    "database": os.environ["DB_NAME"],
    "cursorclass": pymysql.cursors.DictCursor
}

BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

YESTERDAY = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")

CATEGORY_FILTER = """
master_category_id IN (
978,962,887,873,818,1,2,73,84,91,225,240,624,226,416,619,
3,4,5,6,221,224,236,365,505,599,621,635,183,334,544,695,
8,186,237,235,596,537,868,918,940,1009,1010
)
AND master_category_id != 802
AND order_media NOT IN ('B2B','Bondhu')
"""

# =====================
# DB CONNECT WITH RETRY
# =====================
def connect_db(retries=3, delay=5):
    for i in range(retries):
        try:
            return pymysql.connect(**DB_CONFIG)
        except pymysql.err.OperationalError as e:
            print(f"[{i+1}/{retries}] DB connection failed: {e}")
            time.sleep(delay)
    raise Exception("Failed to connect to DB after retries")

# =====================
# RUN QUERY
# =====================
def run_query(sql):
    conn = connect_db()
    with conn.cursor() as cur:
        cur.execute(sql)
        result = cur.fetchone()
    conn.close()
    if result is None:
        return 0
    return list(result.values())[0] or 0  # Ensure 0 if None

# =====================
# CALCULATE METRICS
# =====================
created_orders = run_query(f"""
SELECT COUNT(DISTINCT order_unique_id)
FROM partner_order_report
WHERE order_first_created = '{YESTERDAY}'
AND order_media != 'B2B'
AND {CATEGORY_FILTER}
""")

served_orders = run_query(f"""
SELECT COUNT(DISTINCT order_unique_id)
FROM partner_order_report
WHERE closed_date = '{YESTERDAY}'
AND order_media != 'B2B'
AND {CATEGORY_FILTER}
""")

cancelled_orders = run_query(f"""
SELECT COUNT(DISTINCT order_unique_id)
FROM partner_order_report
WHERE cancelled_date = '{YESTERDAY}'
AND order_media != 'B2B'
AND {CATEGORY_FILTER}
""")

served_gmv = run_query(f"""
SELECT ROUND(SUM(gmv))
FROM partner_order_report
WHERE closed_date = '{YESTERDAY}'
AND order_media != 'B2B'
AND {CATEGORY_FILTER}
""")

served_nr = run_query(f"""
SELECT ROUND(SUM(
(
  IF(service_id LIKE '%2392%' AND sp_cost != 0,
        gmv - sp_cost_service - sp_cost_additional - sp_cost_delivery - discount_partner - discount_sheba,
  IF(service_id LIKE '%3446%' AND sp_cost != 0,
        gmv - sp_cost_service - sp_cost_additional - sp_cost_delivery - discount_partner - discount_sheba,
  IF(service_id LIKE '%2392%', gmv/8700*2500,
  IF(service_id LIKE '%3446%', gmv/10250*3450,
  gmv - sp_cost_service - sp_cost_additional - sp_cost_delivery - discount_partner - discount_sheba
)))))/ (CASE WHEN order_first_created >= '2025-11-03' THEN 115 ELSE 105 END) * 100
))
FROM partner_order_report
WHERE closed_date = '{YESTERDAY}'
AND order_media != 'B2B'
AND {CATEGORY_FILTER}
""")

# =====================
# SEND TELEGRAM MESSAGE
# =====================
message = f"""
📊 *Daily Order Summary*

📅 *Date:* {YESTERDAY}

Created Orders: *{created_orders:,}*
Served Orders: *{served_orders:,}*
Cancelled Orders: *{cancelled_orders:,}*

Served GMV: *{served_gmv:,}*
Served NR: *{served_nr:,}*
"""

try:
    resp = requests.post(
        f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
        json={"chat_id": CHAT_ID, "text": message, "parse_mode": "Markdown"}
    )
    resp.raise_for_status()
    print("Telegram message sent successfully")
except requests.exceptions.RequestException as e:
    print(f"Failed to send Telegram message: {e}")
