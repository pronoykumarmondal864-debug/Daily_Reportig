import pymysql
import requests
from datetime import date, timedelta

# =====================
# CONFIG (ENV VARS)
# =====================
DB_CONFIG = {
    "host":     "${{ secrets.DB_HOST }}",
    "user":     "${{ secrets.DB_USER }}",
    "password": "${{ secrets.DB_PASSWORD }}",
    "database": "${{ secrets.DB_NAME }}",
    "cursorclass": pymysql.cursors.DictCursor
}

BOT_TOKEN = "${{ secrets.TELEGRAM_BOT_TOKEN }}"
CHAT_ID  = "${{ secrets.TELEGRAM_CHAT_ID }}"

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
# DB QUERY EXECUTOR
# =====================
def run_query(sql):
    conn = pymysql.connect(**DB_CONFIG)
    with conn.cursor() as cur:
        cur.execute(sql)
        result = cur.fetchone()
    conn.close()
    return list(result.values())[0]

# =====================
# METRICS
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
)))))
 /
 (CASE WHEN order_first_created >= '2025-11-03' THEN 115 ELSE 105 END) * 100
))
FROM partner_order_report
WHERE closed_date = '{YESTERDAY}'
AND order_media != 'B2B'
AND {CATEGORY_FILTER}
""")

# =====================
# TELEGRAM MESSAGE
# =====================
message = f"""
📊 *Daily Order Summary*

📅 *Date:* {YESTERDAY}

Created Orders: *{created_orders}*
Served Orders: *{served_orders}*
Cancelled Orders: *{cancelled_orders}*

Served GMV: *{served_gmv:,}*
Served NR: *{served_nr:,}*
"""

requests.post(
    f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
    json={"chat_id": CHAT_ID, "text": message, "parse_mode": "Markdown"}
)
