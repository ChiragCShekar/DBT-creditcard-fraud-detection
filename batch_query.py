'''import mysql.connector

conn = mysql.connector.connect(user='root', password='mansi@711', database='creditdb')
cursor = conn.cursor()

query = """
SELECT card_id, COUNT(DISTINCT location)
FROM transactions
GROUP BY card_id
HAVING COUNT(DISTINCT location) > 1;
"""

cursor.execute(query)
for row in cursor.fetchall():
    print(row)

cursor.close()
conn.close()
'''

import mysql.connector

try:
    print("🔌 Connecting to MySQL...")
    conn = mysql.connector.connect(
        user='root',
        password='mansi@711',
        database='creditdb'
    )
    cursor = conn.cursor()

    print("📄 Running batch query...")
    query = """
    SELECT card_id, COUNT(DISTINCT location)
    FROM transactions
    GROUP BY card_id
    HAVING COUNT(DISTINCT location) > 1;
    """
    cursor.execute(query)
    results = cursor.fetchall()

    if results:
        print("📊 Results:")
        for row in results:
            print(f"Card ID: {row[0]}, Unique Locations: {row[1]}")
    else:
        print("✅ No duplicated card usage across multiple locations found.")

except mysql.connector.Error as err:
    print(f"❌ MySQL Error: {err}")

except Exception as e:
    print(f"⚠️ Other Error: {e}")

finally:
    if 'cursor' in locals():
        cursor.close()
    if 'conn' in locals() and conn.is_connected():
        conn.close()
    print("🔒 Connection closed.")
