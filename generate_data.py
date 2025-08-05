import mysql.connector
from faker import Faker
import random
from datetime import datetime, timedelta

# MySQL 資料庫連線資訊
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '@@@@@@@',
    'database': 'kkday_project'
}

# 建立 Faker 實例
fake = Faker('zh_TW')  # 使用台灣的虛擬資料設定

# 產品清單，用來隨機選擇
product_categories = [
    '一日遊', '美食體驗', '門票', '交通票券', '當地體驗',
    '戶外活動', '獨家行程', '表演門票', '住宿', '活動體驗',
    '水上活動', '一日體驗', '遊樂園', '秘境探險', '文化饗宴',
    '夜間動物園', '親子體驗', '烹飪學校', '放鬆渡假', '保護園區'
]
product_cities = [
    '台北', '新北', '台中', '台南', '高雄',
    '東京', '大阪', '首爾', '曼谷', '新加坡', '紐約', '墨爾本',
    '清邁', '上海', '北京', '釜山', '香港', '吉隆坡', '檳城'
]


def get_db_connection():
    """建立並回傳資料庫連線物件"""
    return mysql.connector.connect(**DB_CONFIG)


def generate_and_insert_data(num_users=500, num_products=350, num_orders=600):
    """
    生成並插入使用者、產品與訂單資料。

    參數:
    num_users (int): 欲生成的假使用者數量。
    num_products (int): 欲生成的假產品數量。
    num_orders (int): 欲生成的假訂單數量。
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # 1. 生成並插入使用者資料
        print("Generating users...")
        for _ in range(num_users):
            username = fake.name()
            email = fake.unique.email()  # 使用 unique 確保 email 不重複
            registration_date = fake.date_between(start_date='-2y', end_date='today')
            country = random.choice(['Taiwan', 'USA', 'Japan', 'South Korea', 'Thailand', 'Malaysia', 'Singapore'])

            cursor.execute(
                "INSERT INTO users (username, email, registration_date, country) VALUES (%s, %s, %s, %s)",
                (username, email, registration_date, country)
            )
        print(f"Generated {num_users} users.")

        # 2. 生成並插入產品資料
        print("Generating products...")
        for _ in range(num_products):
            product_name = fake.word() + ' ' + random.choice(['體驗', '之旅', '門票', '一日遊'])
            category = random.choice(product_categories)
            city = random.choice(product_cities)
            price = round(random.uniform(500, 5000), 2)
            is_active = True  # 確保產品為上架狀態，避免 IndexError

            cursor.execute(
                "INSERT INTO products (product_name, category, city, price, is_active) VALUES (%s, %s, %s, %s, %s)",
                (product_name, category, city, price, is_active)
            )
        print(f"Generated {num_products} products.")

        # 提交 products 資料，確保它們在生成訂單前存在
        conn.commit()

        # 3. 生成並插入訂單資料
        print("Generating orders...")

        # 在生成訂單前，先從資料庫中獲取所有 user_id 和 product_id
        cursor.execute("SELECT user_id FROM users")
        user_ids = [row[0] for row in cursor.fetchall()]

        cursor.execute("SELECT product_id FROM products WHERE is_active = TRUE")
        active_product_ids = [row[0] for row in cursor.fetchall()]

        # 檢查 active_product_ids 是否為空，避免錯誤
        if not active_product_ids:
            print("Error: No active products available. Cannot generate orders.")
            return

        for _ in range(num_orders):
            user_id = random.choice(user_ids)
            order_date = fake.date_time_between(start_date='-1y', end_date='now')
            status = random.choice(['已完成', '待付款', '已取消'])

            # 計算訂單總金額
            num_items = random.randint(1, 3)
            item_total = 0

            # 先插入 orders 表
            cursor.execute(
                "INSERT INTO orders (user_id, order_date, status, total_amount) VALUES (%s, %s, %s, %s)",
                (user_id, order_date, status, 0)  # 暫時設為 0
            )
            order_id = cursor.lastrowid

            # 接著插入 order_items 表，並計算總金額
            purchased_product_ids = []
            for _ in range(num_items):
                product_id = random.choice(active_product_ids)

                # 避免同一筆訂單購買重複產品
                if product_id not in purchased_product_ids:
                    quantity = random.randint(1, 2)

                    cursor.execute("SELECT price FROM products WHERE product_id = %s", (product_id,))
                    price_per_unit = cursor.fetchone()[0]

                    item_total += price_per_unit * quantity

                    cursor.execute(
                        "INSERT INTO order_items (order_id, product_id, quantity, price_per_unit) VALUES (%s, %s, %s, %s)",
                        (order_id, product_id, quantity, price_per_unit)
                    )
                    purchased_product_ids.append(product_id)

            # 更新訂單總金額
            cursor.execute(
                "UPDATE orders SET total_amount = %s WHERE order_id = %s",
                (item_total, order_id)
            )
        print(f"Generated {num_orders} orders and their items.")

        conn.commit()
        print("Data generation complete!")

    except mysql.connector.Error as err:
        print(f"Error: {err}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


if __name__ == '__main__':
    # 在 MySQL Workbench 中執行以下指令：
    # SET FOREIGN_KEY_CHECKS = 0; TRUNCATE TABLE order_items; TRUNCATE TABLE orders; TRUNCATE TABLE products; TRUNCATE TABLE users; SET FOREIGN_KEY_CHECKS = 1;
    generate_and_insert_data(num_users=500, num_products=350, num_orders=600)