-- 旅遊網站營運數據分析專案 SQL 腳本
-- 這個檔案包含了從資料庫建立、虛擬資料生成到核心數據分析的所有 SQL 查詢。

-- Step 1: 資料庫與資料表設定
DROP DATABASE IF EXISTS travel_website_project;
CREATE DATABASE travel_website_project;
USE travel_website_project;

-- 建立使用者資料表
CREATE TABLE users (
    user_id INT PRIMARY KEY AUTO_INCREMENT,
    username VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    registration_date DATE NOT NULL,
    country VARCHAR(100)
);

-- 建立產品資料表
CREATE TABLE products (
    product_id INT PRIMARY KEY AUTO_INCREMENT,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    city VARCHAR(100) NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE
);

-- 建立訂單資料表
CREATE TABLE orders (
    order_id INT PRIMARY KEY AUTO_INCREMENT,
    user_id INT NOT NULL,
    order_date DATETIME NOT NULL,
    status VARCHAR(50) NOT NULL,
    total_amount DECIMAL(10, 2) NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

-- 建立訂單明細資料表
CREATE TABLE order_items (
    item_id INT PRIMARY KEY AUTO_INCREMENT,
    order_id INT NOT NULL,
    product_id INT NOT NULL,
    quantity INT NOT NULL,
    price_per_unit DECIMAL(10, 2) NOT NULL,
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);


-- Step 2: 核心分析查詢

-- 查詢 A: 各類別最熱銷的產品
-- 目的: 找出每個產品類別中，銷量最高的單一產品。
WITH ProductSales AS (
    SELECT
        p.category,
        p.product_name,
        SUM(oi.quantity) AS total_quantity,
        ROW_NUMBER() OVER(PARTITION BY p.category ORDER BY SUM(oi.quantity) DESC) AS ranking
    FROM
        order_items AS oi
    JOIN
        products AS p ON oi.product_id = p.product_id
    GROUP BY
        p.category, p.product_name
)
SELECT
    category,
    product_name,
    total_quantity
FROM
    ProductSales
WHERE
    ranking = 1;

-- 查詢 B: 各國家的消費行為分析（優化版）
-- 目的: 深入了解不同國家的消費模式與趨勢。
-- 備註: 此查詢已調整以符合 MySQL 的 ONLY_FULL_GROUP_BY 嚴格模式。
SELECT
    u.country,
    YEAR(o.order_date) AS order_year,
    COUNT(DISTINCT o.user_id) AS total_customers,
    COUNT(o.order_id) AS total_orders,
    SUM(o.total_amount) AS total_revenue,
    AVG(o.total_amount) AS avg_order_amount,
    AVG(oi_counts.num_items_per_order) AS avg_items_per_order,
    (
        SELECT p.category
        FROM orders AS o2
        JOIN order_items AS oi2 ON o2.order_id = oi2.order_id
        JOIN products AS p ON oi2.product_id = p.product_id
        WHERE o2.user_id IN (SELECT user_id FROM users WHERE country = u.country)
        -- 關鍵修正：這裡直接使用外部查詢的 order_year 欄位
        AND YEAR(o2.order_date) = order_year 
        GROUP BY p.category
        ORDER BY SUM(oi2.quantity) DESC
        LIMIT 1
    ) AS top_category
FROM
    users AS u
JOIN
    orders AS o ON u.user_id = o.user_id
LEFT JOIN (
    SELECT order_id, COUNT(product_id) AS num_items_per_order
    FROM order_items
    GROUP BY order_id
) AS oi_counts ON o.order_id = oi_counts.order_id
GROUP BY
    u.country, order_year
ORDER BY
    u.country, order_year;


-- 查詢 C: 回購率分析（按註冊月份）
-- 目的: 追蹤不同用戶群（cohort）的回購行為趨勢。
WITH UserFirstOrder AS (
    SELECT
        user_id,
        MIN(order_date) AS first_order_date
    FROM
        orders
    GROUP BY
        user_id
),
UserOrderCounts AS (
    SELECT
        user_id,
        COUNT(order_id) AS order_count
    FROM
        orders
    GROUP BY
        user_id
)
SELECT
    DATE_FORMAT(ufo.first_order_date, '%Y-%m') AS first_order_month,
    COUNT(uoc.user_id) AS total_customers,
    COUNT(CASE WHEN uoc.order_count > 1 THEN uoc.user_id END) AS returning_customers,
    CAST(COUNT(CASE WHEN uoc.order_count > 1 THEN uoc.user_id END) AS DECIMAL(10, 4)) / COUNT(uoc.user_id) AS repurchase_rate
FROM
    UserFirstOrder AS ufo
LEFT JOIN
    UserOrderCounts AS uoc ON ufo.user_id = uoc.user_id
GROUP BY
    first_order_month
ORDER BY
    first_order_month;

-- 查詢 D: 訂單取消率分析（按產品類別）
-- 目的: 找出哪些產品類別的取消率最高，協助營運團隊優化產品。
SELECT
    p.category,
    COUNT(CASE WHEN o.status = '已取消' THEN o.order_id END) AS canceled_orders,
    COUNT(o.order_id) AS total_orders,
    CAST(COUNT(CASE WHEN o.status = '已取消' THEN o.order_id END) AS DECIMAL(10, 4)) / COUNT(o.order_id) AS cancellation_rate
FROM
    orders AS o
JOIN
    order_items AS oi ON o.order_id = oi.order_id
JOIN
    products AS p ON oi.product_id = p.product_id
GROUP BY
    p.category
ORDER BY
    cancellation_rate DESC;
