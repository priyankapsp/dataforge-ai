
import mysql.connector
import os
from dotenv import load_dotenv

load_dotenv()

conn = mysql.connector.connect(
    host=os.getenv("MYSQL_HOST"),
    port=int(os.getenv("MYSQL_PORT")),
    user=os.getenv("MYSQL_USER"),
    password=os.getenv("MYSQL_PASSWORD"),
    database=os.getenv("MYSQL_DATABASE")
)
cursor = conn.cursor()

# Create tables
cursor.execute("""
CREATE TABLE IF NOT EXISTS elf_stores (
    store_id INT PRIMARY KEY,
    store_name VARCHAR(100),
    city VARCHAR(100),
    state VARCHAR(50),
    country VARCHAR(50),
    store_type VARCHAR(50),
    monthly_target DECIMAL(10,2),
    is_active BOOLEAN
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS elf_inventory (
    inventory_id INT PRIMARY KEY,
    store_id INT,
    product_sku VARCHAR(50),
    product_name VARCHAR(200),
    quantity_on_hand INT,
    reorder_point INT,
    unit_cost DECIMAL(10,2)
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS elf_orders (
    order_id INT PRIMARY KEY,
    store_id INT,
    customer_id INT,
    order_date DATETIME,
    product_sku VARCHAR(50),
    quantity INT,
    unit_price DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    order_status VARCHAR(50)
)
""")

# Insert stores
stores = [
    (1, 'ELF NYC Flagship', 'New York', 'NY', 'USA', 'Flagship', 150000.00, True),
    (2, 'ELF LA Sunset', 'Los Angeles', 'CA', 'USA', 'Standard', 120000.00, True),
    (3, 'ELF Chicago Loop', 'Chicago', 'IL', 'USA', 'Standard', 90000.00, True),
    (4, 'ELF Houston Galleria', 'Houston', 'TX', 'USA', 'Standard', 85000.00, True),
    (5, 'ELF Miami Beach', 'Miami', 'FL', 'USA', 'Premium', 110000.00, True),
]
cursor.executemany(
    "INSERT IGNORE INTO elf_stores VALUES(%s,%s,%s,%s,%s,%s,%s,%s)",
    stores
)

# Insert inventory
inventory = [
    (1, 1, 'ELF-83428', 'Poreless Putty Primer', 450, 100, 5.50),
    (2, 1, 'ELF-83729', 'Halo Glow Liquid Filter', 320, 80, 7.00),
    (3, 2, 'ELF-82416', 'Power Grip Primer', 380, 90, 6.00),
    (4, 2, 'ELF-84024', 'Camo CC Cream', 240, 60, 7.50),
    (5, 3, 'ELF-81788', 'Lash N Roll', 550, 120, 5.00),
    (6, 3, 'ELF-83331', 'No Budge Shadow Stick', 400, 100, 4.00),
    (7, 4, 'ELF-84201', 'Glossy Lip Stain', 340, 80, 5.00),
    (8, 5, 'ELF-83025', 'Lip Lacquer', 280, 70, 4.00),
    (9, 1, 'ELF-84024', 'Camo CC Cream', 15, 60, 7.50),
    (10, 4, 'ELF-83428', 'Poreless Putty Primer', 8, 100, 5.50),
]
cursor.executemany(
    "INSERT IGNORE INTO elf_inventory VALUES(%s,%s,%s,%s,%s,%s,%s)",
    inventory
)

# Insert orders
orders = [
    (1001, 1, 501, '2026-04-09 10:23:00', 'ELF-83428', 2, 10.00, 20.00, 'COMPLETED'),
    (1002, 1, 502, '2026-04-09 11:45:00', 'ELF-83729', 1, 14.00, 14.00, 'COMPLETED'),
    (1003, 2, 503, '2026-04-09 12:30:00', 'ELF-82416', 3, 12.00, 36.00, 'COMPLETED'),
    (1004, 2, 504, '2026-04-09 14:15:00', 'ELF-84024', 2, 15.00, 30.00, 'COMPLETED'),
    (1005, 3, 505, '2026-04-09 15:00:00', 'ELF-81788', 1, 10.00, 10.00, 'COMPLETED'),
    (1006, 3, 506, '2026-04-10 09:30:00', 'ELF-83331', 4, 8.00, 32.00, 'COMPLETED'),
    (1007, 4, 507, '2026-04-10 10:45:00', 'ELF-84201', 2, 10.00, 20.00, 'COMPLETED'),
    (1008, 5, 508, '2026-04-10 11:20:00', 'ELF-83025', 3, 8.00, 24.00, 'COMPLETED'),
    (1009, 1, 509, '2026-04-10 13:00:00', 'ELF-83428', 1, 10.00, 10.00, 'PENDING'),
    (1010, 2, 510, '2026-04-10 14:30:00', 'ELF-83729', 2, 14.00, 28.00, 'COMPLETED'),
]
cursor.executemany(
    "INSERT IGNORE INTO elf_orders VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)",
    orders
)

conn.commit()
cursor.close()
conn.close()
print("ELF MySQL data created successfully!")
print("Tables created: elf_stores, elf_inventory, elf_orders")