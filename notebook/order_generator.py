# 这是基于customer.csv、product.csv的真实数据生成模拟订单的脚本;
# 如果您想复现生成过程，请手动修改 INPUT_DIR、OUTPUT_DIR；

import pandas as pd
import random
import os
from datetime import datetime, timedelta

# ========== 配置 ==========
INPUT_DIR = r"D:\Project\2604_SPARK\data\raw" #数据输入目录
OUTPUT_DIR = r"D:\Project\2604_SPARK\data\temp_orders" #数据输出目录

OUTPUT_ENCODING = 'utf-8' #统一输出代码

TOTAL_ROWS = 10_000_000          # 1000万行
ROWS_PER_FILE = 100_000          # 每个文件10万行 → 共100个文件

START_DATE = datetime(2014, 1, 1)
END_DATE = datetime(2024, 12, 31)

# 分区爆炸日期列表（暂时基于节日）
EXPLOSION_DATES = [
    datetime(2020, 1, 1),   # 元旦
    datetime(2020, 10, 1),  # 国庆
    datetime(2020, 11, 11), # 双11
    datetime(2020, 12, 25), # 圣诞
    datetime(2021, 1, 1),   # 元旦次年
]
EXPLOSION_FACTOR = 50  # 这些日期的订单量是普通日期的50倍

# 数据倾斜：热门客户列表（前10个）
HOT_CUSTOMERS = [f"{13021+i}BA" for i in range(10)]  # 13021BA~13030BA
HOT_CUSTOMER_RATIO = 0.3   # 这些客户合计占30%

# 热门产品列表
HOT_PRODUCTS = [214, 310, 320, 330, 529]
HOT_PRODUCT_RATIO = 0.5    # 这些产品合计占50%

RANDOM_SEED = 42
random.seed(RANDOM_SEED)

# ========== 加载维度 ==========
def load_dimensions():
    customer_df = pd.read_csv(os.path.join(INPUT_DIR, "customer.csv"), encoding='gbk')
    product_df = pd.read_csv(os.path.join(INPUT_DIR, "product.csv"), encoding='utf-8')
    all_customers = customer_df['customer_id'].tolist()
    all_products = product_df['product_id'].tolist()
    price_map = dict(zip(product_df['product_id'], product_df['product_price']))
    return all_customers, all_products, price_map

# ========== 生成器 ==========
def generate_orders(all_customers, all_products, price_map, total_rows):
    # 非热门客户列表
    normal_customers = [c for c in all_customers if c not in HOT_CUSTOMERS]
    normal_products = [p for p in all_products if p not in HOT_PRODUCTS]
    
    # 日期范围参数
    delta_days = (END_DATE - START_DATE).days + 1
    # 构建爆炸日期的权重：每个爆炸日期单独计算概率
    # 为简化：每个爆炸日期出现的概率是普通日期的 EXPLOSION_FACTOR 倍
    # 总天数权重 = (delta_days - len(EXPLOSION_DATES)) * 1 + len(EXPLOSION_DATES) * EXPLOSION_FACTOR
    total_weight = (delta_days - len(EXPLOSION_DATES)) + len(EXPLOSION_DATES) * EXPLOSION_FACTOR
    explosion_prob_per_day = EXPLOSION_FACTOR / total_weight
    normal_prob_per_day = 1 / total_weight
    
    order_id = 1
    while order_id <= total_rows:
        # 客户选择：热门组 or 普通
        if random.random() < HOT_CUSTOMER_RATIO:
            customer = random.choice(HOT_CUSTOMERS)
        else:
            customer = random.choice(normal_customers) if normal_customers else all_customers[0]
        
        # 产品选择
        if random.random() < HOT_PRODUCT_RATIO:
            product = random.choice(HOT_PRODUCTS)
        else:
            product = random.choice(normal_products) if normal_products else all_products[0]
        
        amount = price_map.get(product, 50)
        
        # 日期选择（带爆炸）
        r = random.random()
        cum_prob = 0
        chosen_date = None
        # 先检查爆炸日期
        for d in EXPLOSION_DATES:
            cum_prob += explosion_prob_per_day
            if r < cum_prob:
                chosen_date = d
                break
        if chosen_date is None:
            # 普通日期
            day_offset = random.randint(0, delta_days - 1)
            candidate = START_DATE + timedelta(days=day_offset)
            # 避免选到爆炸日期（简单重试，概率极低）
            while candidate in EXPLOSION_DATES:
                day_offset = random.randint(0, delta_days - 1)
                candidate = START_DATE + timedelta(days=day_offset)
            chosen_date = candidate
        
        date_str = chosen_date.strftime("%Y-%m-%d")
        yield (order_id, customer, product, date_str, amount)
        order_id += 1

# ========== 批量写入 ==========
def write_batch(rows_batch, file_idx):
    out_path = os.path.join(OUTPUT_DIR, f"orders_{file_idx:03d}.csv")
    df = pd.DataFrame(rows_batch, columns=['order_id','customer_id','product_id','order_date','amount'])
    df.to_csv(out_path, index=False, sep=',', encoding=OUTPUT_ENCODING, quoting=0)
    print(f"生成 {out_path}")

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    all_customers, all_products, price_map = load_dimensions()
    print(f"客户数:{len(all_customers)} 产品数:{len(all_products)}")
    print(f"开始生成 {TOTAL_ROWS:,} 行订单，每 {ROWS_PER_FILE:,} 行一个文件...")
    
    batch = []
    file_cnt = 1
    written = 0
    gen = generate_orders(all_customers, all_products, price_map, TOTAL_ROWS)
    
    for row in gen:
        batch.append(row)
        written += 1
        if len(batch) >= ROWS_PER_FILE:
            write_batch(batch, file_cnt)
            file_cnt += 1
            batch = []
            if written % 1_000_000 == 0:
                print(f"进度: {written:,} / {TOTAL_ROWS:,}")
    if batch:
        write_batch(batch, file_cnt)
    
    print(f"完成！共 {written:,} 行，{file_cnt} 个文件。")

if __name__ == "__main__":
    main()