import sqlite3
from typing import Dict, Optional, List, Tuple

DB_FILE = "tri.db"

def get_conn(db_file: str = DB_FILE) -> sqlite3.Connection:
    """获取一个数据库连接。如果文件不存在，SQLite会自动创建"""
    conn = sqlite3.connect(db_file)
    return conn

# =====================基础：连接 & 建表======================== #
def init_db(db_file: str = DB_FILE) -> None:
    """创建三张表: reserve(储备库), offering(出让库), deal(成交库)
    如果表已经存在，就不会重复创建。
    """
    conn = get_conn(db_file)
    try:
        cur = conn.cursor()

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS reserve (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                region TEXT,
                mineral_type TEXT,
                area REAL,
                quantity TEXT,
                recommendations VARCHAR(4),
                coordinates TEXT,
                transfer_conditions VARCHAR(4),
                announcement_date DATE,
                annual_transfer_batch TEXT,
                proj_source TEXT,
                start_price REAL,
                transaction_date DATE,
                transaction_price REAL,
                payable_price REAL,
                success_bidder TEXT,
                contact_name TEXT,
                social_credit_code TEXT,
                contact_number VARCHAR(15),
                company_address TEXT,
                transfer_authority TEXT,
                contract_number TEXT,
                contract_signing_date DATE,
                payment_deadline DATE,
                actual_payment_date DATE
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS offering (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                region TEXT,
                mineral_type TEXT,
                area REAL,
                quantity TEXT,
                recommendations VARCHAR(4),
                coordinates TEXT,
                transfer_conditions VARCHAR(4),
                announcement_date DATE,
                annual_transfer_batch TEXT,
                proj_source TEXT,
                start_price REAL,
                transaction_date DATE,
                transaction_price REAL,
                payable_price REAL,
                success_bidder TEXT,
                contact_name TEXT,
                social_credit_code TEXT,
                contact_number VARCHAR(15),
                company_address TEXT,
                transfer_authority TEXT,
                contract_number TEXT,
                contract_signing_date DATE,
                payment_deadline DATE,
                actual_payment_date DATE
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS deal (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                region TEXT,
                mineral_type TEXT,
                area REAL,
                quantity TEXT,
                recommendations VARCHAR(4),
                coordinates TEXT,
                transfer_conditions VARCHAR(4),
                announcement_date DATE,
                annual_transfer_batch TEXT,
                proj_source TEXT,
                start_price REAL,
                transaction_date DATE,
                transaction_price REAL,
                payable_price REAL,
                success_bidder TEXT,
                contact_name TEXT,
                social_credit_code TEXT,
                contact_number VARCHAR(15),
                company_address TEXT,
                transfer_authority TEXT,
                contract_number TEXT,
                contract_signing_date DATE,
                payment_deadline DATE,
                actual_payment_date DATE
            )
            """
        )

        conn.commit()
    finally:
        conn.close()

# ====== 通用插入：全字段 ======
# 三张表的统一列顺序（与建表字段一致）
ALL_COLUMNS = [
    "id", "name", "region", "mineral_type", "area", "quantity",
    "recommendations", "coordinates", "transfer_conditions",
    "announcement_date", "annual_transfer_batch", "proj_source",
    "start_price", "transaction_date", "transaction_price",
    "payable_price", "success_bidder", "contact_name",
    "social_credit_code", "contact_number", "company_address",
    "transfer_authority", "contract_number", "contract_signing_date",
    "payment_deadline", "actual_payment_date"
]

def get_record(table: str, id_: int, cols: Optional[List[str]] = None) -> Dict[str, object]:
    """按 id 获取单条记录。默认返回全字段（ALL_COLUMNS）"""
    if table not in {"reserve", "offering", "deal"}:
        raise ValueError("table 必须是 reserve/offering/deal")

    cols = cols or ALL_COLUMNS
    col_sql = ", ".join(cols)

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT {col_sql} FROM {table} WHERE id = ?", (id_,))
        row = cur.fetchone()
        if row is None:
            raise ValueError(f"{table} 表中没有 id={id_} 的记录")
        return dict(zip(cols, row))
    finally:
        conn.close()

def add_full_record(table: str, data: dict) -> int:
    """向指定表插入一条“全字段”记录。
    - 要求：data 至少包含 id, name；其余可为空(None/空字符串)。
    - table ∈ {"reserve","offering","deal"}。
    - 返回插入的 id。
    """
    if table not in {"reserve", "offering", "deal"}:
        raise ValueError("table 必须是 reserve/offering/deal")

    # 只取允许的列，按 ALL_COLUMNS 顺序构造 values
    row_values = []
    for col in ALL_COLUMNS:
        row_values.append(data.get(col, None))

    conn = get_conn()
    try:
        placeholders = ",".join(["?"] * len(ALL_COLUMNS))
        cols_sql = ", ".join(ALL_COLUMNS)
        sql = f"INSERT INTO {table} ({cols_sql}) VALUES ({placeholders})"
        cur = conn.cursor()
        cur.execute(sql, row_values)
        conn.commit()
        return int(data["id"])
    finally:
        conn.close()

# ===================== 新增 ======================== #
ALLOWED_TABLES = {"reserve", "offering", "deal"}

def add_record(table: str, id_: int, name: str, quantity: str) -> int:
    """向任意一个库插入记录（只含基础三列：id, name, quantity）
    要求：id 唯一且由外部指定；table 必须是 reserve/offering/deal
    """
    if table not in ALLOWED_TABLES:
        raise ValueError("table 必须是 reserve / offering / deal")

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            f"INSERT INTO {table} (id, name, quantity) VALUES (?, ?, ?)",
            (id_, name, quantity)
        )
        conn.commit()
        return id_
    except sqlite3.IntegrityError as e:
        # 常见：UNIQUE constraint failed（id 重复）
        raise
    finally:
        conn.close()

def update_full_record(table: str, id_: int, data: dict) -> int:
    """按 id 更新一条记录（全字段）。data 的键为列名；id 不允许修改。
    返回受影响行数（1 表示成功更新，0 表示未找到该 id）。
    """
    if table not in {"reserve", "offering", "deal"}:
        raise ValueError("table 必须是 reserve/offering/deal")

    # 构造 SET 子句（排除 id）
    cols_to_update = [c for c in ALL_COLUMNS if c != "id"]
    set_parts = []
    values = []
    for col in cols_to_update:
        set_parts.append(f"{col} = ?")
        values.append(data.get(col, None))  # 空串在外层已转 None

    values.append(id_)  # WHERE id = ?
    sql = f"UPDATE {table} SET {', '.join(set_parts)} WHERE id = ?"

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(sql, values)
        conn.commit()
        return cur.rowcount
    finally:
        conn.close()

def delete_record(table: str, id_: int) -> int:
    """按 id 删除一条记录。返回受影响行数（1=成功删除，0=未找到）。"""
    if table not in {"reserve", "offering", "deal"}:
        raise ValueError("table 必须是 reserve/offering/deal")
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(f"DELETE FROM {table} WHERE id = ?", (id_,))
        conn.commit()
        return cur.rowcount
    finally:
        conn.close()


def list_table(table: str, cols: List[str] | None = None) -> List[Tuple]:
    """列出指定表的所有记录。
    默认只返回 id,name,quantity 三列，避免和三列表头不匹配。
    你也可以传入 cols=["id","name",...] 自定义列。
    """
    if table not in {"reserve", "offering", "deal"}:
        raise ValueError("必须是 reserve / offering / deal 三表之一")

    if cols is None:
        cols = ["id", "name", "region", "mineral_type", "area", "quantity", "recommendations", "coordinates", "transfer_conditions", "announcement_date", "annual_transfer_batch", "proj_source", "start_price", "transaction_date", "transaction_price", "payable_price", "success_bidder", "contact_name", "social_credit_code", "contact_number", "company_address", "transfer_authority", "contract_number", "contract_signing_date", "payment_deadline", "actual_payment_date"]

    conn = get_conn()
    try:
        cur = conn.cursor()
        col_sql = ", ".join(cols)
        cur.execute(f"SELECT {col_sql} FROM {table} ORDER BY id")
        return cur.fetchall()
    finally:
        conn.close()

# =====================转移操作========================= #
def move_reserve_to_offering(reserve_id: int) -> int:
    """把储备库的一条记录转移到出让库"""
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute(
            "SELECT name, region, mineral_type, area, quantity, recommendations, coordinates, transfer_conditions, announcement_date, annual_transfer_batch, proj_source, start_price, transaction_date, transaction_price, payable_price, success_bidder, contact_name, social_credit_code, contact_number, company_address, transfer_authority, contract_number, contract_signing_date, payment_deadline, actual_payment_date FROM reserve WHERE id = ?",
            (reserve_id,),
        )
        row = cur.fetchone()
        if row is None:
            conn.rollback()
            raise ValueError(f"reserve 表中没有 id={reserve_id} 的记录")

        cols = (
            "name, region, mineral_type, area, quantity, recommendations, coordinates, transfer_conditions, "
            "announcement_date, annual_transfer_batch, proj_source, start_price, transaction_date, transaction_price, "
            "payable_price, success_bidder, contact_name, social_credit_code, contact_number, company_address, "
            "transfer_authority, contract_number, contract_signing_date, payment_deadline, actual_payment_date"
        )
        placeholders = ",".join(["?"] * len(row))

        cur.execute(f"INSERT INTO offering ({cols}) VALUES ({placeholders})", row)
        new_offering_id = cur.lastrowid

        cur.execute("DELETE FROM reserve WHERE id = ?", (reserve_id,))
        conn.commit()
        print(
            f"成功将 reserve.id={reserve_id} 的记录转移到 offering.id={new_offering_id}"
        )
        return new_offering_id
    except Exception:
        conn.rollback()
        print(f"将 reserve.id={reserve_id} 转移到 offering 失败，已回滚")
        raise
    finally:
        conn.close()

def move_offering_to_deal(offering_id: int) -> int:
    """把出让库的一条记录转移到成交库"""
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute(
            "SELECT name, region, mineral_type, area, quantity, recommendations, coordinates, transfer_conditions, announcement_date, annual_transfer_batch, proj_source, start_price, transaction_date, transaction_price, payable_price, success_bidder, contact_name, social_credit_code, contact_number, company_address, transfer_authority, contract_number, contract_signing_date, payment_deadline, actual_payment_date FROM offering WHERE id = ?",
            (offering_id,),
        )
        row = cur.fetchone()
        if row is None:
            conn.rollback()
            raise ValueError(f"offering 表中没有 id={offering_id} 的记录")

        cols = (
            "name, region, mineral_type, area, quantity, recommendations, coordinates, transfer_conditions, "
            "announcement_date, annual_transfer_batch, proj_source, start_price, transaction_date, transaction_price, "
            "payable_price, success_bidder, contact_name, social_credit_code, contact_number, company_address, "
            "transfer_authority, contract_number, contract_signing_date, payment_deadline, actual_payment_date"
        )
        placeholders = ",".join(["?"] * len(row))

        cur.execute(f"INSERT INTO deal ({cols}) VALUES ({placeholders})", row)
        new_deal_id = cur.lastrowid

        cur.execute("DELETE FROM offering WHERE id = ?", (offering_id,))
        conn.commit()
        print(
            f"成功将 offering.id={offering_id} 的记录转移到 deal.id={new_deal_id}"
        )
        return new_deal_id
    except Exception:
        conn.rollback()
        print(f"将 offering.id={offering_id} 转移到 deal 失败，已回滚")
        raise
    finally:
        conn.close()
