import pandas as pd
import sqlite3
import logging
from datetime import datetime
 
# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
 
# Database configuration
SOURCE_DB = "source_database.db"
TARGET_DB = "data_warehouse.db"
 
# ─────────────────────────────────────────────
# EXTRACT
# ─────────────────────────────────────────────
 
def extract_customers(conn):
    """
    Extract all active customers from the source database.
 
    Args:
        conn: SQLite database connection to source DB
 
    Returns:
        pd.DataFrame: Raw customer records with columns:
                      customer_id, name, email, region, created_at
    """
    logger.info("Extracting customers from source DB...")
    query = """
        SELECT customer_id, name, email, region, created_at
        FROM customers
        WHERE is_active = 1
    """
    df = pd.read_sql_query(query, conn)
    logger.info(f"Extracted {len(df)} customer records.")
    return df
 
 
def extract_orders(conn):
    """
    Extract all orders placed in the current year from source database.
 
    Args:
        conn: SQLite database connection to source DB
 
    Returns:
        pd.DataFrame: Raw order records with columns:
                      order_id, customer_id, product_id, quantity, unit_price, order_date, status
    """
    logger.info("Extracting orders from source DB...")
    current_year = datetime.now().year
    query = f"""
        SELECT order_id, customer_id, product_id, quantity, unit_price, order_date, status
        FROM orders
        WHERE strftime('%Y', order_date) = '{current_year}'
    """
    df = pd.read_sql_query(query, conn)
    logger.info(f"Extracted {len(df)} order records.")
    return df
 
 
def extract_products(conn):
    """
    Extract product catalog from source database.
 
    Args:
        conn: SQLite database connection to source DB
 
    Returns:
        pd.DataFrame: Product records with columns:
                      product_id, product_name, category, cost_price
    """
    logger.info("Extracting products from source DB...")
    query = """
        SELECT product_id, product_name, category, cost_price
        FROM products
    """
    df = pd.read_sql_query(query, conn)
    logger.info(f"Extracted {len(df)} product records.")
    return df
 
 
# ─────────────────────────────────────────────
# TRANSFORM
# ─────────────────────────────────────────────
 
def clean_customers(df):
    """
    Clean and standardize customer data.
    - Remove duplicates
    - Normalize email to lowercase
    - Strip whitespace from name and region
 
    Args:
        df (pd.DataFrame): Raw customer DataFrame
 
    Returns:
        pd.DataFrame: Cleaned customer DataFrame
    """
    logger.info("Cleaning customer data...")
    df = df.drop_duplicates(subset=['customer_id'])
    df['email'] = df['email'].str.lower().str.strip()
    df['name'] = df['name'].str.strip()
    df['region'] = df['region'].str.upper().str.strip()
    df['created_at'] = pd.to_datetime(df['created_at'])
    return df
 
 
def calculate_order_totals(orders_df, products_df):
    """
    Calculate total revenue and profit for each order by joining with product data.
    Adds columns: total_revenue, total_cost, profit, profit_margin
 
    Args:
        orders_df (pd.DataFrame): Cleaned orders DataFrame
        products_df (pd.DataFrame): Products DataFrame
 
    Returns:
        pd.DataFrame: Enriched orders DataFrame with financial metrics
    """
    logger.info("Calculating order totals and profit margins...")
    df = orders_df.merge(products_df[['product_id', 'cost_price', 'category']], on='product_id', how='left')
    df['total_revenue'] = df['quantity'] * df['unit_price']
    df['total_cost'] = df['quantity'] * df['cost_price']
    df['profit'] = df['total_revenue'] - df['total_cost']
    df['profit_margin'] = ((df['profit'] / df['total_revenue']) * 100).round(2)
    return df
 
 
def aggregate_customer_summary(customers_df, orders_df):
    """
    Aggregate order data per customer to build a customer summary table.
    Includes: total orders, total revenue, average order value, last order date
 
    Args:
        customers_df (pd.DataFrame): Cleaned customers DataFrame
        orders_df (pd.DataFrame): Enriched orders DataFrame
 
    Returns:
        pd.DataFrame: Customer-level summary with KPIs
    """
    logger.info("Aggregating customer summary...")
    summary = orders_df.groupby('customer_id').agg(
        total_orders=('order_id', 'count'),
        total_revenue=('total_revenue', 'sum'),
        total_profit=('profit', 'sum'),
        avg_order_value=('total_revenue', 'mean'),
        last_order_date=('order_date', 'max')
    ).reset_index()
 
    summary['avg_order_value'] = summary['avg_order_value'].round(2)
    summary['total_revenue'] = summary['total_revenue'].round(2)
    summary['total_profit'] = summary['total_profit'].round(2)
 
    # Join with customer info
    result = customers_df.merge(summary, on='customer_id', how='left')
    result['total_orders'] = result['total_orders'].fillna(0).astype(int)
    result['total_revenue'] = result['total_revenue'].fillna(0.0)
    return result
 
 
def flag_high_value_customers(df, threshold=5000):
    """
    Flag customers as high-value if their total revenue exceeds threshold.
    Adds column: customer_segment ('High Value' or 'Standard')
 
    Args:
        df (pd.DataFrame): Customer summary DataFrame
        threshold (float): Revenue threshold for high-value classification
 
    Returns:
        pd.DataFrame: DataFrame with customer_segment column added
    """
    logger.info(f"Flagging high-value customers (threshold={threshold})...")
    df['customer_segment'] = df['total_revenue'].apply(
        lambda x: 'High Value' if x >= threshold else 'Standard'
    )
    return df
 
 
# ─────────────────────────────────────────────
# LOAD
# ─────────────────────────────────────────────
 
def load_customer_summary(df, conn):
    """
    Load transformed customer summary into the data warehouse.
    Replaces existing data on each run (full refresh).
 
    Args:
        df (pd.DataFrame): Final customer summary DataFrame
        conn: SQLite connection to target data warehouse
 
    Returns:
        None
    """
    logger.info("Loading customer summary into data warehouse...")
    df['etl_loaded_at'] = datetime.now().isoformat()
    df.to_sql('customer_summary', conn, if_exists='replace', index=False)
    logger.info(f"Loaded {len(df)} records into customer_summary table.")
 
 
def load_order_details(df, conn):
    """
    Load enriched order details into the data warehouse.
    Replaces existing data on each run (full refresh).
 
    Args:
        df (pd.DataFrame): Enriched orders DataFrame
        conn: SQLite connection to target data warehouse
 
    Returns:
        None
    """
    logger.info("Loading order details into data warehouse...")
    df['etl_loaded_at'] = datetime.now().isoformat()
    df.to_sql('order_details', conn, if_exists='replace', index=False)
    logger.info(f"Loaded {len(df)} records into order_details table.")
 
 
# ─────────────────────────────────────────────
# MAIN PIPELINE
# ─────────────────────────────────────────────
 
def run_etl_pipeline():
    """
    Main ETL pipeline orchestrator.
    Connects to source and target databases, runs all
    extract → transform → load steps in sequence.
 
    Returns:
        None
    """
    logger.info("=" * 50)
    logger.info("Starting ETL Pipeline: Customer Orders Processing")
    logger.info("=" * 50)
 
    try:
        # Connect to source and target
        source_conn = sqlite3.connect(SOURCE_DB)
        target_conn = sqlite3.connect(TARGET_DB)
 
        # EXTRACT
        customers_raw = extract_customers(source_conn)
        orders_raw = extract_orders(source_conn)
        products_raw = extract_products(source_conn)
 
        # TRANSFORM
        customers_clean = clean_customers(customers_raw)
        orders_enriched = calculate_order_totals(orders_raw, products_raw)
        customer_summary = aggregate_customer_summary(customers_clean, orders_enriched)
        customer_summary = flag_high_value_customers(customer_summary, threshold=5000)
 
        # LOAD
        load_customer_summary(customer_summary, target_conn)
        load_order_details(orders_enriched, target_conn)
 
        logger.info("ETL Pipeline completed successfully.")
 
    except Exception as e:
        logger.error(f"ETL Pipeline failed: {e}")
        raise
 
    finally:
        source_conn.close()
        target_conn.close()
        logger.info("Database connections closed.")
 
 
if __name__ == "__main__":
    run_etl_pipeline()
 