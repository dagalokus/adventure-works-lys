# Databricks notebook source
import re

def camel_to_snake(name: str) -> str:
    """
    Converts a camelCase or mixedCase string to snake case.
    Handles acronyms correctly (e.g., HTTPHeader -> http_header).

    Args:
        name (str): The input string.

    Returns:
        str: The converted string in snake case.
    """
    s0 = re.sub(r" ", r"_", name)
    # First, place underscore between lower-to-upper or digit-to-upper transitions
    s1 = re.sub(r"(?<=[a-z0-9])([A-Z])", r"_\1", s0)
    # Then handle transitions from acronym sequences like "HTTPHeader" -> "HTTP_Header"
    s2 = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", s1)
    # Convert to lowercase
    return s2.lower()

# COMMAND ----------

spark.sql(
    """
    create catalog if not exists adventure_works
    """
)

# COMMAND ----------

schema_dict = {
  "department": "human_resources",
  "employee": "human_resources",
  "employee_department_history": "human_resources",
  "employee_pay_history": "human_resources",
  "job_candidate": "human_resources",
  "shift": "human_resources",
  "address": "person",
  "address_type": "person",
  "business_entity": "person",
  "business_entity_address": "person",
  "business_entity_contact": "person",
  "contact_type": "person",
  "country_region": "person",
  "email_address": "person",
  "person": "person",
  "person_phone": "person",
  "phone_number_type": "person",
  "password": "person",
  "state_province": "person",
  "bill_of_materials": "production",
  "culture": "production",
  "document": "production",
  "illustration": "production",
  "location": "production",
  "product": "production",
  "product_category": "production",
  "product_cost_history": "production",
  "product_description": "production",
  "product_document": "production",
  "product_inventory": "production",
  "product_list_price_history": "production",
  "product_model": "production",
  "product_model_illustration": "production",
  "product_model_product_description_culture": "production",
  "product_photo": "production",
  "product_product_photo": "production",
  "product_review": "production",
  "product_subcategory": "production",
  "scrap_reason": "production",
  "transaction_history": "production",
  "transaction_history_archive": "production",
  "unit_measure": "production",
  "work_order": "production",
  "work_order_routing": "production",
  "product_vendor": "purchasing",
  "purchase_order_detail": "purchasing",
  "purchase_order_header": "purchasing",
  "ship_method": "purchasing",
  "vendor": "purchasing",
  "country_region_currency": "sales",
  "credit_card": "sales",
  "currency": "sales",
  "currency_rate": "sales",
  "customer": "sales",
  "person_credit_card": "sales",
  "sales_order_detail": "sales",
  "sales_order_header": "sales",
  "sales_order_header_sales_reason": "sales",
  "sales_person": "sales",
  "sales_person_quota_history": "sales",
  "sales_reason": "sales",
  "sales_tax_rate": "sales",
  "sales_territory": "sales",
  "sales_territory_history": "sales",
  "shopping_cart_item": "sales",
  "special_offer": "sales",
  "special_offer_product": "sales",
  "store": "sales"
}

# COMMAND ----------

for s in set(schema_dict.values()):
    spark.sql(
        f"""
        create schema if not exists adventure_works.{s}
        """
    )

# COMMAND ----------

jdbc_url = "jdbc:mysql://relational.fel.cvut.cz:3306/"
db_user = "guest"
db_password = "ctu-relational"
query = """
select table_schema, table_name
from information_schema.tables
where table_schema = 'AdventureWorks2014'
"""

source_tables = (
    spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("user", db_user)
    .option("password", db_password)
    .option("query", query)
    .load()
).collect()

# COMMAND ----------

for table in source_tables:
    spark.sql(
        f"""
        drop table if exists workspace.adventure_works.{table.table_name}
        """
    )

    columns = (
        spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("user", db_user)
        .option("password", db_password)
        .option(
            "query",
            f"""
            select column_name
            from information_schema.columns
            where table_schema = '{table.table_schema}'
            and table_name = '{table.table_name}'
            order by ordinal_position
            """
        )
        .load()
    ).collect()

    data = (
        spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("user", db_user)
        .option("password", db_password)
        .option("dbtable", f"{table.table_schema}.{table.table_name}")
        .load()
    )

    data = data.toDF(*[camel_to_snake(c.column_name) for c in columns])

    table_name = camel_to_snake(table.table_name)
    (
        data.write
        .mode("overwrite")
        .saveAsTable(f"adventure_works.{schema_dict.get(table_name, 'default')}.{table_name}")
    )
