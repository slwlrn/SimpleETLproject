from pandasql import sqldf
pysqldf = lambda q: sqldf(q, locals()) 

class Transform():
    
    def __init__(self) -> None:
        self.process = 'Transform Process'

        
    def enunciado1(self, customer, orders, items ):
        """Executes an SQL query to retrieve customer information, including their name, email,and aggregated quantities and totals from the provided customer, orders, and items dataframes.
        Parameters:
                customer (DataFrame): DataFrame representing the 'customer' table.
                orders (DataFrame): DataFrame representing the 'orders' table.
                items (DataFrame): DataFrame representing the 'items' table.
            Returns:
                DataFrame: Result of the SQL query execution containing customer information, aggregated quantities,and totals, sorted by the total in descending order. """
        
        q = """
                SELECT
                    customer_id, customer_fname, customer_lname, customer_email, sum(order_item_quantity) as quantity_item_total, sum(order_item_subtotal)as total
                FROM
                    customer as c
                INNER JOIN
                    orders as o
                    ON c.customer_id = o.order_customer_id
                INNER JOIN
                    items as oi
                    ON o.order_id = oi.order_item_order_id
                WHERE order_status <> 'CANCELED'
                GROUP BY customer_id, customer_fname, customer_lname, customer_email
                ORDER BY  total DESC
                LIMIT 20
            """
        result = sqldf(q)

        return result
        

    def enunciado2(self, df_order_items,df_products,df_categories):
        """Executes an SQL query to retrieve aggregated information from the provided order_items,products, and categories dataframes, including the category name, item quantity, and total.
    Parameters:
        df_order_items (DataFrame): DataFrame representing the 'order_items' table.
        df_products (DataFrame): DataFrame representing the 'products' table.
        df_categories (DataFrame): DataFrame representing the 'categories' table.
    Returns:
        DataFrame: Result of the SQL query execution containing aggregated information,including the category name, item quantity, and total."""
        
        q = """
                SELECT
                    ca.category_name, sum(order_item_quantity) as item_quantity, cast(sum(order_item_subtotal) AS INT )as total
                FROM df_order_items as oi
                INNER JOIN
                    df_products as p
                    ON oi.order_item_product_id = p.product_id
                INNER JOIN
                    df_categories as ca
                    ON p.product_category_id = ca.category_id
                GROUP BY ca.category_name
            """

        result = sqldf(q)

        return result