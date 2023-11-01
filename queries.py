# pylint:disable=C0111,C0103
import sqlite3

conn = sqlite3.connect('data/ecommerce.sqlite')
db = conn.cursor()


def get_average_purchase(db):
    # return the average amount spent per order for each customer ordered by customer ID
    query = '''
    WITH customer_cost AS (
    SELECT orders.OrderID, orders.CustomerID as custid,
        orderdetails.UnitPrice * orderdetails.Quantity as tot_amount,
        ROUND(SUM(orderdetails.UnitPrice * orderdetails.Quantity),2) as tot
        FROM OrderDetails
        join Orders on orderdetails.OrderID = orders.OrderID
        GROUP BY  orders.OrderID
    )
    SELECT DISTINCT
    customer_cost.custid,
    AVG(customer_cost.tot) OVER (
            PARTITION BY customer_cost.custid
            ORDER BY customer_cost.custid
        ) AS cumulative_amount
    from customer_cost

    '''
    db.execute(query)
    results = db.fetchall()
    return results

def get_general_avg_order(db):
    # return the average amount spent per order
    query = '''
    WITH order_cost AS (
        SELECT orderid, SUM(unitprice * quantity) as tot_order
        from OrderDetails
        group by orderdetails.OrderID
    )
    SELECT
        ROUND(AVG(order_cost.tot_order))
        FROM order_cost
    '''
    db.execute(query)
    results = db.fetchall()
    return results[0][0]


def best_customers(db):
    # return the customers who have an average purchase greater than the general average purchase
    query = '''
    WITH customer_cost AS (
    SELECT orders.OrderID, orders.CustomerID as custid,
        orderdetails.UnitPrice * orderdetails.Quantity as tot_amount,
        SUM(orderdetails.UnitPrice * orderdetails.Quantity) as tot
        FROM OrderDetails
        join Orders on orderdetails.OrderID = orders.OrderID
        GROUP BY  orders.OrderID
    ),
    cum AS (
    SELECT DISTINCT
        customer_cost.custid as iden,
        AVG(customer_cost.tot) OVER (
                PARTITION BY customer_cost.custid
                ORDER BY customer_cost.custid
            ) AS cumulative_amount
        from customer_cost
        )
    select cum.iden, ROUND(cum.cumulative_amount, 2) from cum
        WHERE cum.cumulative_amount > ((
        WITH order_cost AS (
        SELECT orderid, SUM(unitprice * quantity) as tot_order
        from OrderDetails
        group by orderdetails.OrderID
    )
    SELECT
        ROUND(AVG(order_cost.tot_order))
        FROM order_cost
	))
order by cum.cumulative_amount DESC
    '''
    db.execute(query)
    results = db.fetchall()
    return results


def top_ordered_product_per_customer(db):
    # return the list of the top ordered product by each customer
    # based on the total ordered amount in USD
    pass  # YOUR CODE HERE

def average_number_of_days_between_orders(db):
    # return the average number of days between two consecutive orders of the same customer
    pass  # YOUR CODE HERE



#print(get_average_purchase(db))
#print(get_general_avg_order(db))
print(best_customers(db))
