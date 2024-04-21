{{ config(materialized='view') }}

with customers as (

    select * from {{source('raw_data','Customer')}}

),

orders as (

    select * from {{ source('raw_data','Orders') }}

),

payments as (

    select * from {{ source('raw_data','Payments') }}

),

customer_orders as (

        select
        User_id,

        min(Order_date) as first_order,
        max(Order_date) as most_recent_order,
        count(id) as number_of_orders
    from orders

    group by User_id

),

customer_payments as (

    select
        orders.User_id,
        sum(Amount) as total_amount

    from payments

    left join orders on
         payments.Order_id = orders.id

    group by orders.User_id

),

final as (

    select
        customers.id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order,
        customer_orders.most_recent_order,
        customer_orders.number_of_orders,
        customer_payments.total_amount as customer_lifetime_value

    from customers

    left join customer_orders
        on customers.id = customer_orders.customer_id

    left join customer_payments
        on  customers.id = customer_payments.customer_id

)

select * from final