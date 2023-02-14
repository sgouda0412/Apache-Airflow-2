import csv
import logging
from datetime import datetime, timedelta

import pendulum
import boto3, json
# from include.airflow.defaults import AIRFLOW_ENVIRONMENT
from airflow.decorators import dag
from airflow.exceptions import AirflowSkipException
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# from include.airflow.defaults import AIRFLOW_ENVIRONMENT, S3_BUCKET

from airflow.models import Variable

AIRFLOW_ENVIRONMENT = Variable.get("airflow_environment")
S3_BUCKET = Variable.get("s3_bucket")

local_tz = pendulum.timezone("Asia/Kolkata")

# from include.config.al_pg_to_rs import

def dag_failure_alert(context):
    json_details = {
        "name" : "Dag failed"
    }
    client = boto3.client('events', region_name = "us-east-1")
    response = client.put_events(
        Entries = [
            {
                'Source' : 'airflow',
                'DetailType' : 'user-preferences',
                'Detail' : json.dumps(json_details),
                'EventBusName' : 'american_logistics_poc'
            }
        ]
    )

default_args = {
    'owner': 'jennifer',
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

tables_and_queries = [
    {
        'source_table_name': 'companies',
        'staged_table_name': 'dim_companies_staged',
        'source_query': {
            "first_run": "select * from companies where created_at <= '{}'",
            "subsequent_run": "select * from companies where created_at <= '{}' and created_at > '{}'"
        },
        'delete_target_table': """delete from dim_companies where company_id in (select company_id from  dim_companies_staged)""",
        'insert_target_table': """insert into dim_companies(company_id, name, address, website, created_at, updated_at) (select company_id, name, address, website, created_at, updated_at from dim_companies_staged)""",
        'truncate_stage_table': """truncate dim_companies_staged"""
    },
    {
        'source_table_name': 'customers',
        'staged_table_name': 'dim_customers_staged',
        'source_query': {
            "first_run": "select * from customers where created_at <= '{}'",
            "subsequent_run": "select * from customers where created_at <= '{}' and created_at > '{}'"
        },
        'delete_target_table': """delete from dim_customers where customer_id in (select customer_id from  dim_customers_staged)""",
        'insert_target_table': """insert into dim_customers(customer_id, first_name, last_name, email, address, created_at, updated_at) (select customer_id, first_name, last_name, email, address, created_at, updated_at from dim_customers_staged)""",
        'truncate_stage_table': """truncate dim_customers_staged"""
    },
    {
        'source_table_name': 'deliveries',
        'staged_table_name': 'dim_deliveries_staged',
        'source_query': {
            "first_run": "select * from deliveries where created_at <= '{}'",
            "subsequent_run": "select * from deliveries where created_at <= '{}' and created_at > '{}'"
        },
        'delete_target_table': """delete from dim_deliveries where delivery_id in (select delivery_id from  dim_deliveries_staged)""",
        'insert_target_table': """insert into dim_deliveries(delivery_id, order_id, warehouse_id, delivery_date, created_at, updated_at) (select delivery_id, order_id, warehouse_id, delivery_date, created_at, updated_at from dim_deliveries_staged)""",
        'truncate_stage_table': """truncate dim_deliveries_staged"""
    },
    {
        'source_table_name': 'departments',
        'staged_table_name': 'dim_departments_staged',
        'source_query': {
            "first_run": "select * from departments where created_at <= '{}'",
            "subsequent_run": "select * from departments where created_at <= '{}' and created_at > '{}'"
        },
        'delete_target_table': """delete from dim_departments where department_id in (select department_id from  dim_departments_staged)""",
        'insert_target_table': """insert into dim_departments(department_id, name, company_id, created_at, updated_at) (select department_id, name, company_id, created_at, updated_at from dim_departments_staged)""",
        'truncate_stage_table': """truncate dim_departments_staged"""
    },
    {
        'source_table_name': 'employees',
        'staged_table_name': 'dim_employees_staged',
        'source_query': {
            "first_run": "select * from employees where created_at <= '{}'",
            "subsequent_run": "select * from employees where created_at <= '{}' and created_at > '{}'"
        },
        'delete_target_table': """delete from dim_employees where employee_id in (select employee_id from  dim_employees_staged)""",
        'insert_target_table': """insert into dim_employees(employee_id, first_name, last_name, email, department_id, created_at, updated_at) (select employee_id, first_name, last_name, email, department_id, created_at, updated_at from dim_employees_staged)""",
        'truncate_stage_table': """truncate dim_employees_staged"""
    },
    {
        'source_table_name': 'invoices',
        'staged_table_name': 'dim_invoice_staged',
        'source_query': {
            "first_run": "select * from invoices where created_at <= '{}'",
            "subsequent_run": "select * from invoices where created_at <= '{}' and created_at > '{}'"
        },
        'delete_target_table': """delete from dim_invoice where invoice_id in (select invoice_id from  dim_invoice_staged)""",
        'insert_target_table': """insert into dim_invoice(invoice_id, delivery_id, customer_id, invoice_date, created_at, updated_at) (select invoice_id, delivery_id, customer_id, invoice_date, created_at, updated_at from dim_invoice_staged)""",
        'truncate_stage_table': """truncate dim_invoice_staged"""
    },
    {
        'source_table_name': 'orderdetails',
        'staged_table_name': 'dim_orderdetails_staged',
        'source_query': {
            "first_run": "select * from orderdetails where created_at <= '{}'",
            "subsequent_run": "select * from orderdetails where created_at <= '{}' and created_at > '{}'"
        },
        'delete_target_table': """delete from dim_orderdetails where order_detail_id in (select order_detail_id from  dim_orderdetails_staged)""",
        'insert_target_table': """insert into dim_orderdetails(order_detail_id, order_id, product_id, quantity, created_at, updated_at) (select order_detail_id, order_id, product_id, quantity, created_at, updated_at from dim_orderdetails_staged)""",
        'truncate_stage_table': """truncate dim_orderdetails_staged"""
    },
    {
        'source_table_name': 'orders',
        'staged_table_name': 'dim_orders_staged',
        'source_query': {
            "first_run": "select * from orders where created_at <= '{}'",
            "subsequent_run": "select * from orders where created_at <= '{}' and created_at > '{}'"
        },
        'delete_target_table': """delete from dim_orders where order_id in (select order_id from  dim_orders_staged)""",
        'insert_target_table': """insert into dim_orders(order_id, employee_id, supplier_id, order_date, created_at, updated_at) (select order_id, employee_id, supplier_id, order_date, created_at, updated_at from dim_orders_staged)""",
        'truncate_stage_table': """truncate dim_orders_staged"""
    },
    {
        'source_table_name': 'paymentmethods',
        'staged_table_name': 'dim_paymentmethods_staged',
        'source_query': {
            "first_run": "select * from paymentmethods where created_at <= '{}'",
            "subsequent_run": "select * from paymentmethods where created_at <= '{}' and created_at > '{}'"
        },
        'delete_target_table': """delete from dim_paymentmethods where payment_method_id in (select payment_method_id from  dim_paymentmethods_staged)""",
        'insert_target_table': """insert into dim_paymentmethods(payment_method_id, name, description, created_at, updated_at) (select payment_method_id, name, description, created_at, updated_at from dim_paymentmethods_staged)""",
        'truncate_stage_table': """truncate dim_paymentmethods_staged"""
    },
    {
        'source_table_name': 'payments',
        'staged_table_name': 'dim_payments_staged',
        'source_query': {
            "first_run": "select * from payments where created_at <= '{}'",
            "subsequent_run": "select * from payments where created_at <= '{}' and created_at > '{}'"
        },
        'delete_target_table': """delete from dim_payments where payment_id in (select payment_id from  dim_payments_staged)""",
        'insert_target_table': """insert into dim_payments(payment_id, invoice_id, payment_method_id, payment_amount, payment_date, created_at, updated_at) (select payment_id, invoice_id, payment_method_id, payment_amount, payment_date, created_at, updated_at from dim_payments_staged)""",
        'truncate_stage_table': """truncate dim_payments_staged"""
    },
    {
        'source_table_name': 'products',
        'staged_table_name': 'dim_products_staged',
        'source_query': {
            "first_run": "select * from products where created_at <= '{}'",
            "subsequent_run": "select * from products where created_at <= '{}' and created_at > '{}'"
        },
        'delete_target_table': """delete from dim_products where product_id in (select product_id from  dim_products_staged)""",
        'insert_target_table': """insert into dim_products(product_id, name, description, price, created_at, updated_at) (select product_id, name, description, price, created_at, updated_at from dim_products_staged)""",
        'truncate_stage_table': """truncate dim_products_staged"""
    },
    {
        'source_table_name': 'refunds',
        'staged_table_name': 'dim_refunds_staged',
        'source_query': {
            "first_run": "select * from refunds where created_at <= '{}'",
            "subsequent_run": "select * from refunds where created_at <= '{}' and created_at > '{}'"
        },
        'delete_target_table': """delete from dim_refunds where refund_id in (select refund_id from  dim_refunds_staged)""",
        'insert_target_table': """insert into dim_refunds(refund_id, payment_id, refund_amount, refund_date, created_at, updated_at) (select refund_id, payment_id, refund_amount, refund_date, created_at, updated_at from dim_refunds_staged)""",
        'truncate_stage_table': """truncate dim_refunds_staged"""
    },
    {
        'source_table_name': 'suppliers',
        'staged_table_name': 'dim_suppliers_staged',
        'source_query': {
            "first_run": "select * from suppliers where created_at <= '{}'",
            "subsequent_run": "select * from suppliers where created_at <= '{}' and created_at > '{}'"
        },
        'delete_target_table': """delete from dim_suppliers where supplier_id in (select supplier_id from  dim_suppliers_staged)""",
        'insert_target_table': """insert into dim_suppliers(supplier_id, name, address, website, created_at, updated_at) (select supplier_id, name, address, website, created_at, updated_at from dim_suppliers_staged)""",
        'truncate_stage_table': """truncate dim_suppliers_staged"""
    },
    {
        'source_table_name': 'warehouses',
        'staged_table_name': 'dim_warehouses_staged',
        'source_query': {
            "first_run": "select * from warehouses where created_at <= '{}'",
            "subsequent_run": "select * from warehouses where created_at <= '{}' and created_at > '{}'"
        },
        'delete_target_table': """delete from dim_warehouses where warehouse_id in (select warehouse_id from  dim_warehouses_staged)""",
        'insert_target_table': """insert into dim_warehouses(warehouse_id, name, address, company_id, created_at, updated_at) (select warehouse_id, name, address, company_id, created_at, updated_at from dim_warehouses_staged)""",
        'truncate_stage_table': """truncate dim_warehouses_staged"""
    },
    {
        'source_table_name': 'fact_orders',
        'staged_table_name': 'fact_orders_staged',
        'source_query': {
            "first_run": """select o.order_id, o2.order_detail_id , o.employee_id , e.department_id , o.supplier_id , d.company_id , 2.product_id , d2.delivery_id, o2.quantity, d2.warehouse_id, o.order_date  ,  o.created_at , o.updated_at  from orders o
                        join orderdetails o2 on o.order_id = o2.order_id
                        join employees e on o.employee_id = e.employee_id
                        join departments d on e.department_id = d.department_id
                        join deliveries d2 on d2.order_id = o.order_id
                        join warehouses w on d2.warehouse_id = w.warehouse_id
                        join companies c on d.company_id = c.company_id where o.created_at <= '{}'""",
            "subsequent_run": """select o.order_id, o2.order_detail_id , o.employee_id , e.department_id , o.supplier_id , d.company_id , 2.product_id , d2.delivery_id, o2.quantity, d2.warehouse_id, o.order_date  ,  o.created_at , o.updated_at  from orders o
                        join orderdetails o2 on o.order_id = o2.order_id
                        join employees e on o.employee_id = e.employee_id
                        join departments d on e.department_id = d.department_id
                        join deliveries d2 on d2.order_id = o.order_id
                        join warehouses w on d2.warehouse_id = w.warehouse_id
                        join companies c on d.company_id = c.company_id where o.created_at <= '{}' and o.created_at > '{}'"""

        },
        'delete_target_table': """delete from fact_orders where order_id in (select order_id from  fact_orders_staged)""",
        'insert_target_table': """insert into fact_orders(order_id, order_detail_id, employee_id , department_id, supplier_id ,company_id, product_id , delivery_id , quantity , warehouse_id , order_date , created_at, updated_at)(select order_id, order_detail_id, employee_id , department_id, supplier_id ,company_id, product_id , delivery_id , quantity , warehouse_id , order_date , created_at, updated_at from fact_orders_staged)""",
        'truncate_stage_table': """truncate fact_orders_staged"""
    },
    {
        'source_table_name': 'fact_invoices',
        'staged_table_name': 'fact_invoices_staged',
        'source_query': {
            "first_run": """select i.invoice_id , i.delivery_id , i.customer_id , o.order_id , w.warehouse_id, s.supplier_id , o.employee_id , p.payment_id , p2.payment_method_id , r.refund_id , i.invoice_date, d.delivery_date, i.created_at , i.updated_at  from invoices i
                        join deliveries d on d.delivery_id = i.delivery_id
                        join customers c on c.customer_id = i.customer_id
                        join orders o on d.order_id = o.order_id
                        join warehouses w on d.warehouse_id = w.warehouse_id
                        join payments p on i.invoice_id = p.invoice_id
                        join suppliers s on o.supplier_id = s.supplier_id
                        join paymentmethods p2 on p.payment_method_id  = p2.payment_method_id
                        join refunds r on r.payment_id = p.payment_id where i.created_at <= '{}'""",
            "subsequent_run": """select i.invoice_id , i.delivery_id , i.customer_id , o.order_id , w.warehouse_id, s.supplier_id , o.employee_id , p.payment_id , p2.payment_method_id , r.refund_id , i.invoice_date, d.delivery_date, i.created_at , i.updated_at  from invoices i
                        join deliveries d on d.delivery_id = i.delivery_id
                        join customers c on c.customer_id = i.customer_id
                        join orders o on d.order_id = o.order_id
                        join warehouses w on d.warehouse_id = w.warehouse_id
                        join payments p on i.invoice_id = p.invoice_id
                        join suppliers s on o.supplier_id = s.supplier_id
                        join paymentmethods p2 on p.payment_method_id  = p2.payment_method_id
                        join refunds r on r.payment_id = p.payment_id where i.created_at <= '{}' and i.created_at > '{}'"""
        },
        'delete_target_table': """delete from fact_invoices where invoice_id in (select invoice_id from  fact_invoices_staged)""",
        'insert_target_table': """insert into fact_invoices(invoice_id, delivery_id, customer_id, order_id, warehouse_id , supplier_id, employee_id , payment_id , payment_method_id , refund_id , invoice_date , delivery_date , created_at, updated_at) (select invoice_id, delivery_id, customer_id, order_id, warehouse_id , supplier_id, employee_id , payment_id , payment_method_id , refund_id , invoice_date , delivery_date , created_at, updated_at from fact_invoices_staged)""",
        'truncate_stage_table': """truncate fact_invoices_staged"""
    }
]


@dag(
    dag_id='al.migration.postgresqltoredshift',
    default_args=default_args,
    start_date=datetime(2023, 2, 8, 9, 8),
    schedule_interval='0 * * * *',
    #schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["postgress", "redshift"]
)
def american_logistics_etl():  # move this hooks to respective tasks
    pg_hook = PostgresHook(postgres_conn_id='postgres_localhost')
    s3_hook = S3Hook(aws_conn_id='aws_connection')
    redshift_hook = PostgresHook(
        postgres_conn_id='redshift_connection')
    logger = logging.getLogger(__name__)

    @task()
    def postgres_to_s3(table_and_query, **context):
        pg_conn = pg_hook.get_conn()
        cursor = pg_conn.cursor()
        prev_data_interval_end_success = None
        data_interval_end = None
        logger.info(f"Context : {context}")
        export_query_template = table_and_query["source_query"]["first_run"]

        if 'prev_data_interval_end_success' in context and context['prev_data_interval_end_success'] is not None:
            logger.info(f"Subsequent run detected")
            prev_data_interval_end_success = local_tz.convert(
                context['prev_data_interval_end_success'])
            export_query_template = table_and_query["source_query"]["subsequent_run"]

        data_interval_end = local_tz.convert(context['data_interval_end'])

        sql_stmt = export_query_template.format(
            data_interval_end, prev_data_interval_end_success)

        try:
            logger.info(f"Executing query : {sql_stmt}")
            cursor.execute(sql_stmt)
        except Exception as e:
            logger.error(
                f"Error occured while fetching data from source PG in table {table_and_query['source_table']}. Error : {e}")
            raise Exception(e)

        records = cursor.fetchall()
        print(records)
        
        if not records:
            table_and_query["should_skip"] = True
            return table_and_query

        with open(f"{table_and_query['source_table_name']}.csv", 'w') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerows(records)
        cursor.close()
        pg_conn.close()

        current_time = local_tz.convert(context['data_interval_end']).strftime(
            "%Y/%m/%d/%H/%M/%S")

        s3_hook.load_file(
            filename=f"{table_and_query['source_table_name']}.csv",

            key=f"{AIRFLOW_ENVIRONMENT}/{table_and_query['source_table_name']}/{current_time}/{table_and_query['source_table_name']}.csv",
            bucket_name="american-logistics-migration",
            replace=True
        )
        table_and_query['s3Url'] = f"s3://{S3_BUCKET}/{AIRFLOW_ENVIRONMENT}/{table_and_query['source_table_name']}/{current_time}/"

        return table_and_query

    @task()
    def s3_to_staging_table(output):
        logger.info(output)
        if "should_skip" in output and output["should_skip"]:
            return output
        s3Url = output["s3Url"]
        table_name = output["staged_table_name"]
        query = f""" copy {table_name} from '{s3Url}' iam_role 'arn:aws:iam::894811220469:role/service-role/AmazonRedshift-CommandsAccessRole-20230212T135939' delimiter ',' csv quote '\"' """
        print(query)

        redshift_conn = redshift_hook.get_conn()
        cursor = redshift_conn.cursor()
        try:
            cursor.execute(query)
            redshift_conn.commit()
        except Exception as e:
            logger.error(
                f"Error occured while inserting data to stage table {table_name}. Error : {e}")
            raise Exception(e)
        cursor.close()
        redshift_conn.close()

        return output

    @task(on_failure_callback = dag_failure_alert)
    def staging_table_to_target_table(stage_table_output):
        logger.info(stage_table_output)
        if "should_skip" in stage_table_output and stage_table_output["should_skip"]:
            return stage_table_output
        delete_target_table = stage_table_output['delete_target_table']
        insert_target_table = stage_table_output['insert_target_table']
        truncate_stage_table = stage_table_output['truncate_stage_table']

        redshift_conn = redshift_hook.get_conn()
        cursor = redshift_conn.cursor()
        try:
            cursor.execute(delete_target_table)
            cursor.execute(insert_target_table)
            cursor.execute(truncate_stage_table)
            redshift_conn.commit()
        except Exception as e:
            logger.error(
                f"Error occured while upserting in to table. Error : {e}")
            raise Exception(e)
        cursor.close()
        redshift_conn.close()

    output = postgres_to_s3.expand(table_and_query=tables_and_queries)
    stage_table_output = s3_to_staging_table.expand(output=output)
    final = staging_table_to_target_table.expand(
        stage_table_output=stage_table_output)

    output >> stage_table_output >> final


american_logistics_dag = american_logistics_etl()