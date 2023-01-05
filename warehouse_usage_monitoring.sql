create or replace procedure warehouse_usage_monitoring(alert_threshold_low float, 
                                                       alert_threshold_high float, 
                                                       warehouse_names array, 
                                                       email_addresses array)
returns variant
language PYTHON
runtime_version='3.8'
packages=('snowflake-snowpark-python', 'pandas')
handler='run'
EXECUTE AS CALLER -- cannot use OWNER when querying INFORMATION_SCHEMA
as 
$$
def run(session, alert_threshold_low:float, alert_threshold_high:float, warehouse_names:list, email_addresses:list):
    import pandas as pd 
    
    summ = session.sql(
    """with warehouse_by_day as (
        select 
            warehouse_name,
            date(end_time) as date,
            sum(credits_used) as credits_used,
            sum(credits_used_compute) as credits_used_compute,
            sum(credits_used_cloud_services) as credits_used_cloud_services
        from
            table(information_schema.WAREHOUSE_METERING_HISTORY(
                                    DATE_RANGE_START => current_date() - interval '5 days'
                                )
                 ) a
        where 
            true 
            and hour(end_time) < hour(current_time())
        group by 
            warehouse_name,
            date(end_time)  
    )
    select 
        warehouse_name, 
        avg(case when date = current_date() then credits_used end)  credits_used_today,
        avg(case when date = current_date() then credits_used_compute end) credits_used_compute_today,
        avg(case when date = current_date() then credits_used_cloud_services end) credits_used_cloud_services_today,
        avg(case when date = current_date() then null else credits_used end)  credits_used_trend,
        avg(case when date = current_date() then null else credits_used_compute end) credits_used_compute_trend,
        avg(case when date = current_date() then null else credits_used_cloud_services end) credits_used_cloud_services_trend,
        div0(credits_used_today, credits_used_trend) - 1 as credits_used_var_to_trend,
        div0(credits_used_compute_today, credits_used_compute_trend) - 1 as credits_used_compute_var_to_trend,
        div0(credits_used_cloud_services_today, credits_used_cloud_services_trend) - 1 as credits_used_cloud_services_var_to_trend
    from
        warehouse_by_day
    group by 
        warehouse_name""").to_pandas()
    
    if not warehouse_names:
        warehouse_names = summ['WAREHOUSE_NAME'].tolist()
    
    warehouse_names = [c.upper() for c in warehouse_names]
    
    alert = summ.loc[summ['WAREHOUSE_NAME'].isin(warehouse_names)]
    alert = alert.loc[(summ['CREDITS_USED_VAR_TO_TREND'] < alert_threshold_low) | (summ['CREDITS_USED_VAR_TO_TREND'] > alert_threshold_high)]
    
    if alert.shape[0] > 0:
        email_content =  "\n\n=======WAREHOUSE ALERTS=======\n\n"
        email_content += "THE FOLLOWING WAREHOUSES HAVE REACHED ALERT THRESHOLD: " + ", ".join(alert['WAREHOUSE_NAME'].tolist())
        email_content += "\n\n"
        email_content += '\n\n'.join([str(i['WAREHOUSE_NAME']) +'\n\t' + '\n\t'.join([f'{j}: {k}' for j, k in i.items()] )  for i in alert.T.to_dict().values() ])

        email_content += "\n\n=======ALL WAREHOUSE STATS=======\n\n"
        email_content += '\n\n'.join([str(i['WAREHOUSE_NAME']) +'\n\t' + '\n\t'.join([f'{j}: {k}' for j, k in i.items()] )  for i in summ.T.to_dict().values() ])

        session.call('SYSTEM$SEND_EMAIL',
                     'email_notification',
                     ', '.join(email_addresses),
                     'WAREHOUSE USAGE ALERT',
                      email_content
        );
    
    return alert.T.to_dict()
    
$$;

-- test for ± 50% variance from trend
call warehouse_usage_monitoring(-0.5, 0.5, [], ['your.email@yourdomain.com']);

-- schedule a run at 5 minutes after every hour
create or replace task usage_monitoring 
    warehouse='XSMALL'
    schedule= 'USING CRON 5 * * * * America/Chicago'
    as call warehouse_usage_monitoring(-0.5, 0.5, [], ['your.email@yourdomain.com']);
    
alter task usage_monitoring resume;
