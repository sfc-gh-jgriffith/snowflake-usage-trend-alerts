# snowflake-usage-trend-alerts
This code will compare email usage to a five day trend and send an email alert if any warehouse hits the alert threshold criteria. 

[create_notification_integration.sql](create_notification_integration.sql): Creates a notification integration that will be used by our stored procedure to send [email notifications](https://docs.snowflake.com/en/sql-reference/email-stored-procedures.html). You must specify allowed email addresses for this integration. These email addresses must be [verified](https://docs.snowflake.com/en/sql-reference/email-stored-procedures.html#verifying-email-addresses-of-notification-recipients) to receive emails from your Snowflake account. 

[warehouse_usage_monitoring.sql](warehouse_usage_monitoring.sql): Creates a Python stored procedure and a task to run that procedure at 5 minutes after every hour. This procedure queries `information_schema.WAREHOUSE_METERING_HISTORY()` and compares today's usage through the past hour to a five day trend. Alert thresholds, warehouses, and emails to alert are arguments to the stored procedure.

Procedure call syntax:
```
warehouse_usage_monitoring(
  alert_threshold_low float
  , alert_threshold_high float
  , warehouse_names array
  , email_addresses array)
```

Alert thresholds are percent changes versus trend. If you want to alert for ± 50% versus trend, `alert_threshold_low` would be -0.5 and `alert_threshold_high` would be 0.5.

`warehouse_names` is an array. If you would like to monitor all warehouses, send an empty array: `[]`.

`email_addresses` is an array containing email addresses to be notified if an alert threshold is met. 

