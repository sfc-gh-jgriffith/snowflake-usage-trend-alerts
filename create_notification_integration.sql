-- allowed recipients must have verified email addresses
CREATE NOTIFICATION INTEGRATION 
    email_notification
    TYPE=EMAIL
    ENABLED=TRUE
    ALLOWED_RECIPIENTS=('your.email@snowflake.com');
