create or replace table merrill.dev_data_science.usage_usa_outreach_audit_events
   copy grants
   comment = 'Event level data for Outreach / DatasiteOne Marketing'
as 

-- Union all datasite_one_marketing audit/event tables
select d.project_info:subscriptionId::string as subscription_id
    , b.salesforce_id
    , a.data:projectId::string as project_id
    , d.project_info:demo::boolean as is_demo
    , a.data:userId::string as user_id
    , 'DS1M_BUYER' as event_group
    , a.data:eventName::string as event_name
    , to_date(to_timestamp(a.data:eventDateTime::numeric, 3)) as event_date
from PROD_DS1_AUDIT_EVENTS_USA_V2.DATASITE_ONE_MARKETING_BUYER a
    join  MERRILL.PROD_DS1_MONGO_GLOBAL_PROJECTS.DETAILS d on d._ID = a.data:projectId::string
    join  merrill.prod_ds1_mongo_global_subscription.subscription AS b on b._id = d.project_info:subscriptionId::string
where b.salesforce_id like 'a27%'

UNION ALL

select d.project_info:subscriptionId::string as subscription_id
    , b.salesforce_id
    , a.data:projectId::string as project_id
    , d.project_info:demo::boolean as is_demo
    , a.data:userId::string as user_id
    , 'DS1M_BUYER_ACTIVITY' as event_group
    , a.data:eventName::string as event_name
    , to_date(to_timestamp(a.data:eventDateTime::numeric, 3)) as event_date
from PROD_DS1_AUDIT_EVENTS_USA_V2.DATASITE_ONE_MARKETING_BUYER_ACTIVITY a
    join  MERRILL.PROD_DS1_MONGO_GLOBAL_PROJECTS.DETAILS d on d._ID = a.data:projectId::string
    join  merrill.prod_ds1_mongo_global_subscription.subscription AS b on b._id = d.project_info:subscriptionId::string
where b.salesforce_id like 'a27%'

UNION ALL

select d.project_info:subscriptionId::string as subscription_id
    , b.salesforce_id
    , a.data:projectId::string as project_id
    , d.project_info:demo::boolean as is_demo
    , a.data:userId::string as user_id
    , 'DS1M_BUYER_CONTACT' as event_group
    , a.data:eventName::string as event_name
    , to_date(to_timestamp(a.data:eventDateTime::numeric, 3)) as event_date
from PROD_DS1_AUDIT_EVENTS_USA_V2.DATASITE_ONE_MARKETING_BUYER_CONTACT a
    join  MERRILL.PROD_DS1_MONGO_GLOBAL_PROJECTS.DETAILS d on d._ID = a.data:projectId::string
    join  merrill.prod_ds1_mongo_global_subscription.subscription AS b on b._id = d.project_info:subscriptionId::string
where b.salesforce_id like 'a27%'

UNION ALL

select a.data:subscriptionId::string as subscription_id
    , b.salesforce_id
    , a.data:projectId::string as project_id
    , NULL as is_demo
    , a.data:userId::string as user_id
    , 'DS1M_COMPANY' as event_group
    , a.data:eventName::string as event_name
    , to_date(to_timestamp(a.data:eventDateTime::numeric, 3)) as event_date
from PROD_DS1_AUDIT_EVENTS_USA_V2.DATASITE_ONE_MARKETING_COMPANY a
    join  merrill.prod_ds1_mongo_global_subscription.subscription AS b on b._id = a.data:subscriptionId::string
where b.salesforce_id like 'a27%'

UNION ALL

select a.data:subscriptionId::string as subscription_id
    , b.salesforce_id
    , a.data:projectId::string as project_id
    , NULL as is_demo
    , a.data:userId::string as user_id
    , 'DS1M_COMPANY_CONTACT' as event_group
    , a.data:eventName::string as event_name
    , to_date(to_timestamp(a.data:eventDateTime::numeric, 3)) as event_date
from PROD_DS1_AUDIT_EVENTS_USA_V2.DATASITE_ONE_MARKETING_COMPANY_CONTACT a
    join  merrill.prod_ds1_mongo_global_subscription.subscription AS b on b._id = a.data:subscriptionId::string
where b.salesforce_id like 'a27%'

UNION ALL

select d.project_info:subscriptionId::string as subscription_id
    , b.salesforce_id
    , a.data:projectId::string as project_id
    , d.project_info:demo::boolean as is_demo
    , a.data:userId::string as user_id
    , 'DS1M_USER_ACTIVITY' as event_group
    , a.data:eventName::string as event_name
    , to_date(to_timestamp(a.data:eventDateTime::numeric, 3)) as event_date
from PROD_DS1_AUDIT_EVENTS_USA_V2.DATASITE_ONE_MARKETING_USER_ACTIVITY a
    join  MERRILL.PROD_DS1_MONGO_GLOBAL_PROJECTS.DETAILS d on d._ID = a.data:projectId::string
    join  merrill.prod_ds1_mongo_global_subscription.subscription AS b on b._id = d.project_info:subscriptionId::string
where b.salesforce_id like 'a27%'

UNION ALL

select d.project_info:subscriptionId::string as subscription_id
    , b.salesforce_id
    , a.data:projectId::string as project_id
    , d.project_info:demo::boolean as is_demo
    , a.data:userId::string as user_id
    , 'DS1M_USER_EMAIL' as event_group
    , a.data:eventName::string as event_name
    , to_date(to_timestamp(a.data:eventDateTime::numeric, 3)) as event_date
from PROD_DS1_AUDIT_EVENTS_USA_V2.DATASITE_ONE_MARKETING_USER_EMAIL a
    join  MERRILL.PROD_DS1_MONGO_GLOBAL_PROJECTS.DETAILS d on d._ID = a.data:projectId::string
    join  merrill.prod_ds1_mongo_global_subscription.subscription AS b on b._id = d.project_info:subscriptionId::string
where b.salesforce_id like 'a27%'
;