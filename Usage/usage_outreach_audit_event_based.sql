create or replace table merrill.dev_data_science.usage_usa_outreach_audit_event_based
   copy grants
   comment = 'Outreach Usage Activity based on Audit Events'
as

with ds1m_subscription as
(select distinct a._id as subscription_id
    , a.salesforce_id
    , a.name as subscription_name
from merrill.prod_ds1_mongo_global_subscription.subscription a 
    join prod_ds1_mongo_global_projects.details b on b.project_info:subscriptionId::string = a._id  
where a.salesforce_id like 'a27%'
  and a.name NOT LIKE 'DS1M Demo%'
  and lower(b.project_info:projectName::string) NOT LIKE '%test automation%' 
  and b.deleted = FALSE
  and (to_date(b.audit_create_date) <= $endDate or b.audit_create_date IS NULL)
),

ds1m_heap_event as
(
  select b.subscription_name
    , d.project_info:projectName::string as project_name
    , d.project_info:demo::boolean as is_demo
    , case when lower(d.project_info:projectName::string) like '%support%' then true else false end as is_support
    , case when c.email_address IS NULL then 'Unidentified user' else lower(c.email_address) end as email_address
    , case when lower(c.email_address) like '%merrill%'
             or lower(c.email_address) like '%datasite%' then true else false end as merrill_email
    , a.event_name
    , a.event_date
    , a.subscription_id
    , a.project_id
    , a.user_id
from merrill.dev_data_science.usage_usa_outreach_audit_events as a
    join ds1m_subscription b on b.subscription_id = a.subscription_id
    left join  merrill.prod_ds1_mongo_global_projects.details d on d._id = a.project_id
    left join  merrill.prod_ds1_mongo_global_userdetails.details c on c._id = a.user_id
where project_id is not null
and lower(project_name) not like '%support%'
)

select distinct subscription_id
              , user_id
              , event_date
              , 'Outreach' as feature
from ds1m_heap_event
where event_date >= $startDate
  and event_date <= $endDate
  and merrill_email = FALSE 
  and is_support = FALSE
  and (is_demo=FALSE or is_demo IS NULL)
;
