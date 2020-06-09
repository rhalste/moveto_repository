// Add a comment
create or replace table merrill.dev_data_science.heap_usage_usa_outreach
  copy grants
  comment = 'DS1 Marketing Heap Table with salesforce_id filter a27'

as
select 'ds1_marketing_add_deal_team_member' as event_name, event_id, target_text, type, user_id, subscriptionid, projectid, session_id, session_time, time
from datasite_one_heap_landing.heap.ds1_marketing_add_deal_team_member a
    join  merrill.prod_ds1_mongo_global_subscription.subscription AS b on b._id = a.subscriptionid
where b.salesforce_id  like 'a27%'
    union all

select 'ds1_marketing_add_watermark_to_pdf_in_email_' as event_name, event_id, target_text, type, user_id, subscriptionid, projectid, session_id, session_time, time
from datasite_one_heap_landing.heap.ds1_marketing_add_watermark_to_pdf_in_email_ a
    join  merrill.prod_ds1_mongo_global_subscription.subscription AS b on b._id = a.subscriptionid
where b.salesforce_id  like 'a27%'
    union all

select 'ds1_marketing_bulk_email_button' as event_name, event_id, target_text, type, user_id, subscriptionid, projectid, session_id, session_time, time
from datasite_one_heap_landing.heap.ds1_marketing_bulk_email_button a
    join  merrill.prod_ds1_mongo_global_subscription.subscription AS b on b._id = a.subscriptionid
where b.salesforce_id  like 'a27%'
    union all

select 'ds1_marketing_cancel_project_create' as event_name, event_id, target_text, type, user_id, subscriptionid, projectid, session_id, session_time, time
from datasite_one_heap_landing.heap.ds1_marketing_cancel_project_create a
    join  merrill.prod_ds1_mongo_global_subscription.subscription AS b on b._id = a.subscriptionid
where b.salesforce_id  like 'a27%'
    union all

select 'ds1_marketing_choose_another_project' as event_name, event_id, target_text, type, user_id, subscriptionid, projectid, session_id, session_time, time
from datasite_one_heap_landing.heap.ds1_marketing_choose_another_project a
    join  merrill.prod_ds1_mongo_global_subscription.subscription AS b on b._id = a.subscriptionid
where b.salesforce_id  like 'a27%'
    union all

select 'ds1_marketing_click_on_buyer_bids_page' as event_name, event_id, target_text, type, user_id, subscriptionid, projectid, session_id, session_time, time
from datasite_one_heap_landing.heap.ds1_marketing_click_on_buyer_bids_page a
    join  merrill.prod_ds1_mongo_global_subscription.subscription AS b on b._id = a.subscriptionid
where b.salesforce_id  like 'a27%'
    union all

select 'ds1_marketing_click_on_buyer_nda_page' as event_name, event_id, target_text, type, user_id, subscriptionid, projectid, session_id, session_time, time
from datasite_one_heap_landing.heap.ds1_marketing_click_on_buyer_nda_page a
    join  merrill.prod_ds1_mongo_global_subscription.subscription AS b on b._id = a.subscriptionid
where b.salesforce_id  like 'a27%'
    union all

select 'ds1_marketing_click_on_buyer_tracking_page' as event_name, event_id, target_text, type, user_id, subscriptionid, projectid, session_id, session_time, time
from datasite_one_heap_landing.heap.ds1_marketing_click_on_buyer_tracking_page a
    join  merrill.prod_ds1_mongo_global_subscription.subscription AS b on b._id = a.subscriptionid
where b.salesforce_id  like 'a27%'
    union all

select 'ds1_marketing_click_on_email_drafts_page' as event_name, event_id, target_text, type, user_id, subscriptionid, projectid, session_id, session_time, time
from datasite_one_heap_landing.heap.ds1_marketing_click_on_email_drafts_page a
    join  merrill.prod_ds1_mongo_global_subscription.subscription AS b on b._id = a.subscriptionid
where b.salesforce_id  like 'a27%'
    union all

select 'ds1_marketing_click_on_inbox_page' as event_name, event_id, target_text, type, user_id, subscriptionid, projectid, session_id, session_time, time
from datasite_one_heap_landing.heap.ds1_marketing_click_on_inbox_page a
    join  merrill.prod_ds1_mongo_global_subscription.subscription AS b on b._id = a.subscriptionid
where b.salesforce_id  like 'a27%'
    union all

select 'ds1_marketing_click_on_watermarking_settings' as event_name, event_id, target_text, type, user_id, subscriptionid, projectid, session_id, session_time, time
from datasite_one_heap_landing.heap.ds1_marketing_click_on_watermarking_settings a
    join  merrill.prod_ds1_mongo_global_subscription.subscription AS b on b._id = a.subscriptionid
where b.salesforce_id  like 'a27%'
    union all

select 'ds1_marketing_convert_inbound_email_to_an_activity' as event_name, event_id, target_text, type, user_id, subscriptionid, projectid, session_id, session_time, time
from datasite_one_heap_landing.heap.ds1_marketing_convert_inbound_email_to_an_activity a
    join  merrill.prod_ds1_mongo_global_subscription.subscription AS b on b._id = a.subscriptionid
where b.salesforce_id  like 'a27%'
    union all

select 'ds1_marketing_download_draft_email' as event_name, event_id, target_text, type, user_id, subscriptionid, projectid, session_id, session_time, time
from datasite_one_heap_landing.heap.ds1_marketing_download_draft_email a
    join  merrill.prod_ds1_mongo_global_subscription.subscription AS b on b._id = a.subscriptionid
where b.salesforce_id  like 'a27%'
    union all

select 'ds1_marketing_download_or_bulk_download' as event_name, event_id, target_text, type, user_id, subscriptionid, projectid, session_id, session_time, time
from datasite_one_heap_landing.heap.ds1_marketing_download_or_bulk_download a
    join  merrill.prod_ds1_mongo_global_subscription.subscription AS b on b._id = a.subscriptionid
where b.salesforce_id  like 'a27%'
    union all

select 'ds1_marketing_ds1m_add_deal_docs_button' as event_name, event_id, target_text, type, user_id, subscriptionid, projectid, session_id, session_time, time
from datasite_one_heap_landing.heap.ds1_marketing_ds1m_add_deal_docs_button a
    join  merrill.prod_ds1_mongo_global_subscription.subscription AS b on b._id = a.subscriptionid
where b.salesforce_id  like 'a27%'
    union all

select 'ds1_marketing_ds1m_buyer_type_filter_in_buyer_tracking' as event_name, event_id, target_text, type, user_id, subscriptionid, projectid, session_id, session_time, time
from datasite_one_heap_landing.heap.ds1_marketing_ds1m_buyer_type_filter_in_buyer_tracking a
    join  merrill.prod_ds1_mongo_global_subscription.subscription AS b on b._id = a.subscriptionid
where b.salesforce_id  like 'a27%'
    union all

select 'ds1_marketing_ds1m_cancel_button_in_preview_email_draft' as event_name, event_id, target_text, type, user_id, subscriptionid, projectid, session_id, session_time, time
from datasite_one_heap_landing.heap.ds1_marketing_ds1m_cancel_button_in_preview_email_draft a
    join  merrill.prod_ds1_mongo_global_subscription.subscription AS b on b._id = a.subscriptionid
where b.salesforce_id  like 'a27%'
    union all

select 'ds1_marketing_ds1m_create_zip_for_x_drafts' as event_name, event_id, target_text, type, user_id, subscriptionid, projectid, session_id, session_time, time
from datasite_one_heap_landing.heap.ds1_marketing_ds1m_create_zip_for_x_drafts a
    join  merrill.prod_ds1_mongo_global_subscription.subscription AS b on b._id = a.subscriptionid
where b.salesforce_id  like 'a27%'
    union all

select 'ds1_marketing_ds1m_generate_activity_summary' as event_name, event_id, target_text, type, user_id, subscriptionid, projectid, session_id, session_time, time
from datasite_one_heap_landing.heap.ds1_marketing_ds1m_generate_activity_summary a
    join  merrill.prod_ds1_mongo_global_subscription.subscription AS b on b._id = a.subscriptionid
where b.salesforce_id  like 'a27%'
    union all

select 'ds1_marketing_ds1m_generate_email_drafts_new' as event_name, event_id, target_text, type, user_id, subscriptionid, projectid, session_id, session_time, time
from datasite_one_heap_landing.heap.ds1_marketing_ds1m_generate_email_drafts_new a
    join  merrill.prod_ds1_mongo_global_subscription.subscription AS b on b._id = a.subscriptionid
where b.salesforce_id  like 'a27%'
    union all

select 'ds1_marketing_generate_activity_summary' as event_name, event_id, target_text, type, user_id, subscriptionid, projectid, session_id, session_time, time
from datasite_one_heap_landing.heap.ds1_marketing_generate_activity_summary a
    join  merrill.prod_ds1_mongo_global_subscription.subscription AS b on b._id = a.subscriptionid
where b.salesforce_id  like 'a27%'
    union all

select 'ds1_marketing_save_activity_from_inbound_email' as event_name, event_id, target_text, type, user_id, subscriptionid, projectid, session_id, session_time, time
from datasite_one_heap_landing.heap.ds1_marketing_save_activity_from_inbound_email a
    join  merrill.prod_ds1_mongo_global_subscription.subscription AS b on b._id = a.subscriptionid
where b.salesforce_id  like 'a27%'
    union all

select 'ds1_marketing_save_activity_manually_from_buyer_tracking_drilldown' as event_name, event_id, target_text, type, user_id, subscriptionid, projectid, session_id, session_time, time
from datasite_one_heap_landing.heap.ds1_marketing_save_activity_manually_from_buyer_tracking_drilldown a
    join  merrill.prod_ds1_mongo_global_subscription.subscription AS b on b._id = a.subscriptionid
where b.salesforce_id  like 'a27%'
    union all

select 'ds1_marketing_save_activity_manually_from_buyer_tracking_table' as event_name, event_id, target_text, type, user_id, subscriptionid, projectid, session_id, session_time, time
from datasite_one_heap_landing.heap.ds1_marketing_save_activity_manually_from_buyer_tracking_table a
    join  merrill.prod_ds1_mongo_global_subscription.subscription AS b on b._id = a.subscriptionid
where b.salesforce_id  like 'a27%'
    union all

select 'ds1_marketing_save_buyer_to_project' as event_name, event_id, target_text, type, user_id, subscriptionid, projectid, session_id, session_time, time
from datasite_one_heap_landing.heap.ds1_marketing_save_buyer_to_project a
    join  merrill.prod_ds1_mongo_global_subscription.subscription AS b on b._id = a.subscriptionid
where b.salesforce_id  like 'a27%'
    union all

select 'ds1_marketing_save_project_create' as event_name, event_id, target_text, type, user_id, subscriptionid, projectid, session_id, session_time, time
from datasite_one_heap_landing.heap.ds1_marketing_save_project_create a
         join  merrill.prod_ds1_mongo_global_subscription.subscription AS b on b._id = a.subscriptionid
where b.salesforce_id  like 'a27%'
;