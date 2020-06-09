create or replace table merrill.dev_data_science.heap_usage_usa_outreach
  copy grants
  comment = 'DS1 Marketing Heap Table'

as

select 'ds1_marketing_add_deal_team_member' as event_name, event_id, target_text, type, user_id, subscriptionid, projectid, session_id, session_time, time
from datasite_one_heap_landing.heap.ds1_marketing_add_deal_team_member
   union all

select 'ds1_marketing_add_watermark_to_pdf_in_email_' as event_name, event_id, target_text, type, user_id, subscriptionid, projectid, session_id, session_time, time
from datasite_one_heap_landing.heap.ds1_marketing_add_watermark_to_pdf_in_email_
   union all

select 'ds1_marketing_bulk_email_button' as event_name, event_id, target_text, type, user_id, subscriptionid, projectid, session_id, session_time, time
from datasite_one_heap_landing.heap.ds1_marketing_bulk_email_button
   union all

select 'ds1_marketing_cancel_project_create' as event_name, event_id, target_text, type, user_id, subscriptionid, projectid, session_id, session_time, time
from datasite_one_heap_landing.heap.ds1_marketing_cancel_project_create
   union all

select 'ds1_marketing_choose_another_project' as event_name, event_id, target_text, type, user_id, subscriptionid, projectid, session_id, session_time, time
from datasite_one_heap_landing.heap.ds1_marketing_choose_another_project
   union all

select 'ds1_marketing_click_on_buyer_bids_page' as event_name, event_id, target_text, type, user_id, subscriptionid, projectid, session_id, session_time, time
from datasite_one_heap_landing.heap.ds1_marketing_click_on_buyer_bids_page
   union all

select 'ds1_marketing_click_on_buyer_nda_page' as event_name, event_id, target_text, type, user_id, subscriptionid, projectid, session_id, session_time, time
from datasite_one_heap_landing.heap.ds1_marketing_click_on_buyer_nda_page
   union all

select 'ds1_marketing_click_on_buyer_tracking_page' as event_name, event_id, target_text, type, user_id, subscriptionid, projectid, session_id, session_time, time
from datasite_one_heap_landing.heap.ds1_marketing_click_on_buyer_tracking_page
   union all

select 'ds1_marketing_click_on_email_drafts_page' as event_name, event_id, target_text, type, user_id, subscriptionid, projectid, session_id, session_time, time
from datasite_one_heap_landing.heap.ds1_marketing_click_on_email_drafts_page
   union all

select 'ds1_marketing_click_on_inbox_page' as event_name, event_id, target_text, type, user_id, subscriptionid, projectid, session_id, session_time, time
from datasite_one_heap_landing.heap.ds1_marketing_click_on_inbox_page
   union all

select 'ds1_marketing_click_on_watermarking_settings' as event_name, event_id, target_text, type, user_id, subscriptionid, projectid, session_id, session_time, time
from datasite_one_heap_landing.heap.ds1_marketing_click_on_watermarking_settings
   union all

select 'ds1_marketing_convert_inbound_email_to_an_activity' as event_name, event_id, target_text, type, user_id, subscriptionid, projectid, session_id, session_time, time
from datasite_one_heap_landing.heap.ds1_marketing_convert_inbound_email_to_an_activity
   union all

select 'ds1_marketing_download_draft_email' as event_name, event_id, target_text, type, user_id, subscriptionid, projectid, session_id, session_time, time
from datasite_one_heap_landing.heap.ds1_marketing_download_draft_email
   union all

select 'ds1_marketing_download_or_bulk_download' as event_name, event_id, target_text, type, user_id, subscriptionid, projectid, session_id, session_time, time
from datasite_one_heap_landing.heap.ds1_marketing_download_or_bulk_download
   union all

select 'ds1_marketing_ds1m_add_deal_docs_button' as event_name, event_id, target_text, type, user_id, subscriptionid, projectid, session_id, session_time, time
from datasite_one_heap_landing.heap.ds1_marketing_ds1m_add_deal_docs_button
   union all

select 'ds1_marketing_ds1m_buyer_type_filter_in_buyer_tracking' as event_name, event_id, target_text, type, user_id, subscriptionid, projectid, session_id, session_time, time
from datasite_one_heap_landing.heap.ds1_marketing_ds1m_buyer_type_filter_in_buyer_tracking
   union all

select 'ds1_marketing_ds1m_cancel_button_in_preview_email_draft' as event_name, event_id, target_text, type, user_id, subscriptionid, projectid, session_id, session_time, time
from datasite_one_heap_landing.heap.ds1_marketing_ds1m_cancel_button_in_preview_email_draft
   union all

select 'ds1_marketing_ds1m_create_zip_for_x_drafts' as event_name, event_id, target_text, type, user_id, subscriptionid, projectid, session_id, session_time, time
from datasite_one_heap_landing.heap.ds1_marketing_ds1m_create_zip_for_x_drafts
   union all

select 'ds1_marketing_ds1m_generate_activity_summary' as event_name, event_id, target_text, type, user_id, subscriptionid, projectid, session_id, session_time, time
from datasite_one_heap_landing.heap.ds1_marketing_ds1m_generate_activity_summary
   union all

select 'ds1_marketing_ds1m_generate_email_drafts_new' as event_name, event_id, target_text, type, user_id, subscriptionid, projectid, session_id, session_time, time
from datasite_one_heap_landing.heap.ds1_marketing_ds1m_generate_email_drafts_new
   union all

select 'ds1_marketing_generate_activity_summary' as event_name, event_id, target_text, type, user_id, subscriptionid, projectid, session_id, session_time, time
from datasite_one_heap_landing.heap.ds1_marketing_generate_activity_summary
   union all

select 'ds1_marketing_save_activity_from_inbound_email' as event_name, event_id, target_text, type, user_id, subscriptionid, projectid, session_id, session_time, time
from datasite_one_heap_landing.heap.ds1_marketing_save_activity_from_inbound_email
   union all

select 'ds1_marketing_save_activity_manually_from_buyer_tracking_drilldown' as event_name, event_id, target_text, type, user_id, subscriptionid, projectid, session_id, session_time, time
from datasite_one_heap_landing.heap.ds1_marketing_save_activity_manually_from_buyer_tracking_drilldown
   union all

select 'ds1_marketing_save_activity_manually_from_buyer_tracking_table' as event_name, event_id, target_text, type, user_id, subscriptionid, projectid, session_id, session_time, time
from datasite_one_heap_landing.heap.ds1_marketing_save_activity_manually_from_buyer_tracking_table
   union all

select 'ds1_marketing_save_buyer_to_project' as event_name, event_id, target_text, type, user_id, subscriptionid, projectid, session_id, session_time, time
from datasite_one_heap_landing.heap.ds1_marketing_save_buyer_to_project
   union all

select 'ds1_marketing_save_project_create' as event_name, event_id, target_text, type, user_id, subscriptionid, projectid, session_id, session_time, time
from datasite_one_heap_landing.heap.ds1_marketing_save_project_create
;