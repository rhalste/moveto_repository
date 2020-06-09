/********************************************************************/
/*** Create Project Detail ******************************************/
/********************************************************************/
create or replace table merrill.dev_data_science.usage_usa_project_detail
   copy grants
   comment = 'Usage Project Detail'
as select _id                                      as project_id
        , project_info:salesforceProjectId::string as salesforce_project_id
        , project_info:projectType::string         as project_type
        , to_char(audit_create_date,'YYYY-MM-DD')  as create_date
        , to_char(to_date(to_timestamp(project_info:launchInfo:launchDate::numeric, 3))) as launch_date
        , max(case when value:settingName::String = 'qaEnabled'         and value:value::String in ('true')                 then 1 else 0 end) as qa_enabled
        , max(case when value:settingName::String = 'sandboxEnabled'    and value:value::String in ('true')                 then 1 else 0 end) as sandbox_enabled
        , max(case when value:settingName::String = 'smartToolsEnabled' and value:value::String in ('true')                 then 1 else 0 end) as smartTools_enabled
        , max(case when value:settingName::String = 'redactionEnabled'  and value:value::String in ('true','BULK','MANUAL') then 1 else 0 end) as redaction_enabled
        , max($endDate) as cutoff_date
from prod_ds1_mongo_global_projects.details, lateral flatten(project_configurations)
where project_info:datacenter::string = $region
  and project_info:demo::boolean = FALSE
  and project_info:salesforceProjectId::string like 'a27%'
  and lower(project_info:projectName::string) NOT LIKE '%test automation%' 
  and deleted = FALSE
  and (to_date(audit_create_date) <= $endDate or audit_create_date IS NULL)
group by 1,2,3,4,5;
