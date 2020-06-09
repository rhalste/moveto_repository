/********************************************************************/
/*** Create User Detail *********************************************/
/********************************************************************/
create or replace table merrill.dev_data_science.usage_usa_user_detail
   copy grants
   comment = 'Usage User Detail'
as 

with pre_user as
(
select distinct pm.project_id
              , pm.user_id 
              , pd.project_type
              , to_date(pm.audit_create_date) as create_date
              , $endDate                      as cutoff_date
from prod_ds1_mongo_global_userdetails.project_membership pm
  inner join dev_data_science.usage_usa_project_detail pd on pm.project_id = pd.project_id
  inner join prod_ds1_mongo_global_userdetails.details ud on ud._id = pm.user_id
where (to_date(pm.audit_create_date) <= $endDate or pm.audit_create_date IS NULL)
    and lower(ud.email_address) NOT LIKE '%merrillcorp%'
    and lower(ud.email_address) NOT LIKE '%mailinator%'
    and lower(ud.email_address) NOT LIKE '%mailintor%'  
    and lower(ud.email_address) NOT LIKE '%fakename%'
    and lower(ud.email_address) NOT LIKE '%@mail.stp.mrll%'
    and lower(ud.email_address) NOT LIKE '%@mymerrillcorp.mail.onmicrosoft%'
    and lower(ud.email_address) NOT LIKE '%@user.com%'  
    and lower(ud.email_address) NOT LIKE '%@test.com%'  
    and lower(ud.email_address) NOT LIKE '%@email.com%' 
    and lower(ud.email_address) NOT LIKE '%@mail.com%' 
    and lower(ud.email_address) NOT LIKE '%@domain.com%' 
    and lower(ud.email_address) NOT LIKE 'email-test%'   
    and lower(ud.email_address) NOT LIKE '%@test.org'
    and lower(ud.email_address) NOT LIKE '%@test.co.uk%'   
    and lower(ud.email_address) NOT LIKE '%@datasite.com%'
),

qa_team as
(
select pm.project_id
     , pm.user_id
     , max(case when grp.value:userGroupType::string = 'QA_TEAM' then 1 else 0 end) as qa_team_flag
from prod_ds1_mongo_global_userdetails.project_membership pm
  inner join dev_data_science.usage_usa_project_detail pd on pm.project_id = pd.project_id
  inner join lateral flatten (user_groups) grp
group by 1,2
),

first_user as
(
select project_id
     , user_id
     , min(create_date) as min_create_date
from pre_user
group by 1, 2
)

select pu.*
     , qa.qa_team_flag
from pre_user pu
  inner join first_user fu
    on  pu.project_id  = fu.project_id
    and pu.user_id     = fu.user_id
    and pu.create_date = fu.min_create_date
  left  join qa_team qa
    on  pu.project_id  = qa.project_id
    and pu.user_id     = qa.user_id;