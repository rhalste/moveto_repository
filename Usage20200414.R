#################################################################
### Set up environment: Options/Packages/Functions ##############
#################################################################
basedir     <- "X:\\Base Reporting\\"
coredatadir <- "X:\\Core Data\\"
usagedir    <- "X:\\Usage\\"

gitdir      <- "C:\\GitHub\\ProdScripts\\"
gitcoredir  <- paste(gitdir,"Core\\",sep='')
snowflakecoredir <- paste(gitdir,"Snowflake\\CreateCoreTables\\",sep='')

stdfile <- source(paste(gitcoredir,"ProdOptionsPackagesFunctions.R",sep=""))

rpt_month_diff <- "202004"
### NOTE: we redefine the standard mapping for outdir here
outdir      <- paste(usagedir ,rpt_month,"\\",sep='')

# Output directory
usagesqldir <- "C:\\GitHub\\ProdScripts\\Usage\\"
olddir <- "C:\\Users\\dholmst\\Downloads\\Rusty20200413\\"

# Activity definition
start_dt <- as.Date('2020-01-01')
end_dt   <- as.Date('2020-05-31')

#################################################################
### Create Database Connections #################################
#################################################################
odbcConnectString(SF_US, Snowflake01)
odbcConnectString(SF_EU, Snowflake02)

#################################################################
### USA Build ###################################################
#################################################################
### Setup parameters
dbGetQuery(SF_US, "set region    = 'USA';")
dbGetQuery(SF_US, "set startDate = to_date('2020-01-01');")
dbGetQuery(SF_US, "set endDate   = to_date('2020-05-31');")

### Create Project Table
dbGetQuery(SF_US, read_file(paste(usagesqldir,"project.sql",sep='')))
dbGetQuery(SF_US, "select count(*), count(distinct(project_id)) from dev_data_science.usage_usa_project_detail;")
dbGetQuery(SF_US, "select count(*), count(distinct(project_id)) from dev_data_science.project_detail;")
dbGetQuery(SF_US, "select project_type, count(*) from dev_data_science.usage_usa_project_detail group by 1;")

### Create User Table
dbGetQuery(SF_US, read_file(paste(usagesqldir,"user.sql",sep='')))
dbGetQuery(SF_US, "select count(*), count(distinct(project_id||user_id)) from dev_data_science.usage_usa_user_detail;")
dbGetQuery(SF_US, "select count(*), count(distinct(project_id||user_id)) from dev_data_science.user_detail;")

### Create Doc Activity Table
dbGetQuery(SF_US, read_file(paste(usagesqldir,"usage_doc_activity.sql",sep='')))
dbGetQuery(SF_US, "select count(*), count(distinct(project_id||user_id||event_date)) from dev_data_science.usage_usa_doc_activity;")
dbGetQuery(SF_US, "select project_type, count(*) from dev_data_science.usage_usa_doc_activity group by 1;")

### Create Diligence Activity Table
dbGetQuery(SF_US, read_file(paste(usagesqldir,"usage_diligence.sql",sep='')))
dbGetQuery(SF_US, "select count(*), count(distinct(project_id||user_id||event_date)) from dev_data_science.usage_usa_diligence;")

### Create Sandbox Activity Table
dbGetQuery(SF_US, read_file(paste(usagesqldir,"usage_sandbox.sql",sep='')))
dbGetQuery(SF_US, "select count(*), count(distinct(project_id||user_id||event_date)) from dev_data_science.usage_usa_sandbox;")

### Create QA Activity Table
dbGetQuery(SF_US, read_file(paste(usagesqldir,"usage_qa.sql",sep='')))
dbGetQuery(SF_US, "select count(*), count(distinct(project_id||user_id||event_date)) from dev_data_science.usage_usa_qa;")

### Update the Redaction HEAP Table
dbGetQuery(SF_US, read_file(paste(usagesqldir,"heap_usage_redaction.sql",sep='')))
dbGetQuery(SF_US, "select count(*) as heap_events from heap_usage_redaction;")

### Create Redaction HEAP Activity table
dbGetQuery(SF_US, read_file(paste(usagesqldir,"usage_redaction_heap_based.sql",sep='')))
dbGetQuery(SF_US, "select count(*), count(distinct(project_id||user_id||event_date)) from dev_data_science.usage_usa_redaction_heap;")

### Create Redaction Activity Table
dbGetQuery(SF_US, read_file(paste(usagesqldir,"usage_redaction.sql",sep='')))
dbGetQuery(SF_US, "select count(*), count(distinct(project_id||user_id||event_date)) from dev_data_science.usage_usa_redaction;")

### Create Smart Categories Activity Table
dbGetQuery(SF_US, read_file(paste(usagesqldir,"usage_ai_categories.sql",sep='')))
dbGetQuery(SF_US, "select count(*), count(distinct(project_id||user_id||event_date)) from dev_data_science.usage_usa_ai_categories;")

### Create Acquire Activity Table
dbGetQuery(SF_US, read_file(paste(usagesqldir,"usage_acquire.sql",sep='')))
dbGetQuery(SF_US, "select count(*), count(distinct(project_id||user_id||event_date)) from dev_data_science.usage_usa_acquire;")

### Update the Outreach HEAP Table
dbGetQuery(SF_US, read_file(paste(usagesqldir,"heap_usage_outreach.sql",sep='')))
dbGetQuery(SF_US, "select count(*) as heap_events from heap_usage_usa_outreach;")

### Create Outreach HEAP Activity Summary Table
dbGetQuery(SF_US, read_file(paste(usagesqldir,"usage_outreach_heap_based.sql",sep='')))
dbGetQuery(SF_US, "select count(*), count(distinct(subscription_id||user_id||event_date)) from dev_data_science.usage_usa_outreach_heap;")
dbGetQuery(SF_US, "select count(*), count(distinct(subscription_id||event_date)) from dev_data_science.usage_usa_outreach_heap;")
dbGetQuery(SF_US, "select count(*), count(distinct(subscription_id||user_id)) from dev_data_science.usage_usa_outreach_heap;")

### Create Outreach AUDIT EVENTS Table
dbGetQuery(SF_US, read_file(paste(usagesqldir,"usage_outreach_audit_events.sql",sep='')))
dbGetQuery(SF_US, "select count(*), count(distinct(subscription_id||user_id||event_date)) from dev_data_science.usage_usa_outreach_audit_events;")
dbGetQuery(SF_US, "select count(*), count(distinct(subscription_id||user_id)) from dev_data_science.usage_usa_outreach_audit_events;")

### Create Outreach Audit Events Activity Summary Table
dbGetQuery(SF_US, read_file(paste(usagesqldir,"usage_outreach_audit_event_based.sql",sep='')))
dbGetQuery(SF_US, "select count(*), count(distinct(subscription_id||user_id||event_date)) from dev_data_science.usage_usa_outreach_audit_event_based;")
dbGetQuery(SF_US, "select count(*), count(distinct(subscription_id||event_date)) from dev_data_science.usage_usa_outreach_audit_event_based;")
dbGetQuery(SF_US, "select count(*), count(distinct(subscription_id||user_id)) from dev_data_science.usage_usa_outreach_audit_event_based;")

### Create Combined Activity Table
dbGetQuery(SF_US, read_file(paste(usagesqldir,"usage_out.sql",sep='')))
dbGetQuery(SF_US, "select count(*), count(distinct(project_id||user_id||event_date)) from dev_data_science.usage_usa;")
dbGetQuery(SF_US, "select feature, count(*) as cnt, count(distinct(project_id)) as n_project, count(distinct(project_id||user_id)) as n_user from dev_data_science.usage_usa group by 1;")

#################################################################
### DEU Build ###################################################
#################################################################
### Setup parameters
dbGetQuery(SF_EU, "set region = 'DEU';")
dbGetQuery(SF_EU, "set startDate = to_date('2020-03-13');")
dbGetQuery(SF_EU, "set endDate   = to_date(sysdate())-3;")

### Create Project Table
dbGetQuery(SF_EU, read_file(paste(usagesqldir,"deu_project.sql",sep='')))
dbGetQuery(SF_EU, "select count(*), count(distinct(project_id)) from dev_data_science.usage_deu_project_detail;")
dbGetQuery(SF_EU, "select count(*), count(distinct(project_id)) from dev_data_science.project_detail;")
dbGetQuery(SF_EU, "select project_type, count(*) from dev_data_science.usage_deu_project_detail group by 1;")

### Create User Table
dbGetQuery(SF_EU, read_file(paste(usagesqldir,"deu_user.sql",sep='')))
dbGetQuery(SF_EU, "select count(*), count(distinct(project_id||user_id)) from dev_data_science.usage_deu_user_detail;")
dbGetQuery(SF_EU, "select count(*), count(distinct(project_id||user_id)) from dev_data_science.user_detail;")

### Create Doc Activity Table
dbGetQuery(SF_EU, read_file(paste(usagesqldir,"deu_usage_doc_activity.sql",sep='')))
dbGetQuery(SF_EU, "select count(*), count(distinct(project_id||user_id||event_date)) from dev_data_science.usage_deu_doc_activity;")
dbGetQuery(SF_EU, "select project_type, count(*) from dev_data_science.usage_deu_doc_activity group by 1;")

### Create Diligence Activity Table
dbGetQuery(SF_EU, read_file(paste(usagesqldir,"deu_usage_diligence.sql",sep='')))
dbGetQuery(SF_EU, "select count(*), count(distinct(project_id||user_id||event_date)) from dev_data_science.usage_deu_diligence;")

### Create Sandbox Activity Table
dbGetQuery(SF_EU, read_file(paste(usagesqldir,"deu_usage_sandbox.sql",sep='')))
dbGetQuery(SF_EU, "select count(*), count(distinct(project_id||user_id||event_date)) from dev_data_science.usage_deu_sandbox;")

### Create QA Activity Table
dbGetQuery(SF_EU, read_file(paste(usagesqldir,"deu_usage_qa.sql",sep='')))
dbGetQuery(SF_EU, "select count(*), count(distinct(project_id||user_id||event_date)) from dev_data_science.usage_deu_qa;")

### Create Redaction Activity Table
dbGetQuery(SF_EU, read_file(paste(usagesqldir,"deu_usage_redaction.sql",sep='')))
dbGetQuery(SF_EU, "select count(*), count(distinct(project_id||user_id||event_date)) from dev_data_science.usage_deu_redaction;")

### Create Smart Categories Activity Table
dbGetQuery(SF_EU, read_file(paste(usagesqldir,"deu_usage_ai_categories.sql",sep='')))
dbGetQuery(SF_EU, "select count(*), count(distinct(project_id||user_id||event_date)) from dev_data_science.usage_deu_ai_categories;")

### Create Acquire Activity Table
dbGetQuery(SF_EU, read_file(paste(usagesqldir,"deu_usage_acquire.sql",sep='')))
dbGetQuery(SF_EU, "select count(*), count(distinct(project_id||user_id||event_date)) from dev_data_science.usage_deu_acquire;")

### Outreach is updated out of HEAP which only resides in the USA

### Create Combined Actvity Table
dbGetQuery(SF_EU, read_file(paste(usagesqldir,"deu_usage_out.sql",sep='')))
dbGetQuery(SF_EU, "select count(*), count(distinct(project_id||user_id||event_date)) from dev_data_science.usage_deu;")
dbGetQuery(SF_EU, "select feature, count(*) as cnt, count(distinct(project_id)) as n_project, count(distinct(project_id||user_id)) as n_user from dev_data_science.usage_deu group by 1;")

#################################################################
### QA ##########################################################
#################################################################
### Not all DS1 Buyer records have project_id in SFDC
xref <- getOdbcData(SF_US,"select distinct _id as project_id
                                , project_info:salesforceProjectId::string as salesforce_project_id
                           from prod_ds1_mongo_global_projects.details
                           where project_info:salesforceProjectId::string like 'a27%'")

### Get usage data
all_usa <- getOdbcData(SF_US, "select * from dev_data_science.usage_usa;")
all_deu <- getOdbcData(SF_EU, "select * from dev_data_science.usage_deu;")

all00 <- rbindlist(list(all_usa,all_deu))

saveRDS(all00,paste(usagesqldir,"all00.rds",sep=''))
# all00 <- readRDS(all00,paste(usagesqldir,"all00.rds",sep=''))

all01 <- merge(x=all00, y=xref, by="project_id", all.x=TRUE)

saveRDS(all01,paste(outdir,"all01.rds",sep=''))
# all01 <- readRDS(all01,paste(outdir,"all01.rds",sep=''))

### Bring in SFDC Data
sfdc01 <- readRDS(paste(olddir,"sfdc01.rds",sep='')) # 8,783
sfdc02 <- sfdc01[is.na(close_dt) | close_dt > end_dt,] # 8,773

sfdc_ds1m00 <- readRDS(paste(olddir,"sfdc_ds1m00.rds",sep='')) # 19
sfdc_ds1m01 <- sfdc_ds1m00[is.na(close_dt),] # 18

sfdc_ds1_buyer00 <- readRDS(paste(olddir,"sfdc_ds1_buyer00.rds",sep='')) # 5

# Keep only open project
sfdc_ds1_buyer01 <- sfdc_ds1_buyer00[is.na(close_dt) | close_dt > end_dt,] # 4

### DS1 Buyer
### 1 project is marked as demo.  Taken out in new code.
### DS1 Buyer users were not validated against dev_data_science.user_detail
### so one datasite.com user slipped through the cracks.

ds1b00 <- merge(x=all01, y=sfdc_ds1_buyer01[,.(sfdc_id)],by.x="salesforce_project_id",by.y="sfdc_id",all.x=FALSE)
ds1b00[, .(countDistinct(project_id), countDistinct(user_id))]

sqldf("select distinct project_id, user_id from ds1_buyer_users 
         except select distinct project_id, user_id from ds1b00")

all01[feature=='Acquire', .(countDistinct(user_id)), keyby=.(project_id)]

### AI Categories
# Match

