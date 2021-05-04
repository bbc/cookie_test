#clear environment variables for any tokens
Sys.unsetenv("AWS_ACCESS_KEY_ID")
Sys.unsetenv("AWS_SECRET_ACCESS_KEY")
Sys.unsetenv("AWS_SESSION_TOKEN")

################## Connect to Redshift ########################
## Note - if you're running this on a MAP pipeline you'll need to pass in REDSHIFT_USERNAME and REDSHIFT_PASSWORD as environment variables.
library("RJDBC")
options(java.parameters = "-Xmx1024m")

get_redshift_connection_ccog <- function() {
  
  # create JDBC driver
  driver <- JDBC(driverClass = "com.amazon.redshift.jdbc.Driver",  classPath = "/usr/lib/drivers/RedshiftJDBC42-no-awssdk-1.2.41.1065.jar", identifier.quote="`")
  
  # create redshift url with username and password
  url <- stringr::str_interp("jdbc:redshift://live-idl-prod-redshift-component-redshiftcluster-1q6vyltqf8lth.ctm1v7db0ubd.eu-west-1.redshift.amazonaws.com:5439/redshiftdb?user=${Sys.getenv('REDSHIFT_USERNAME')}&password=${Sys.getenv('REDSHIFT_PASSWORD')}")
  conn <- dbConnect(driver, url)
  return(conn)
  
}


## Get temporany S3 credentials
get_s3_credentials <- function() {
  
  s3credentials <- fromJSON(content(GET("http://169.254.169.254/latest/meta-data/iam/security-credentials/live-EKS-jupyterhub-NodeInstanceRole")))
  
  return(s3credentials)
}

#function to write data is Redshift via s3
library(rjson)
library(httr)
library(aws.s3)
library(aws.ec2metadata)
library(data.table)
library(tidyverse)
write_to_redshift <- function(df, s3_folder, redshift_schema, redshift_table) {
  
  redshift_location <- paste(redshift_schema, redshift_table, sep = ".")
  
  s3_bucket <- 'rstudio-input-output'
  s3credentials<- get_s3_credentials() #Call function to get s3 credentials
  
  token <- s3credentials$Token
  secret_access_key <- s3credentials$SecretAccessKey
  access_key_id <- s3credentials$AccessKeyId
  
  job_start_time <- Sys.time()
  

  # move dataset to S3 folder:
  s3write_using(df, 
                FUN = fwrite,
                row.names = FALSE,
                bucket = s3_bucket, 
                object = paste(s3_folder, quote(df), sep = "/"))

  
  # define table column types:
  db_coltypes <- sapply(df, class)
  
  integers <- sapply(df, 
                     function(x){
                       x <- x[!is.na(x)]
                       # find numeric columns that are not integers:                   
                       ifelse(!is.numeric(x), FALSE, ifelse(all(x%%1==0), TRUE, FALSE))
                     } )
  
  has_nas <- sapply(df, function(x) any(is.na(x)))
  
  db_coltypes_df <- data.frame(names = names(db_coltypes),
                               type = unlist(db_coltypes),
                               has_nas = unlist(has_nas),
                               integers = unlist(integers),
                               row.names = NULL,
                               stringsAsFactors = FALSE)
  
  db_coltypes_df <- db_coltypes_df %>% 
    mutate(aws_type = case_when(
      type == "Date" ~ "DATE",
      grepl("factor|character",type) ~ "VARCHAR(400)",
      grepl("integer|numeric",type) & integers == TRUE ~ "INT", 
      grepl("integer|numeric",type) & integers == FALSE ~ "DECIMAL(18, 4)"
    )) %>% 
    mutate(var_def = paste(names, aws_type, sep = " "))
  
  table_def <- paste(db_coltypes_df$var_def, collapse = ", ")
  
  conn <- get_redshift_connection_ccog()
  
  # Create temp table in Redshift for the latest processed records:
  redshift_location_LATEST <- paste0(redshift_location, "_LATEST")

  dbSendUpdate(conn, 
               paste0(
                 "DROP TABLE IF EXISTS ", redshift_location_LATEST, ";
                 CREATE TABLE ", redshift_location_LATEST, " (",  table_def , ");
                 GRANT SELECT ON ", redshift_location_LATEST, 
                 " TO GROUP central_insights;"
               )
  )
  
  # Create the copy statement
  redshift_copy_statement <-
    paste0(
      "COPY ", redshift_location_LATEST, 
      " FROM 's3://", s3_bucket, "/", s3_folder, "/", quote(df), 
      "' credentials 'aws_access_key_id=", access_key_id,
      ";aws_secret_access_key=", secret_access_key,
      ";token=", token,
      "' CSV DELIMITER ',' IGNOREHEADER 1 DATEFORMAT 'YYYY-MM-DD' NULL AS 'NA' EMPTYASNULL;"
    )

  
  # Execute the copy statement:
  dbSendUpdate(conn, redshift_copy_statement)
  #if there are issues loading the table in redshift, check with:
  #dbGetQuery(conn, "select * from stl_load_errors;")
  
  
  # Insert "latest" table into permanent table:
  dbSendUpdate(conn, 
               paste0(
                 "CREATE TABLE IF NOT EXISTS ", redshift_location, " (",  table_def , 
                 "); GRANT SELECT ON ", redshift_location, 
                 " TO GROUP central_insights; ", 
                 "INSERT INTO ", redshift_location, 
                 " SELECT * FROM ", redshift_location_LATEST, ";"
               )
  )
  
  job_end_time <- Sys.time()
  print(paste0("table ", redshift_location, " written."))
  
  # update the relevant history table:
  history_table <- paste0(redshift_location, "_HISTORY")
  
  dbSendUpdate(conn,
               paste0(
                 "CREATE TABLE IF NOT EXISTS ",
                 history_table,
                 " (
                 Job                 VARCHAR(100)
                 ,Updated_By          VARCHAR(100)
                 ,Updated_Date        DATE
                 ,Started             TIMESTAMP
                 ,Finished            TIMESTAMP
               );

                 GRANT SELECT ON ", history_table,
                 " TO service_central_insights;
                 GRANT SELECT ON ", history_table,
                 " TO GROUP central_insights_server;
                 GRANT ALL ON ", history_table,
                 " TO GROUP central_insights;

                 INSERT INTO ",  history_table,
                 " VALUES ( '",
                 redshift_table,
                 "', 'service_central_insights', '",
                 Sys.Date(), "', '",
                 job_start_time, "', '",
                 job_end_time,
                 
                 "');"
  )
  )
  
  #If this runs on a pipeline then you'll need it to grant access to you.
  dbSendUpdate(conn, paste0("GRANT ALL on ", redshift_schema, ".", redshift_table, " TO vicky_banks ;") )
}

conn<- get_redshift_connection_ccog()
test_df<- dbGetQuery(conn, "SELECT * FROM s3_audience.publisher WHERE dt = 20210403 AND destination = \'PS_IPLAYER\' LIMIT 10;")

#source("/data/functions/write_to_redshift.R")
write_to_redshift(df = test_df, s3_folder = "vicky_banks", redshift_schema = "dataforce_sandbox",  redshift_table = "vb_test")

