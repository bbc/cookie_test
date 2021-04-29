getwd()
print(Sys.time())
################## 0. Packages and set up AWS creds ########################

get_redshift_connection_ccog <- function() {
  
  # create JDBC driver
  driver <- JDBC(driverClass = "com.amazon.redshift.jdbc.Driver",  classPath = "/usr/lib/drivers/RedshiftJDBC42-no-awssdk-1.2.41.1065.jar", identifier.quote="`")
  
  # create redshift url with username and password
  url <- stringr::str_interp("jdbc:redshift://live-idl-prod-redshift-component-redshiftcluster-1q6vyltqf8lth.ctm1v7db0ubd.eu-west-1.redshift.amazonaws.com:5439/redshiftdb?user=${Sys.getenv('REDSHIFT_USERNAME')}&password=${Sys.getenv('REDSHIFT_PASSWORD')}")
  conn <- dbConnect(driver, url)
  return(conn)
  
}

library(RJDBC)
library(lubridate)
library(tidyverse)

######################################################

### bring in the run date ###

args = commandArgs(trailingOnly=TRUE)
run_date_input_1 <- args[1]
run_date_input_2 <- args[2]
print(run_date_input_1)
print(run_date_input_2)
print(class(run_date_input_1))
print(class(run_date_input_2))

## run date format 20210427
run_date<-as.Date(run_date_input_1, format='%Y%m%d')
print(run_date)
print(class(run_date))

week_begining_monday<-floor_date(run_date, unit = 'week')+1
print(week_begining_monday)
last_monday<- floor_date(run_date %m-% weeks(1), unit = 'week')+1
print(last_monday)

## run date format 2021-04-27
run_date_2<-as.Date(run_date_input_2, format='%Y-%m-%d')
print(run_date_2)
print(class(run_date_2))

week_begining_monday<-floor_date(run_date_2, unit = 'week')+1
print(week_begining_monday)
last_monday<- floor_date(run_date_2 %m-% weeks(1), unit = 'week')+1
print(last_monday)


##### ##### Code to run ##### ##### 
startDate <- floor_date(Sys.Date() %m-% weeks(1), unit = 'week') %m+% days(1)
endDate <- ceiling_date(Sys.Date() %m-% weeks(1), 'week')
str_remove_all(startDate, '-')

print(paste0("start date is ", startDate))
print(paste0("end date is ", endDate))

sql_query<- paste0("
                  INSERT INTO dataforce_sandbox.vb_cookie_test
                   SELECT dt, visit_id, 'from_R'::varchar as date_name
                   FROM s3_audience.publisher 
                   WHERE destination = 'PS_IPLAYER' AND dt = ", 
                   str_remove_all(startDate, '-'),
                   " LIMIT 1;"
                   )
conn <- get_redshift_connection_ccog()
dbSendUpdate(conn, sql_query)



