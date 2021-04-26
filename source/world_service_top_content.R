getwd()
print(Sys.time())
################## 0. Set up AWS creds ########################
options(java.parameters = "-Xmx1024m")

# Function to install package if not present. If present, load.
getPackage <- function(pkg) { 
  if (!pkg %in% installed.packages()) { 
    install.packages(pkg) 
  } else { 
    library(pkg, character.only = TRUE)
    paste(pkg, character.only=TRUE)
  }
}


get_redshift_connection_ccog <- function() {
  
  # create JDBC driver
  driver <- JDBC(driverClass = "com.amazon.redshift.jdbc.Driver",  classPath = "/usr/lib/drivers/RedshiftJDBC42-no-awssdk-1.2.41.1065.jar", identifier.quote="`")
  
  # create redshift url with username and password
  url <- stringr::str_interp("jdbc:redshift://live-idl-prod-redshift-component-redshiftcluster-1q6vyltqf8lth.ctm1v7db0ubd.eu-west-1.redshift.amazonaws.com:5439/redshiftdb?user=${Sys.getenv('REDSHIFT_USERNAME')}&password=${Sys.getenv('REDSHIFT_PASSWORD')}")
  conn <- dbConnect(driver, url)
  return(conn)
  
}

getPackage("RJDBC")
getPackage("mailR")
getPackage("xlsx")
getPackage("lubridate")
getPackage("tidyverse")

################## 0. Set up complete ########################
today <- Sys.Date()
print(Sys.time())


##### ##### 1. Set variables for SQL ##### ##### 
startDate <- floor_date(Sys.Date() %m-% weeks(1), unit = 'week') %m+% days(1)
endDate <- ceiling_date(Sys.Date() %m-% weeks(1), 'week')

print(paste0("start date is ", startDate))
print(paste0("end date is ", endDate))



####### Set SQL queries ########

# 1. top epsidoes per country
# 2. top episodes total
# 3. top TLEOS per country
# 4. top TLEOs total
# 5. number of listeners to WS SI vs SO.
# 6. number of listeners to WS SI vs SO with coutnry
# 7. how many hours of live vs od content listened split by country, SI vs SO
# 8. how many hours of live vs od content listened, SI vs SO
# 9. top 10 countries by number of listeners
# 10. top 10 countries by time spent
# 11. top 10 countries by number of listeners - every week
# 12. top 10 countries by playback time - every week


sql_query_1 <- paste0("
with top_eps AS (
SELECT most_common_master_brand AS masterbrand,
                      week_commencing,
                      country,
                      signed_in_status,
                      concatenated_title             as episode_title,
                      sum(num_plays)           as number_of_plays,
                      sum(num_accounts)        AS number_of_accounts
                      FROM radio1_sandbox.dataforce_listeners_international_top_episodes_final
                      WHERE most_common_master_brand = 'bbc_world_service'
                      AND app_type = 'All'
                      AND week_commencing = (
                      SELECT max(week_commencing) FROM radio1_sandbox.dataforce_listeners_international_top_episodes_final
                      )
                      GROUP BY 1,
                      2,
                      3,
                      4,
                      5
),
                      top_eps_ranked AS (
                      SELECT * ,
                      row_number() over (
                      partition by week_commencing,
                      country,
                      signed_in_status ORDER BY number_of_plays DESC
                      ) as rank_by_plays
                      FROM top_eps
                      )
                      SELECT *
                      FROM top_eps_ranked
                      WHERE rank_by_plays <= 10
                      ORDER BY week_commencing, country, rank_by_plays
                      
                    ")


sql_query_2 <- paste0("
with top_eps AS (
SELECT most_common_master_brand AS masterbrand,
                      week_commencing,
                      signed_in_status,
                      concatenated_title             as episode_title,
                      sum(num_plays)           as number_of_plays,
                      sum(num_accounts)        AS number_of_accounts
                      FROM radio1_sandbox.dataforce_listeners_international_top_episodes_final
                      WHERE most_common_master_brand = 'bbc_world_service'
                      AND app_type = 'All'
                      AND week_commencing = (
                      SELECT max(week_commencing) FROM radio1_sandbox.dataforce_listeners_international_top_episodes_final
                      )
                      
                      GROUP BY 1,
                      2,
                      3,
                      4
),
                      top_eps_ranked AS (
                      SELECT * ,
                      row_number() over (
                      partition by week_commencing,
                      signed_in_status ORDER BY number_of_plays DESC
                      ) as rank_by_plays
                      FROM top_eps
                      )
                      SELECT *
                      FROM top_eps_ranked
                      WHERE rank_by_plays <= 10
                      ORDER BY week_commencing, signed_in_status,  rank_by_plays
                      
                    ")

sql_query_3 <- paste0("
                      with top_tleos AS (
                      SELECT most_common_master_brand AS masterbrand,
                      week_commencing,
                      country,
                      signed_in_status,
                      tleo,
                      sum(num_plays)           as number_of_plays,
                      sum(num_accounts)        AS number_of_accounts
                      FROM radio1_sandbox.dataforce_listeners_international_top_content_final
                      WHERE most_common_master_brand = 'bbc_world_service'
                      AND app_type = 'All'
                      AND week_commencing = (
                      SELECT max(week_commencing) FROM radio1_sandbox.dataforce_listeners_international_top_content_final
                      )
                      GROUP BY 1,
                      2,
                      3,
                      4,
                      5
                      ),
                      top_tleos_ranked AS (
                      SELECT * ,
                      row_number() over (
                      partition by week_commencing,
                      country,
                      signed_in_status ORDER BY number_of_plays DESC
                      ) as rank_by_plays
                      FROM top_tleos
                      )
                      SELECT *
                      FROM top_tleos_ranked
                      WHERE rank_by_plays <= 10
                      ORDER BY week_commencing, country, rank_by_plays
                      

                      ")
sql_query_4 <- paste0("
with top_tleos AS (
SELECT most_common_master_brand AS masterbrand,
                      week_commencing,
                      signed_in_status,
                      tleo,
                      sum(num_plays)           as number_of_plays,
                      sum(num_accounts)        AS number_of_accounts
                      FROM radio1_sandbox.dataforce_listeners_international_top_content_final
                      WHERE most_common_master_brand = 'bbc_world_service'
                      AND app_type = 'All'
                      
                      AND week_commencing = (
                      SELECT max(week_commencing) FROM radio1_sandbox.dataforce_listeners_international_top_content_final
                      )
                      GROUP BY 1,
                      2,
                      3,
                      4
),
                      top_tleos_ranked AS (
                      SELECT * ,
                      row_number() over (
                      partition by week_commencing,
                      signed_in_status ORDER BY number_of_plays DESC
                      ) as rank_by_plays
                      FROM top_tleos
                      )
                      SELECT *
                      FROM top_tleos_ranked
                      WHERE rank_by_plays <= 10
                      ORDER BY week_commencing, signed_in_status,  rank_by_plays
                      
                      ")
sql_query_5 <- paste0("
SELECT master_brand_id, week_commencing, signed_in_status, sum(num_listeners) AS number_of_listeners
FROM radio1_sandbox.dataforce_listeners_international_weekly_summary
                      WHERE master_brand_id = 'bbc_world_service'
                      AND app_type = 'All'
                      AND week_commencing =
                      (
                      SELECT max(week_commencing) FROM radio1_sandbox.dataforce_listeners_international_weekly_summary
                      )
                      GROUP BY 1, 2, 3
                      ORDER BY 2, 3, 4
                      
                      ")
sql_query_6 <- paste0("
SELECT master_brand_id, week_commencing, country, signed_in_status, sum(num_listeners) AS number_of_listeners
FROM radio1_sandbox.dataforce_listeners_international_weekly_summary
                      WHERE master_brand_id = 'bbc_world_service'
                      AND app_type = 'All'
                      AND week_commencing =
                      (
                      SELECT max(week_commencing) FROM radio1_sandbox.dataforce_listeners_international_weekly_summary
                      )
                      GROUP BY 1, 2, 3, 4
                      ORDER BY 2, 3, 4, 5
                      
                      
                    ")
sql_query_7 <- paste0("
SELECT master_brand_id, week_commencing, signed_in_status, broadcast_type, round(sum(playback_time_total)::double precision /
(60 * 60), 1) as playback_time_hours
                      FROM radio1_sandbox.dataforce_listeners_international_weekly_summary
                      WHERE master_brand_id = 'bbc_world_service'
                      AND app_type = 'All'
                      AND week_commencing =
                      (
                      SELECT max(week_commencing) FROM radio1_sandbox.dataforce_listeners_international_weekly_summary
                      )
                      GROUP BY 1, 2, 3, 4
                      ORDER BY 3, 4
                      

                      ")
sql_query_8 <- paste0("
SELECT master_brand_id, week_commencing, country, signed_in_status, broadcast_type, round(sum(playback_time_total)::double precision /
(60 * 60), 1) as playback_time_hours
                      FROM radio1_sandbox.dataforce_listeners_international_weekly_summary
                      WHERE master_brand_id = 'bbc_world_service'
                      AND app_type = 'All'
                      AND week_commencing =
                      (
                      SELECT max(week_commencing) FROM radio1_sandbox.dataforce_listeners_international_weekly_summary
                      )
                      GROUP BY 1, 2, 3, 4, 5
                      ORDER BY 3, 4, 5
                      
                      ")
sql_query_9 <- paste0("
with top_by_listners AS
         (
                      SELECT master_brand_id,
                      week_commencing,
                      country,
                      sum(num_listeners) as number_of_listeners
                      FROM radio1_sandbox.dataforce_listeners_international_weekly_summary
                      WHERE master_brand_id = 'bbc_world_service'
                      AND app_type = 'All'
                      AND week_commencing =
                      (
                      SELECT max(week_commencing) FROM radio1_sandbox.dataforce_listeners_international_weekly_summary
                      )
                      GROUP BY 1,
                      2,
                      3
                      ORDER BY 3
)
SELECT * ,
row_number() over (order by number_of_listeners DESC) as rank_by_listener_numbers
FROM top_by_listners
ORDER BY rank_by_listener_numbers

                      ")

sql_query_10 <- paste0("
with top_by_time AS
         (
                      SELECT master_brand_id,
                      week_commencing,
                      country,
                      round(sum(playback_time_total)::double precision / (60 * 60), 1) as playback_time_hours
                      FROM radio1_sandbox.dataforce_listeners_international_weekly_summary
                      WHERE master_brand_id = 'bbc_world_service'
                      AND app_type = 'All'
                      AND week_commencing =
                      (
                      SELECT max(week_commencing) FROM radio1_sandbox.dataforce_listeners_international_weekly_summary
                      )
                      GROUP BY 1,
                      2,
                      3
                      ORDER BY 3
)
SELECT * ,
row_number() over (order by playback_time_hours DESC) as rank_by_time
FROM top_by_time
ORDER BY rank_by_time

                      ")

sql_query_11 <- paste0("
with top_by_listners AS
         (
                       SELECT master_brand_id,
                       week_commencing,
                       country,
                       sum(num_listeners) as number_of_listeners
                       FROM radio1_sandbox.dataforce_listeners_international_weekly_summary
                       WHERE master_brand_id = 'bbc_world_service'
                       AND app_type = 'All'
                       GROUP BY 1,
                       2,
                       3
                       ORDER BY 3
),
                       top_by_listners_ranked AS (
                       SELECT * ,
                       row_number()
                       over (partition by week_commencing order by number_of_listeners DESC) as rank_by_listener_numbers
                       FROM top_by_listners
                       )
                       SELECT *
                       FROM top_by_listners_ranked
                       WHERE rank_by_listener_numbers <= 10
                       ORDER BY rank_by_listener_numbers, week_commencing
                       
                       ")

sql_query_12 <- paste0("
with top_by_time AS
         (
                       SELECT master_brand_id,
                       week_commencing,
                       country,
                       round(sum(playback_time_total)::double precision / (60 * 60), 1) as playback_time_hours
                       FROM radio1_sandbox.dataforce_listeners_international_weekly_summary
                       WHERE master_brand_id = 'bbc_world_service'
                       AND app_type = 'All'
                       GROUP BY 1,
                       2,
                       3
                       ORDER BY 3
),
                       top_by_time_ranked AS (
                       SELECT * ,
                       row_number()
                       over (partition by week_commencing order by playback_time_hours DESC) as rank_by_playback_time
                       FROM top_by_time
                       )
                       SELECT *
                       FROM top_by_time_ranked
                       WHERE rank_by_playback_time <= 10
                       ORDER BY rank_by_playback_time, week_commencing
                       
                       ")


# Now run the query and get the result. 
# 1. top epsidoes per country
# 2. top episodes total
# 3. top TLEOS per country
# 4. top TLEOs total
# 5. number of listeners to WS SI vs SO.
# 6. number of listeners to WS SI vs SO with coutnry
# 7. how many hours of live vs od content listened split by country, SI vs SO
# 8. how many hours of live vs od content listened, SI vs SO
# 9. top 10 countries by number of listeners
# 10. top 10 countries by time spent
# 11. top 10 countries by number of listeners - every week
# 12. top 10 countries by playback time - every week
# 
conn <- get_redshift_connection_ccog() #Because there are lots of queries here and the connecion times out, re-do the connection
top_episodes_per_country <- dbGetQuery(conn, sql_query_1)
print("top_episodes_per_country - DONE")

top_episodes <- dbGetQuery(conn, sql_query_2)
print("top_episodes_per_country - DONE")

top_TLEOs_per_country <- dbGetQuery(conn, sql_query_3)
print("top_episodes- DONE")

conn <- get_redshift_connection_ccog()
top_TLEOs <- dbGetQuery(conn, sql_query_4)
print("top_TLEOs - DONE")

si_so_listeners <- dbGetQuery(conn, sql_query_5)
print("si_so_listeners - DONE")

si_so_listeners_countries <- dbGetQuery(conn, sql_query_6)
print("si_so_listeners_countries  - DONE")

conn <- get_redshift_connection_ccog()
listening_time_si_so<- dbGetQuery(conn, sql_query_7)
print("listening_time_si_so_countries - DONE")

listening_time_si_so_countries <- dbGetQuery(conn, sql_query_8)
print("listening_time_si_so - DONE")

conn <- get_redshift_connection_ccog()
top_countries_by_listeners <- dbGetQuery(conn, sql_query_9)
print("top_countries_by_listeners- DONE")

top_countries_by_listening_time <- dbGetQuery(conn, sql_query_10)
print("top_countries_by_listening_time  - DONE")

conn <- get_redshift_connection_ccog()
top_countries_weekly_listeners<- dbGetQuery(conn, sql_query_11)
print("top_countries_weekly_listeners - DONE")

top_countries_weekly_playbacktime<- dbGetQuery(conn, sql_query_12)
print("top_countries_weekly_playbacktime - DONE")



#### Manipulate the weekly data into a database for weekly top countries ####
top_countries_weekly_listeners <- top_countries_weekly_listeners %>%
  select(-number_of_listeners) %>%
  spread(key = rank_by_listener_numbers, value = country) %>%
  arrange(week_commencing)

top_countries_weekly_playbacktime<-top_countries_weekly_playbacktime %>%
  select(-playback_time_hours) %>%
  spread(key = rank_by_playback_time, value = country) %>%
  arrange(week_commencing)


##### ##### 2. Get all DF to write to different Excel sheets ##### ##### 
# a. Get a list of their names
#my_df_names <- ls()[grepl("data", ls())] #This gets df with names containing 'data' for two options use | i.e "a|b" gets att df with a or b in the name
my_df_names <-ls()[sapply(ls(), function(x) class(get(x))) == 'data.frame']
#my_df_names

print("got the list of data frame names")

#b. load the actual data frames into a list
dfList <- list()

for (i in 1:length(my_df_names)) {
  dfList[[i]] <- get(my_df_names[i])
}
names(dfList) <- my_df_names


#c. Write to an excel file
print("attempting to write excel file")
fileName<- paste0("BBC-Sounds-World-Service-",startDate,"-",endDate,".xlsx")
print(paste0("file name is ", fileName))

for(name in 1:length(dfList)){
  print(name)
  print(my_df_names[[name]])
  write.xlsx(x = as.data.frame(dfList[[name]]),
             file = fileName,
             sheetName = my_df_names[[name]],
             row.names = FALSE,
             col.name = TRUE,
             append = TRUE)
}

print("file written now sending email")
###### ##### 3. Send out filte via email ##### ##### 

messageBody<- "Hi there, <br> <br>

This is an automated email. <br>
<br>
See attached file for your regular report. If you have any questions please get in contact with myself or the Data Force team. <br>
<br>
(data.force@bbc.co.uk or on slack @help-data-force)
<br>

thanks, <br>
Vicky 
"
send.mail(from = "vicky.banks@bbc.co.uk",
          to = "vicky.banks@bbc.co.uk",#c("katherine.campbell@bbc.co.uk", "anna.doble@bbc.co.uk"),
          bcc = "vicky.banks@bbc.co.uk",
          subject = paste0("BBC Sounds International - Weekly Top Content"),
          #body = html_body,
          body = messageBody,
          encoding = "utf-8",
          html = TRUE,
          smtp = list(host.name = "localhost"),
          authenticate = F,
          send = TRUE,
          attach.files = fileName,
          file.names = fileName)


