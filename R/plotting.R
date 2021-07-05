install.packages("RPostgreSQL")

library(tidyverse)

library(DBI)

library(RPostgreSQL)

library(lubridate)

#DBI::dbDriver('PostgreSQL')
#require(RPostgreSQL)
#drv=dbDriver("PostgreSQL")
#con=dbConnect(drv,
#              dbname="real_estate",
#              host="localhost",
#              port=5432,
#              user="postgres",
#              password="admin")
#
#dbListTables(conn = con)
#
#x <- RPostgreSQL::dbGetQuery(conn = con,
#                        statement = 'select * from holmes limit 10;')
#
#Encoding(colnames(x)) <- "UTF-8"
#
#Sys.setlocale(category = "LC_COLLATE", locale = "Bulgarian")
#
#Sys.setlocale("LC_CTYPE", "bulgarian")
#
#Sys.setlocale("bg_BG")

setwd("D:/git/bi_challenge")
list.files('.')

acc <- read_csv("Accident_Information.csv")
veh <- read_csv("Vehicle_Information.csv")

acc_subset <- acc %>%
  select(Accident_Index, Accident_Severity, Date, Latitude, Longitude, Light_Conditions, Number_of_Casualties, Number_of_Vehicles, Road_Surface_Conditions, Road_Type, Speed_limit, Time, Urban_or_Rural_Area, Weather_Conditions) %>%
  mutate(date_month = round_date(Date, "month")) %>%
  select(-Time)

acc_subset %>%
  group_by(date_month) %>%
  summarize(casualties = sum(Number_of_Casualties)) %>%
  ggplot(aes(x = date_month, y=casualties)) + geom_line()
