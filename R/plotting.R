library(tidyverse)

library(lubridate)

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
