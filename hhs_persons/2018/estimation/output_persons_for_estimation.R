library(data.table)
library(magrittr)

setwd("~/psrc/urbansim-baseyear-prep/hhs_persons/2018/estimation/output")

pers <- fread("persons_for_estimation_raw.csv")

pers[, `:=`(job_id = NULL, school_name = NULL, school_id = NULL, is_in_school = NULL, school_category = NULL)]

fwrite(pers, file = "persons_for_estimation_20210413.csv")
