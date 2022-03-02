library(data.table)
#library(magrittr)

setwd("~/psrc/urbansim-baseyear-prep/hhs_persons/2018/estimation")

pers <- fread("output/persons_for_estimation_raw.csv")

#pers[, `:=`(job_id = NULL, school_name = NULL, school_id = NULL, is_in_school = NULL, school_category = NULL)]

#fwrite(pers, file = "output/persons_for_estimation_20210413.csv")

# connect to matched school_id implemented in data-science/HHSurvey/school_matching/schools_matching.R
pers.schools <- fread("~/psrc/data-science/HHSurvey/school_matching/matched_students_schools.csv")

pers[pers.schools, school_id := i.school_id, on = "person_id"]
pers[is.na(school_id), school_id := -1]
pers[, is_in_school := as.integer(school_id > 0)]

column.types <- list("i4" = c("age", "edu", "employment_status", "household_id", "job_id", "member_id", #"pernum",
                                "person_id", "relate", "school_id", "sex"),
                     "b1" = c("is_in_school", "is_worker", "student", "work_at_home"),
                     "S50" = c("school_name")
                    )

pers <- pers[, unlist(column.types), with = FALSE]


for(tp in names(column.types)){
    setnames(pers, column.types[[tp]], paste(column.types[[tp]], tp, sep = ":"))
}


fwrite(pers, file = "urbansimBYestimation/persons_for_estimation.csv")
