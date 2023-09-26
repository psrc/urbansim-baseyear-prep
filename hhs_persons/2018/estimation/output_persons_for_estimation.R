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

# connect with jobs
set.seed(1234)
jobs <- fread("urbansimBYinputs/jobs.csv")
bldgs <- fread("urbansimBYinputs/buildings.csv")
pcls <- fread("urbansimBYinputs/parcels.csv")
hhs <- fread("urbansimBYinputs/households.csv")
colnames(hhs) <- substr(colnames(hhs), 1, nchar(colnames(hhs))-3)
pers[hhs, building_id := i.building_id, on = "household_id"]
pers[pcls, `:=`(work_census_block_id = i.census_block_id, work_census_block_group_id = i.census_block_group_id, 
                work_census_tract_id = i.census_tract_id),
     on = c("work_parcel_id" = "parcel_id")]

jobs[bldgs, `:=`(parcel_id = i.parcel_id), on = "building_id"]
jobs[pcls, `:=`(census_block_id = i.census_block_id, census_block_group_id = i.census_block_group_id, 
                census_tract_id = i.census_tract_id),
        on = "parcel_id"]

jobs[, taken := FALSE]

for(pid in pers[work_parcel_id > 0, person_id]) {
    this.pers <- pers[person_id == pid]
    if(this.pers[, work_at_home] == 1){ # work-at-home should be in the same building or parcel
        availj <- jobs[!taken & building_id == this.pers[, building_id]] # select jobs from the same building
        if(nrow(availj) == 0) 
            availj <- jobs[!taken & parcel_id == this.pers[, work_parcel_id]] # select jobs from the same parcel
        if(nrow(availj) == 0) next
        if(any(availj[, home_based_status] == 1)) availj <- availj[home_based_status == 1]
    } else { # non-home-based jobs
        availj <- jobs[!taken & parcel_id == this.pers[, work_parcel_id]] # select jobs from the same parcel
        if(nrow(availj) == 0) 
            availj <- jobs[!taken & census_block_id == this.pers[, work_census_block_id]] # use census block
        if(nrow(availj) == 0) 
            availj <- jobs[!taken & census_block_group_id == this.pers[, work_census_block_group_id]] # use census block group
        if(nrow(availj) == 0) 
            availj <- jobs[!taken & census_tract_id == this.pers[, work_census_tract_id]] # use census block group
        if(nrow(availj) == 0) stop('') # to check why
        if(any(availj[, home_based_status] == 0)) availj <- availj[home_based_status == 0]
    }
    selected.job <- sample(availj[, job_id], 1)
    jobs[job_id == selected.job, taken := TRUE]
    pers[person_id == pid, job_id := selected.job]
}

# some QC
print(pers[work_parcel_id > 0, .N, by = .(job_id > 0, work_at_home)])
pers[jobs, home_based_status := i.home_based_status, on = "job_id"]
print(pers[work_parcel_id > 0, .N, by = .(job_id > 0, work_at_home, home_based_status)])
pers[, .N, by = job_id][order(N, decreasing = TRUE)] # apart from -1 should be 1 for all

# TODO: should jobs be modified by inconsistencies in location and home-based status?

column.types <- list("i4" = c("age", "edu", "employment_status", "household_id", "job_id", "member_id", #"pernum",
                                "person_id", "relate", "school_id", "sex", "race_id"),
                     "b1" = c("is_in_school", "is_worker", "student", "work_at_home"),
                     "S50" = c("school_name")
                    )

pers <- pers[, unlist(column.types), with = FALSE]


for(tp in names(column.types)){
    setnames(pers, column.types[[tp]], paste(column.types[[tp]], tp, sep = ":"))
}


fwrite(pers, file = "urbansimBYestimation/persons_for_estimation2.csv")
