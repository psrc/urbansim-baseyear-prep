# Post-process households and persons from the travel survey for the purpose of urbansim estimation.
# Given parcel_id, find an appropriate building for household's current and previous location.
# Find records in the synthetic dataset that are similar to the estimation records and replace them 
# by the survey records.
# As input it uses the output of Brice's python script
# https://github.com/psrc/travel-studies/blob/master/2017/daysim_conversions/locate_parcels.py
# (files 1_household.csv, 2_person.csv)
#
# Hana Sevcikova, PSRC
# February 2024

library(data.table)
library(magrittr)

setwd("~/psrc/urbansim-baseyear-prep/hhs_persons/2023/estimation")

output.dir <- "output"
if(!file.exists(output.dir)) dir.create(output.dir)

set.seed(123)
bld <- fread("../../../imputation/data2023/buildings_imputed_phase3_lodes_20240226.csv") # latest buildings dataset
hhs.rawsurvey <- fread(file.path("surveydata", "raw", "1_household.csv")) 
pers.rawsurvey <- fread(file.path("surveydata", "raw", "2_person.csv"))
hhs <- fread(file.path("../synthpop", "synthetic_households.csv")) # non-parcelized synthetic households
pers <- fread(file.path("../synthpop", "synthetic_persons.csv")) # synthetic set of persons

colnames(hhs) <- tolower(colnames(hhs))
colnames(pers) <- tolower(colnames(pers))

# create the various estimation columns
hhsest <- hhs.rawsurvey[!is.na(hhparcel), .(hhid = hhno, #puma = final_home_puma10,
                         parcel_id = hhparcel,
                         #previous_parcel_id = prev_home_parcel,
                         workers = hhwkrs, 
                         tenure = hownrent,
                         income_cat = hhincome, 
                         persons = hhsize,
                         #move = ifelse(!is.na(prev_home_wa) & prev_home_wa %in% c(1,2), 1, 0),
                         sampling_weight = hhexpfac
                         )]
hhsest[tenure == 5, tenure := -1]
hhsest[tenure > 2, tenure := 3]

# assign income by sampling from income of the synthetic households of the corresponding category
income.categories <- c(1000, 10000, 25000, 35000, 50000, 75000, 100000, 
                      150000, 200000, 250000, max(hhs$hincp) + 1)
for(icat in 1:(length(income.categories)-1)){
    income.values <- hhs[hincp >= income.categories[icat] & hincp < income.categories[icat+1], hincp]
    hhsest[income_cat == icat, hincp := sample(income.values, nrow(hhsest[income_cat == icat]), replace = TRUE)]
}
hhsest[income_cat == 11, income := -1][, income_cat := NULL]

# assign age of head and number of children
hhsest[pers.rawsurvey[pernum == 1], age_of_head := i.age, on = "hhid"]
hhsest[pers.rawsurvey[, .(children=sum(age < 18)), by = "hhid"], 
       children := i.children, on = "hhid"]

# determine the set of buildings that are candidates for residential location
resbld <- bld[residential_units > 0]

hhbld <- hhsest[, .N, by = parcel_id]
hhbld[resbld[, .N, by = parcel_id], nbld := i.N, on = "parcel_id"]

# assign buildings
# first for parcels with just one building
hhbld1 <- hhbld[nbld == 1]
hhbld1[resbld, `:=`(building_id = i.building_id, 
                              building_type_id = i.building_type_id), on = "parcel_id"]
hhsest[hhbld1, building_id := i.building_id, on = "parcel_id"]

# if there are more buildings, sample randomly
# with of without replacement depending if there are more HHs than buildings or not
hhbld2 <- hhbld[nbld > 1]
for(i in 1:nrow(hhbld2)) {
    replace <- FALSE
    if(hhbld2[i, N] > hhbld2[i, nbld]) replace <- TRUE
    hhsest[parcel_id == hhbld2$parcel_id[i], 
               building_id := sample(resbld[parcel_id == hhbld2$parcel_id[i], building_id], 
                                     hhbld2[i, N], replace = replace)]
}
hhsest[is.na(building_id), building_id := -1]

# assign buildings for previous location
# first for parcels with just one building
hhbldprev <- hhsest[!is.na(previous_parcel_id) & parcel_id != previous_parcel_id, .N, by = previous_parcel_id]
hhbldprev[resbld[, .N, by = parcel_id], nbld := i.N, on = c(previous_parcel_id="parcel_id")]
hhbld1 <- hhbldprev[nbld == 1]
hhbld1[resbld, `:=`(building_id = i.building_id, 
                    building_type_id = i.building_type_id), 
       on = c(previous_parcel_id="parcel_id")]
hhsest[hhbld1, previous_building_id := i.building_id, on = "previous_parcel_id"]

# if there are more buildings, sample randomly
# with of without replacement depending if there are more HHs than buildings or not
hhbld2 <- hhbldprev[nbld > 1]
for(i in 1:nrow(hhbld2)) {
    replace <- FALSE
    if(hhbld2[i, N] > hhbld2[i, nbld]) replace <- TRUE
    hhsest[previous_parcel_id == hhbld2$previous_parcel_id[i], 
           previous_building_id := sample(resbld[parcel_id == hhbld2$previous_parcel_id[i], building_id], 
                                 hhbld2[i, N], replace = replace)]
}
hhsest[is.na(previous_building_id), previous_building_id := -1]
hhsest[, is_inmigrant := ifelse(previous_building_id <= 0, 1, 0)]


# identify households similar to the records in the survey within their respective census block groups
# assign block group
pcl <- fread("parcels.csv") # latest parcels dataset
hhsest[pcl, census_block_group_id := i.census_block_group_id, on = "parcel_id"]

# iterate over hhs
selected <- rep(FALSE, nrow(hhs))
cat("\n")
for (i in 1:nrow(hhsest)) {
    cat("\rProcessing ", round(i/nrow(hhsest)*100), "%")
    hhbg <- hhs[census_block_group_id == hhsest[i, census_block_group_id] & !selected]
    # First, match by HH size
    hhsel <- hhbg[persons == hhsest[i, persons]]
    if(nrow(hhsel) == 0 && hhsest[i, persons] > 6){
        hhsel <- hhbg[persons > 6]
    }
    if(nrow(hhsel) == 0){ # if there is no match, take those with the minimum difference
        mindif <- hhbg[, min(abs(persons - hhsest[i, persons]))]
        hhsel <- hhbg[persons %between% c(hhsest[i, persons] - mindif, hhsest[i, persons] + mindif)]
    }
    # Second, match by number of workers
    if(nrow(hhsel) > 1) {
        hhsel2 <- hhsel[workers == hhsest[i, workers]]
        if(nrow(hhsel2) == 0){
            mindif <- hhsel[, min(abs(workers - hhsest[i, workers]))]
            hhsel2 <- hhsel[workers %between% c(hhsest[i, workers] - mindif, hhsest[i, workers] + mindif)]
        }
        hhsel <- hhsel2
        # Third, match by age of head
        if(nrow(hhsel) > 1) {
            age.dif <- hhsel[, abs(age_of_head - hhsest[i, age_of_head])]
            hhsel <- hhsel[which.min(age.dif)[1]]
        }
    }
    if(nrow(hhsel) != 1) stop('')
    selected[which(hhs$household_id == hhsel$household_id[1])] <- TRUE
    # set household_id in the survey dataset to the one of the matched synthetic HH
    hhsest[i, household_id := hhsel$household_id[1]]
}
cat("\n")


# create a dataset of survey persons to be replaced in the synthetic dataset
# race-related attributes
for(r in c("race_afam", "race_aiak", "race_asian", "race_hapi", "race_hisp", 
           "race_white", "race_other")) {
    pers.rawsurvey[is.na(pers.rawsurvey[[r]]) | pers.rawsurvey[[r]] > 900, (r) := 0]
}
pers.rawsurvey[, nrace := race_afam + race_aiak + race_asian + race_hapi + race_white + race_other]
pers.rawsurvey[nrace == 0 & race_hisp == 1, `:=`(race_white = 1, nrace = 1)]
pers.rawsurvey[is.na(nrace), nrace := 0]
pers.rawsurvey[, id := 1:nrow(pers.rawsurvey)]

persest <- pers.rawsurvey[, .(id = id, hhid = hhid, member_id = pernum,
                               relate = relationship, 
                               age_cat = age,
                               sex = gender, 
                               earnings = as.integer(0), 
                               employment_status = fcase(employment %in% c(1), 1, 
                                                         employment == 2, 2,
                                                         ! employment %in% 1:2, 0),
                               student = ifelse(student %in% 2:3, 1, 0),
                               school_parcel_id = school_loc_parcel,
                              school_name_from_survey = tolower(school_loc_name),
                              school_address_from_survey = school_loc_address,
                              school_lng_from_survey = school_loc_lng,
                              school_lat_from_survey = school_loc_lat,
                              school_parcel_distance = school_loc_parcel_distance,
                               work_parcel_id = work_parcel,
                               edu = education, hours = hours_work,
                               race_id = fcase(nrace < 2 &  race_white == 1 & race_hisp == 0, 1,
                                            nrace < 2 &  race_afam == 1 & race_hisp == 0, 2,
                                            nrace < 2 &  race_asian == 1 & race_hisp == 0, 3,
                                            nrace < 2 & (race_aiak == 1 |  race_hapi == 1 | race_other == 1) & race_hisp == 0, 4,
                                            nrace > 1 & race_hisp == 0, 5,
                                            race_hisp == 1 & race_white == 1, 6,
                                            race_hisp == 1 & race_white == 0, 7,
                                            nrace == 0, -1)
                            )
                           ][hhid %in% hhsest$hhid]

# set age by sampling from ages of the synthetic population of the corresponding category
age.categories <- c(1, 5, 12, 16, 18, 25, 35, 
                       45, 55, 65, 75, 85, max(pers$age) + 1)
for(icat in 1:(length(age.categories)-1)){
    age.values <- pers[age >= age.categories[icat] & age < age.categories[icat+1], age]
    persest[age_cat == icat, age := sample(age.values, nrow(persest[age_cat == icat]), replace = TRUE)]
}
# deal with missing values
persest[relate > 90, relate := -1]
persest[hours > 900 | is.na(hours), hours := -1]
persest[edu > 900 | is.na(edu), edu := -1]
persest[sex > 2, sex := -1]

# work-related stuff
persest[, is_worker := as.integer(employment_status > 0)]
persest[hhsest, home_parcel_id := i.parcel_id, on = .(hhid)]
persest[, work_at_home := as.integer(work_parcel_id == home_parcel_id)]
persest[, job_id := -1]

# school-related stuff
schools <- fread("schools.csv")
schools[, sname := tolower(sname)]
persest[schools, `:=`(school_id = i.school_id, school_name = tolower(i.sname), school_category = i.category), on = c(school_parcel_id = "parcel_id")]
persest[school_id > 0, is_in_school := 1] # correct missing values from the survey
tmp <- persest[school_parcel_id > 0]
tmp2 <- tmp[school_name != school_name_from_survey & school_name_from_survey != ""]
tmp2[school_name_from_survey %in% schools$sname, .(school_id, school_parcel_id, school_name, school_name_from_survey, age, school_category, school_parcel_distance)]
# match non-matches
# as.data.frame(tmp[school_name != school_name_from_survey & school_name_from_survey != "", .(school_id, school_name, school_name_from_survey, age, school_category, school_parcel_distance)][order(school_parcel_distance, decreasing = TRUE)])


# correct some mismatches
persest[id %in% tmp2[school_name_from_survey == "holy rosary school"]$id, school_id := schools[sname == "holy rosary school" & tolower(scity) == "seattle"]$school_id]
persest[id %in% tmp2[school_name_from_survey == "concordia lutheran school"]$id, school_id := schools[sname == "concordia lutheran school" & tolower(scity) == "tacoma"]$school_id]

snames <- c("hawthorne elementary school", "tacoma community college", "beacon hill elementary school", "westside school", 
            "john stanford international school", "sakai intermediate school", "west woodland elementary")
for(name in snames)
    persest[id %in% tmp2[school_name_from_survey == name]$id & age > 5, school_id := schools[sname == name & student_count > 0]$school_id]

persest[id %in% tmp[school_name_from_survey == "boeing factory"]$id, school_id := NA]
persest[id %in% tmp[school_name_from_survey == "fred hutchinson cancer research center"]$id, school_id := NA]
persest[schools, `:=`(school_name = i.sname, school_category = i.category), on = "school_id"]
#persest[is.na(school_id), school_id := -1]
#persest[is.na(is_in_school), is_in_school := 0]
#stop('')

# replace persons
to.remove <- pers[household_id %in% hhsest$household_id, person_id]
if(length(to.remove) < nrow(persest)) stop("Removing less persons than in the estimation set.")
persest[, person_id := to.remove[1:nrow(persest)]] # only works if persest is smaller than what we remove
persest[hhsest, household_id := i.household_id, on = .(hhid)]

# put datasets together
#hhsest2 <- copy(hhsest)[, `:=`(puma = -1, hhno = NULL, parcel_id = NULL)]
hhsest2 <- hhsest[, colnames(hhs), with = FALSE]
households <- hhs[!household_id %in% hhsest$household_id]
households <- rbind(households, hhsest2)
households <- households[order(household_id)]

persest2 <- copy(persest)[, `:=`(hhid = NULL)]
persest2 <- persest[, colnames(pers), with = FALSE]
persons <- pers[!person_id %in% to.remove]
persons <- rbind(persons, persest2)
persons <- persons[order(person_id)]

# append column types for Opus	
attr.types <- list(pums_serialno="S13", census_2010_block_group_id="S12")
# default is integer
colnames(households)[!colnames(households) %in% names(attr.types)] %<>% paste("i4", sep=":")
colnames(persons)[!colnames(persons) %in% names(attr.types)] %<>% paste("i4", sep=":")
for (attr in names(attr.types)) {	
    if(attr %in% colnames(households))
        colnames(households)[colnames(households)==attr] %<>% paste(attr.types[[attr]], sep=":")
    if(attr %in% colnames(persons))
        colnames(persons)[colnames(persons)==attr] %<>% paste(attr.types[[attr]], sep=":")
}

# write output
fwrite(households, file=file.path(output.dir, "households.csv"))
fwrite(persons, file=file.path(output.dir, "persons.csv"))

fwrite(hhsest, file=file.path(output.dir, "households_for_estimation_raw.csv"))
fwrite(persest, file=file.path(output.dir, "persons_for_estimation_raw.csv"))



