# Hana Sevcikova, PSRC, 8/25/2020
# The scripts takes output from the population synthesizer (households and persons),
# and creates datasets as needed for UrbanSim run. It includes the characteristics of person's race.
# For codes, see https://www2.census.gov/programs-surveys/acs/tech_docs/pums/data_dict/PUMS_Data_Dictionary_2018.pdf
# 
# Inputs: files adjusted_synthetic_households.csv, adjusted_synthetic_persons.csv
# Outputs: files households.csv and persons.csv in the directory set in output.dir

library(magrittr)
library(data.table)
setwd("~/psrc/urbansim-baseyear-prep/hhs_persons/2018/with_race")
#hhs <- fread('final_adjusted_synthetic_households.csv')
#pers <- fread('final_adjusted_synthetic_persons.csv')

hhs <- fread('synthetic_households_all_pums.csv', 
             colClasses = c(block_group_id = "character", GEOID10 = "character"))
pers <- fread('synthetic_persons_all_pums.csv')
bg <- fread("census_block_groups.csv", colClasses = c(census_2010_block_group_id = "character"))

output.dir <- "output"
if(!file.exists(output.dir)) dir.create(output.dir)

#hhs.raw <- copy(hhs)
#pers.raw <- copy(pers)

colnames(hhs) %<>% tolower
colnames(pers) %<>% tolower

# assign new person_id to cloned persons while keeping the original person_id
#pers[, person_id_orig := person_id]
#max_pid <- max(pers[clone == 0, person_id]) # max for non-cloned records
#pers[clone == 1, person_id := (max_pid + 1):(max_pid + nrow(pers[clone == 1]))]
pers[, person_id := 1:nrow(pers)]
#pers[hhs, household_id := i.household_id, on = .(hh_id)]

# check if per_num has a head of household for all households and if not, fix
if(max(pers[, min(per_num), by = household_id]$V1) > 1) {
    pers[, min_per_num := min(per_num), by = household_id]
    pers[, new_per_num := per_num]
    pers[min_per_num > 1, new_per_num := per_num - min_per_num + 1]
}

# convert block group to urbansim block group
hhs[bg, census_block_group_id := i.census_block_group_id, on = c(geoid10 = "census_2010_block_group_id")]


# select the relevant variables for households
households <- hhs[, .(household_id, puma, census_block_group_id, 
                      tenure=fcase(ten %in% 1:2, 1, 
                                   ten == 3, 2, 
                                   ten > 3, 3), 
                      income=as.integer(hincp), building_id=-1)] 


# get some aggregates from persons table needed in the households table
households %<>% merge(pers[,list(persons=.N, 
                                children=sum(agep < 18), 
                                workers=sum(esr %in% c(1,2,4,5))), by="household_id"], 
                      by="household_id")
households %<>% merge(subset(pers, per_num==1)[, .(household_id, agep)], by="household_id")
setnames(households, "agep", "age_of_head")

# create persons dataset
pers[is.na(esr), esr := -1]
persons <- pers[, .(person_id, household_id, sex, #person_id_orig,
                        member_id = per_num,
                        age = agep,
                        earnings = as.integer(pincp),
                        edu = fcase(schl < 16, 1, # less than high school
                                    schl %in% 16:17, 2, # high school, GED
                                    schl == 18, 3, # some college
                                    schl == 19, 4, # vocational/no degree
                                    schl == 20, 5, # associate degree
                                    schl == 21, 6, # bachelor
                                    schl > 21, 7 # master/doctorate
                        ), 
                        employment_status= -1 * (esr %in% c(-1,6)) + 0*(esr==3) + as.integer(esr %in% c(1,2,4,5)) * (2*(is.na(wkhp) | wkhp < 35) + 1*(!is.na(wkhp) & wkhp >= 35)), # 0 - unemployed, 1 - full time, 2 - part time  
                        #grade = as.integer(schg),
                        hours = as.integer(wkhp),
                        student = as.integer(sch %in% c(2,3)),
                        relate = relp,
                        race_id = fcase(
                            rac1p == 1 & hisp < 2, 1, # white non-hisp 
                            rac1p == 2 & hisp < 2, 2, # black non-hisp 
                            rac1p == 6 & hisp < 2, 3, # asian non-hisp 
                            ! rac1p %in% c(1, 2, 6, 9) & hisp < 2, 4, # other non-hisp
                            rac1p == 9 & hisp < 2, 5, # two+ races non-hisp 
                            rac1p == 1 & hisp > 1, 6, # white hisp 
                            rac1p > 1 & hisp > 1, 7 # non-white hisp 
                            ),
                        job_id = -1
                        )
                    ]

hhs.fin <- copy(households)
pers.fin <- copy(persons)

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


fwrite(households, file=file.path(output.dir, "households.csv"))
fwrite(persons, file=file.path(output.dir, "persons.csv"))

