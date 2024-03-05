# Hana Sevcikova, PSRC, 3/05/2024
# The scripts takes output from the population synthesizer (households and persons),
# and creates datasets as needed for UrbanSim run.
# For codes, see https://www2.census.gov/programs-surveys/acs/tech_docs/pums/data_dict/PUMS_Data_Dictionary_2021.pdf
# 
# Inputs: files synthetic_households.csv, synthetic_persons.csv,
#               seed_households.csv, seed_persons.csv,
#               census_2020_block_groups.csv
# Outputs: files households_v1.csv and persons_v1.csv in the directory set in output.dir

library(magrittr)
library(data.table)

# load output of populationsim
hhs <- fread('synthpop/synthetic_households.csv')
pers <- fread('synthpop/synthetic_persons.csv')

# load pums files
hhs.seed <- fread('synthpop/seed_households.csv')
pers.seed <- fread('synthpop/seed_persons.csv')

# load Xwalk table of block groups
bgs <- fread("../../imputation/data2023/census_2020_block_groups.csv")

# where should results be stored
output.dir <- "output"

# suffix of output files
file.suffix <- "_v1"

if(!file.exists(output.dir)) dir.create(output.dir)

colnames(hhs) %<>% tolower
colnames(pers) %<>% tolower
colnames(hhs.seed) %<>% tolower
colnames(pers.seed) %<>% tolower

# assign new person_id
pers[, person_id := 1:nrow(pers)]

# select the relevant variables for households
households <- hhs[, .(household_id, puma, census_2020_block_group_geoid = block_group_id, tenure=ten, 
                      income=as.integer(hincp), building_id=-1)] 

# join with block group ids and keep only the urbansim BG id
households <- merge(households, bgs, by = "census_2020_block_group_geoid")[, census_2020_block_group_geoid := NULL]

# since we have a few fake parcels in JBLM that might not match the OFM BGs, we put the households into those BGs
swaps <- list("530530729072" = "530530729073", "530530729082" = "530530729083")
for(swapbg in names(swaps)){
    bg1 <- bgs[census_2020_block_group_geoid == swapbg, census_2020_block_group_id]
    bg2 <- bgs[census_2020_block_group_geoid == swaps[[swapbg]], census_2020_block_group_id]
    households[census_2020_block_group_id == bg1, census_2020_block_group_id := bg2]
}

# get some aggregates from persons table needed in the households table
households %<>% merge(pers[,list(persons = .N, 
                                children = sum(agep < 18), 
                                workers = sum(is_worker)), by = "household_id"], 
                      by="household_id")

# assign age of head by taking the age of the first person in each HH
households %<>% merge(subset(pers, per_num==1)[, .(household_id, agep)], by="household_id")
setnames(households, "agep", "age_of_head")

# merge synthetic persons with seed persons to get all the attributes
pers <- merge(pers, pers.seed[, .(hh_id = hhnum, per_num = sporder, esr, pincp, schl, wkhp, schg, relshipp)], 
              by = c("hh_id", "per_num"))
# create persons dataset
pers[is.na(esr), esr := -1]
persons <- pers[, .(person_id, household_id, sex, 
                        member_id = per_num,
                        age = agep,
                        earnings = as.integer(pincp),
                        edu = as.integer(schl),
                        employment_status= -1 * (esr %in% c(-1,6)) + 0*(esr==3) + as.integer(esr %in% c(1,2,4,5)) * (2*(is.na(wkhp) | wkhp < 35) + 1*(!is.na(wkhp) & wkhp >= 35)), # 0 - unemployed, 1 - full time, 2 - part time  
                        grade = as.integer(schg),
                        hours = as.integer(wkhp),
                        student = as.integer(sch %in% c(2,3)),
                        relate = relshipp,
                        race_id = rac1p 
                        )
                    ]

hhs.fin <- copy(households)
pers.fin <- copy(persons)

# append column types for Opus	
attr.types <- list(census_2020_block_group_geoid="S12") # not used as this column was deleted
# default is integer
colnames(households)[!colnames(households) %in% names(attr.types)] %<>% paste("i4", sep=":")
colnames(persons)[!colnames(persons) %in% names(attr.types)] %<>% paste("i4", sep=":")
for (attr in names(attr.types)) {	
	if(attr %in% colnames(households))
		colnames(households)[colnames(households)==attr] %<>% paste(attr.types[[attr]], sep=":")
	if(attr %in% colnames(persons))
		colnames(persons)[colnames(persons)==attr] %<>% paste(attr.types[[attr]], sep=":")
}

# save into csv files
fwrite(households, file=file.path(output.dir, paste0("households", file.suffix, ".csv")))
fwrite(persons, file=file.path(output.dir, paste0("persons", file.suffix, ".csv")))

