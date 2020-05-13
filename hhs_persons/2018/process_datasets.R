# Hana Sevcikova, PSRC, 5/11/2020
# The scripts takes output from the population synthesizer (households and persons),
# and creates datasets as needed for UrbanSim run.
# For codes, see https://www2.census.gov/programs-surveys/acs/tech_docs/pums/data_dict/PUMS_Data_Dictionary_2018.pdf
# 
# Inputs: files adjusted_synthetic_households.csv, adjusted_synthetic_persons.csv
# Outputs: files households.csv and persons.csv in the directory set in output.dir

library(magrittr)
library(data.table)
hhs <- fread('adjusted_synthetic_households.csv')
pers <- fread('adjusted_synthetic_persons.csv')
output.dir <- "output"
if(!file.exists(output.dir)) dir.create(output.dir)

hhs.raw <- hhs
pers.raw <- pers

colnames(hhs) %<>% tolower
colnames(pers) %<>% tolower

# assign new person_id to cloned persons while keeping the original person_id
pers[, person_id_orig := person_id]
max_pid <- max(pers[clone == 0, person_id]) # max for non-cloned records
pers[clone == 1, person_id := (max_pid + 1):(max_pid + nrow(pers[clone == 1]))]


# select the relevant variables for households
households <- hhs[, .(household_id, puma, census_block_group_id, tenure=ten, income=as.integer(hincp), building_id=-1)] 

# get some aggregates from persons table needed in the households table
households %<>% merge(pers[,list(persons=.N, 
                                children=sum(agep < 18), 
                                workers=sum(esr %in% c(1,2,4,5))), by="household_id"], 
                      by="household_id")
households %<>% merge(subset(pers, sporder==1)[, .(household_id, agep)], by="household_id")
setnames(households, "agep", "age_of_head")

# create persons dataset
pers[is.na(esr), esr := -1]
persons <- pers[, .(person_id, household_id, sex, person_id_orig,
                        member_id = sporder,
                        age = agep,
                        earnings = as.integer(pincp),
                        edu = as.integer(schl),
                        employment_status= -1 * (esr %in% c(-1,6)) + 0*(esr==3) + as.integer(esr %in% c(1,2,4,5)) * (2*(is.na(wkhp) | wkhp < 35) + 1*(!is.na(wkhp) & wkhp >= 35)), # 0 - unemployed, 1 - full time, 2 - part time  
                        grade = as.integer(schg),
                        hours = as.integer(wkhp),
                        student = as.integer(sch %in% c(2,3)),
                        relate = relp
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

