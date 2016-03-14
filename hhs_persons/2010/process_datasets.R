# Hana Sevcikova, PSRC, 11/23/2015
# The scripts takes output from the population synthesizer (households and persons),
# joins it with the PUMs data and creates datasets as needed for UrbanSim run.
# 
# Inputs: files households2.csv, persons2.csv, pums_HH.csv, pums_person.csv
# Outputs: files households.csv and persons.csv in the directory set in output.dir

library(magrittr)
library(data.table)
hhs <- read.table('households2.csv', header=TRUE, sep=',')
pers <- read.table('persons2.csv', header=TRUE, sep=',')
output.dir <- "output"
if(!file.exists(output.dir)) dir.create(output.dir)

# rename unnamed id to household_id and adjust by one since it starts from 0
colnames(hhs)[colnames(hhs)=='X'] <- "household_id"
hhs[,'household_id'] %<>% add(1)
colnames(pers)[colnames(pers)=='X'] <- "person_id"
colnames(pers)[colnames(pers) == "hh_id"] <- "household_id"
pers[,'household_id'] %<>% add(1)
colnames(hhs) %<>% tolower
colnames(pers) %<>% tolower

# read pums
pums.hh <- read.table('pums_HH.csv', header=TRUE, sep=',')
pums.pers <- read.table('pums_person.csv', header=TRUE, sep=',')
colnames(pums.hh) %<>% tolower
colnames(pums.pers) %<>% tolower

# join hhs with pums
hhs.join <- merge(hhs, pums.hh, by='serialno')
households <- hhs.join[,c('household_id', 'serialno', 'adj_hh_income', 'ten')]
colnames(households) <- c('household_id', 'pums_serialno', 'income', 'tenure')
households[,'income'] %<>% round %>% as.integer
households[,'household_id'] %<>% as.integer # to avoid scientific notation
households[,'pums_serialno'] %<>% as.character
# create a concatenated block-group-id attribute 
households %<>% cbind(census_block_group_id=apply(
					data.frame(hhs.join$state, sprintf("%03s", hhs.join$county), sprintf("%06s", hhs.join$tract), hhs.join$block_group), 
											1, paste, collapse=""))
households %<>% cbind(building_id=-1)

# join persons with pums
pers.join <- merge(pers[,c('person_id', 'household_id', 'serialno', 'pnum', 'sex')], 
					pums.pers[,c('serialno', 'pnum', 'agep', 'pincp', 'adjinc', 'sch', 'schl', 'schg', 'esr', "wkhp", "relp")], 
				by=c('serialno', 'pnum'))

# get some aggregates from persons table needed in the households table
tpers <- data.table(pers.join)
households %<>% merge(tpers[,list(persons=.N, children=sum(agep < 18), age_of_head=max(agep)), by="household_id"])

persons <- cbind(data.frame(pers.join[,c('person_id', 'household_id', 'sex')],
					member_id=pers.join$pnum,
					age=pers.join$agep,
					earnings=round(pers.join$pinc * (pers.join$adjinc/1000000)) %>% as.integer,
					edu=pers.join$schl,
					employment_status=as.integer(!(pers.join$esr %in% c(0,3,6))) * (2*(pers.join$wkhp < 35) + 1*(pers.join$wkhp >= 35)), # 0 - unemployed, 1 - full time, 2 - part time  
					grade=pers.join$schg,
					hours=pers.join$wkhp,
					student=as.integer(pers.join$sch %in% c(2,3)),
					relate=pers.join$relp
			))
persons[,'household_id'] %<>% as.integer # to avoid scientific notation
persons[,'person_id'] %<>% as.integer

# append column types for Opus	
attr.types <- list(pums_serialno="S13", census_block_group_id="S12")
# default is integer
colnames(households)[!colnames(households) %in% names(attr.types)] %<>% paste("i4", sep=":")
colnames(persons)[!colnames(persons) %in% names(attr.types)] %<>% paste("i4", sep=":")
for (attr in names(attr.types)) {	
	if(attr %in% colnames(households))
		colnames(households)[colnames(households)==attr] %<>% paste(attr.types[[attr]], sep=":")
	if(attr %in% colnames(persons))
		colnames(persons)[colnames(persons)==attr] %<>% paste(attr.types[[attr]], sep=":")
}


write.table(households, file=file.path(output.dir, "households.csv"), sep=',', row.names=FALSE)
write.table(persons, file=file.path(output.dir, "persons.csv"), sep=',', row.names=FALSE)

