# Hana Sevcikova, PSRC, 3/14/2016
# The scripts takes output from the population synthesizer (households and persons),
# and creates datasets as needed for UrbanSim run.
# 
# Inputs: files households_region.csv, people_region_all_vars.csv
# Outputs: files households.csv and persons.csv in the directory set in output.dir

library(magrittr)
library(data.table)
hhs <- read.table('households_region.csv', header=TRUE, sep=',')
pers <- read.table('people_region_all_vars.csv', header=TRUE, sep=',')
output.dir <- "output"
if(!file.exists(output.dir)) dir.create(output.dir)

income_adjustment_factors <- list( "1094136" = (1.007624 * 1.08585701), 
							"1071861" = (1.018237 * 1.05266344), 
							"1041654" = (1.010207 * 1.03112956),
                                   "1024037" = (1.007549 * 1.01636470), 
							"1008425" = (1.008425 * 1.00000000)
						)
hhs.raw <- hhs
pers.raw <- pers
counties <- unique(hhs$county)
first.hh.id <- 1
for (cnty in counties) {
	idx <- which(hhs$county==cnty)
	dif.id <- first.hh.id - min(hhs$X[idx])
	hhs$X[idx] <- hhs$X[idx] + dif.id
	pidx <- which(pers$county==cnty)
	pers$hh_id[pidx] <- pers$hh_id[idx] + dif.id
	first.hh.id <- max(hhs$X[idx]) + 1
}	
# rename unnamed id to household_id
colnames(hhs)[colnames(hhs)=='X'] <- "household_id"
colnames(pers)[colnames(pers)=='X'] <- "person_id"
colnames(pers)[colnames(pers) == "hh_id"] <- "household_id"
pers[,'person_id'] <- 1:nrow(pers)
colnames(hhs) %<>% tolower
colnames(pers) %<>% tolower

# select the relevant variables from households
households <- hhs[,c('household_id', 'serialno', 'ten')]
colnames(households) <- c('household_id', 'pums_serialno', 'tenure')
households[,'household_id'] %<>% as.integer # to avoid scientific notation
households[,'pums_serialno'] %<>% as.character
households %<>% cbind(income= (hhs$hincp * unlist(income_adjustment_factors[as.character(hhs$adjinc)])) %>% round %>% as.integer)


# create a concatenated block-group-id attribute 
households %<>% cbind(census_2010_block_group_id=apply(
					data.frame(hhs$state, sprintf("%03s", hhs$county), sprintf("%06s", hhs$tract), hhs$block.group), 
											1, paste, collapse=""))
households %<>% cbind(building_id=-1)

pers.sel <- pers[,c('person_id', 'household_id', 'serialno', 'sex', 'sporder', 'agep', 'pincp', 'adjinc', 'sch', 'schl', 'schg', 'esr', "wkhp", "relp")]
# get some aggregates from persons table needed in the households table
tpers <- data.table(pers.sel)
households %<>% merge(tpers[,list(persons=.N, children=sum(agep < 18), workers=sum(! esr %in% c(0,3,6) )), by="household_id"], by="household_id")
households %<>% merge(subset(pers.sel, sporder==1)[,c('household_id', 'agep')], by="household_id")
colnames(households)[colnames(households) == "agep"] <- "age_of_head"

persons <- cbind(data.frame(pers.sel[,c('person_id', 'household_id', 'sex')],
					member_id=pers.sel$sporder,
					age=pers.sel$agep,
					earnings=(pers.sel$pincp * unlist(income_adjustment_factors[as.character(pers.sel$adjinc)])) %>% round %>% as.integer,
					edu=pers.sel$schl,
					employment_status=as.integer(!(pers.sel$esr %in% c(0,3,6))) * (2*(pers.sel$wkhp < 35) + 1*(pers.sel$wkhp >= 35)), # 0 - unemployed, 1 - full time, 2 - part time  
					grade=pers.sel$schg,
					hours=pers.sel$wkhp,
					student=as.integer(pers.sel$sch %in% c(2,3)),
					relate=pers.sel$relp
			))
persons[,'household_id'] %<>% as.integer # to avoid scientific notation
persons[,'person_id'] %<>% as.integer
persons[is.na(persons)] <- -1


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

