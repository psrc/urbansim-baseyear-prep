library(magrittr)
source('ipf.R')
select.jobs <- TRUE # should sampling of job records happen
# load input tables
# ACS marginals
CPsums <- read.table('CPSum10.csv', header=TRUE, sep=',')
colnames(CPsums)[colnames(CPsums)=="CDP_id"] <- "CP_id"
sectors <- read.table('FIPSector10.csv', header=TRUE, sep=',')
# job records from the business license database
# should have columns: "Key","FIPS","CPID","ACS_code"
jobs <- read.table('tblMain.csv', header=TRUE, sep=',')
colnames(jobs)[colnames(jobs)=="ACS_code"] <- "Sector_Code"
colnames(jobs)[colnames(jobs)=="CPID"] <- "CP_id"

# all CP_id values in the jobs table that are not included in the ACS tables
# are renamed to "*rmd" (remainder) where * is the county number
fips <- sectors$FIPS %>% unique
for(county in fips) {
	CPs <- CPsums %>% subset(FIPS==county)  %>% extract(,'CP_id') %>%  as.character
	not.in.acs <- jobs$FIPS == county & !(jobs$CP_id %in% CPs)	 
	fipsrmd <- CPs[grep(pattern="rmd$", x=CPs)]
	jobs[which(not.in.acs), 'CP_id'] <- fipsrmd
}
# add column for counting the number of being selected
jobs %<>% cbind(selected=0)
rownames(jobs) <- jobs$Key

# RNG to ensure reproducibility
set.seed(100)

# iterate over FIPS
for(county in fips) {
	cat('\nCounty: ', county, ' ')
	# subset 
	sects <- sectors %>% subset(FIPS==county)
	CPs <- CPsums %>% subset(FIPS==county)
	job.records <- jobs %>% subset(FIPS == county) 
	# create a two-way table and arrays of marginals
	table.ini <- matrix(0, nrow=nrow(CPs), ncol=nrow(sects), dimnames=list(as.character(CPs$CP_id), as.character(sects$Sector_Code)))
	margins <- list(CPs$Estimate, sects$SectorJobs)
	table.tmp <- table(as.character(job.records$CP_id), as.character(job.records$Sector_Code))
	# match the margins ordering
	table.ini[rownames(table.tmp), colnames(table.tmp)] <- table.tmp
	zero.sect <- which(colSums(table.ini) == 0)
	if(length(zero.sect) > 0 && margins[[2]][zero.sect] > 0) { # impute initial values
		table.ini[,zero.sect] <- margins[[2]][zero.sect]/nrow(table.ini)
		cat("\nZero sectors with non-zero margins:")
		print(sects[zero.sect,])
	}
	
	# Run IPF
	res.distr <- ipf(margins, table.ini, maxiter=500, closure=0.01)
	if(!select.jobs) next
	
	# Sample results	
	# iterate over sectors
	for(fsec in 1:nrow(sects)) {
		secid <- as.character(sects$Sector_Code[fsec])
		# iterate over CPs
		for(cp in 1:nrow(CPs)) {
			cpid <- as.character(CPs$CP_id[cp])
			jobs.sec.cp <- job.records %>% subset(Sector_Code == secid & CP_id == cpid)
			if(res.distr[cp,fsec] == 0) next
			if(nrow(jobs.sec.cp) == res.distr[cp,fsec]) { # the cell matches the number of records
				jobs[jobs.sec.cp$Key %>% as.character,'selected'] <- jobs[jobs.sec.cp$Key %>% as.character,'selected'] + 1
				next
			}
			if(nrow(jobs.sec.cp) > res.distr[cp,fsec]) { # more records available; sample 
				sampled.keys <- sample(jobs.sec.cp$Key, res.distr[cp,fsec]) %>% as.character
				jobs[sampled.keys,'selected'] <- jobs[sampled.keys,'selected'] + 1
			} else { # less records available; sample with replacement
				if(nrow(jobs.sec.cp) > 0) {
					# repeat the key array so that enough records can be sampled
					keys <- rep(jobs.sec.cp$Key, ceiling(res.distr[cp,fsec]/nrow(jobs.sec.cp)))
					sampled.keys <- sample(keys, res.distr[cp,fsec]) %>% as.character
					freq <- table(sampled.keys)
					jobs[names(freq),'selected'] <- jobs[names(freq),'selected'] + freq
				} else { # no records available
					warning('No records available for sampling ', res.distr[cp,fsec], ' jobs in county ', county, ', CP ', cpid, ', sector ', secid)
				}
			}
		}
	}
}
if(select.jobs) write.table(jobs, file="tblOutput.csv", sep=',', row.names=FALSE)
cat('\n')