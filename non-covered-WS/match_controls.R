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
	res.distr <- ipf(margins, table.ini, maxiter=100, closure=0.01)
	if(!select.jobs) next
	
	# Sample results
	# for rounding use a version of the truncate-replicate-sample method (Lovelace,  Ballas, 2013)
	distr.trunc <- trunc(res.distr) # truncated results
	distr.rmd <- res.distr - distr.trunc # remainders; used as probabilities
	# iterate over sectors
	for(fsec in 1:nrow(sects)) {
		if(sects$SectorJobs[fsec] == 0) next
		secid <- as.character(sects$Sector_Code[fsec])
		# iterate over CPs
		for(cp in 1:nrow(CPs)) {
			if(res.distr[cp,fsec] == 0 || CPs$Estimate[cp] == 0) next
			cpid <- as.character(CPs$CP_id[cp])
			jobs.sec.cp <- job.records %>% subset(Sector_Code == secid & CP_id == cpid)
			if(nrow(jobs.sec.cp) == 0) {
				warning('No records available for sampling ', res.distr[cp,fsec], ' jobs in county ', county, ', CP ', cpid, ', sector ', secid)
				next
			}
			job.keys <- jobs.sec.cp$Key %>% as.character
			if(nrow(jobs.sec.cp) == distr.trunc[cp,fsec]) { # the truncated cell matches the number of records
				jobs[job.keys,'selected'] %<>% add(1)
				rmd.keys <- job.keys
			} else {
				if(nrow(jobs.sec.cp) > distr.trunc[cp,fsec]) { # more records available; sample 
					sampled.keys <- sample(job.keys, distr.trunc[cp,fsec])
					jobs[sampled.keys,'selected']  %<>% add(1)
					rmd.keys <- job.keys[!(job.keys %in% sampled.keys)]
				} else { # less records available; sample with replacement
					# repeat the key array so that enough records can be sampled
					keys <- rep(job.keys, ceiling(res.distr[cp,fsec]/nrow(jobs.sec.cp)))
					sampled.keys <- sample(keys, distr.trunc[cp,fsec]) 
					freq <- table(sampled.keys)
					jobs[names(freq),'selected'] %<>% add(freq)
					rmd.keys <- keys			
				}
			}
			# remainder
			if(distr.rmd[cp,fsec]==0) next
			add.one <- sample(c(FALSE,TRUE), 1, prob=c(1-distr.rmd[cp,fsec], distr.rmd[cp,fsec])) # should one be added or not
			if(add.one) {
				sampled.key <- sample(rmd.keys, 1)
				jobs[sampled.key,'selected']  %<>% add(1)
			}
		}
	}
}
if(select.jobs) write.table(jobs, file="tblOutput.csv", sep=',', row.names=FALSE)
cat('\nTotals: ')
df <- data.frame(jobs=sum(jobs$selected), CPs=sum(CPsums$Estimate), Sectors=sum(sectors$SectorJobs))
print(df)
