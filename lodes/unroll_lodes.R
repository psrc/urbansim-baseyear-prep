library(magrittr)

# load data
#lodes.file <- 'LODES10.txt'
lodes.file <- 'AdjLODES14.txt'

lodes <- read.table(lodes.file, sep=",", header=TRUE, stringsAsFactors=FALSE)

#wac <- read.table("WAC.txt", sep=",", header=TRUE)
#wac$WACnum <- as.numeric(substr(wac$WAC, 4,5))
#sectors <- read.table("employment_sectors.csv", sep=",", header=TRUE)

# function for trimming leading and trailing spaces
trim <- function (x) gsub("^\\s+|\\s+$", "", x)

if(!('sector_id' %in% colnames(lodes))) {
	# join LODES with PSEF codes
	wac.psef <- read.table("WAC_PSEF.txt", sep=",", header=TRUE)
	lodes <- merge(lodes, wac.psef[,c("WAC", "PSEF")], by="WAC", all=TRUE)
	colnames(lodes)[colnames(lodes)=="PSEF"] <- "sector_id"
} 
lodes$sector_id <- as.integer(lodes$sector_id)
lodes$block_id <- trim(lodes$block_id)

# replace census block id with an internal id from a lookup table
blocks <- read.table("census_blocks.tab", sep="\t", header=TRUE, stringsAsFactors=FALSE)
lodes <- merge(lodes, blocks[,c('census_block_id', 'census_2010_block_id', 'census_block_group_id')], by.x='block_id', by.y='census_2010_block_id', all.x=TRUE)
lodes <- cbind(id=1:nrow(lodes), lodes)

lodes.missing <- subset(lodes, is.na(census_block_id))
cat("\n\nMissing: #blocks: ", length(unique(lodes.missing$block_id)), ", # jobs involved: ", sum(lodes.missing$number_of_jobs), "\n")
#write.table(lodes.missing$block_id, file="missing_blocks20160412.txt", sep='\n', row.names=FALSE, col.names=FALSE, quote=FALSE)

# deal with negatives
neg <- subset(lodes, number_of_jobs < 0)
geo.hier <- list(block_group=14, tract=11, county=5, state=2)
log.distr <- log.distr.counter <- rep(0, length(geo.hier))
names(log.distr) <- names(log.distr.counter) <- names(geo.hier)

for(i in 1:nrow(neg)) {
	to.remove <- abs(neg$number_of_jobs[i])
	lodes.neg.idx <- which(lodes$id == neg$id[i])
	# iterate over geography
	for(geo in names(geo.hier)) {
		subs <- subset(lodes, substr(block_id, 1, geo.hier[[geo]]) == substr(neg$block_id[i], 1, geo.hier[[geo]]) & sector_id == neg$sector_id[i] & number_of_jobs > 0)
		if(nrow(subs) > 0) {
			ids.to.select <- rep(subs$id, subs$number_of_jobs)
			selected <- sample(1:length(ids.to.select), min(to.remove, sum(subs$number_of_jobs)))
			tab <- table(ids.to.select[selected])
			tab <- tab[order(as.integer(names(tab)))]
			lodes.idx <- which(lodes$id %in% names(tab))
			lodes[lodes.idx,'number_of_jobs'] <- lodes[lodes.idx,'number_of_jobs'] - tab
			stab <- sum(tab)		
			lodes[lodes.neg.idx, 'number_of_jobs'] <- lodes[lodes.neg.idx, 'number_of_jobs'] + stab
			to.remove <- to.remove - stab
			log.distr[geo] <- log.distr[geo] + stab
			log.distr.counter[geo] <- log.distr.counter[geo] + 1
			if(to.remove <= 0) break
		}
	}
}
logres <- data.frame(number_of_jobs=log.distr, number_of_blocks=log.distr.counter)
rownames(logres) <- names(log.distr)
cat("\nNegatives redistributed as follows:\n")
print(logres)

# unroll into individual jobs
lodes.pos <- subset(lodes, number_of_jobs > 0)
ublocks <- unique(lodes.pos$census_block_id) %>% na.omit
append <- FALSE
jid <- 1
for(bl in ublocks) {
	recs <- subset(lodes.pos, census_block_id==bl)
	njobs <- sum(recs$number_of_jobs)
	jobs <- data.frame(job_id=jid:(jid+njobs-1), 
						sector_id=rep(recs$sector_id, recs$number_of_jobs), 
						home_based_status=0, building_id=-1,
						census_block_id=bl)
	if(!append) colnames(jobs) <- c('job_id:i4', 'sector_id:i1', 'home_based_status:b1', 'building_id:i4', 'census_block_id:i4')
	write.table(jobs, "jobs.csv", sep=",", row.names=FALSE, quote=FALSE, append=append, col.names=!append)
	append <- TRUE
	jid <- jid+njobs
}

# convert it from command line into Opus cache:
# python -m opus_core.tools.convert_table csv flt -d . -o /path/to/opus/cache/2014 -t jobs
