library(magrittr)

# load data 
lodes <- read.table("LODES10.txt", sep=",", header=TRUE)
lodes$Block <- as.character(lodes$Block)
#wac <- read.table("WAC.txt", sep=",", header=TRUE)
#wac$WACnum <- as.numeric(substr(wac$WAC, 4,5))
#sectors <- read.table("employment_sectors.csv", sep=",", header=TRUE)
# join LODES with PSEF codes
wac.psef <- read.table("WAC_PSEF.txt", sep=",", header=TRUE)
lodes <- merge(lodes, wac.psef[,c("WAC", "PSEF")], by="WAC", all=TRUE)
colnames(lodes)[colnames(lodes)=="PSEF"] <- "sector_id"
# replace census block id with an internal id from a lookup table
blocks <- read.table("census_blocks.txt", sep="\t", header=TRUE)
blocks$census_2010_block_id <- as.character(blocks$census_2010_block_id)
lodes <- merge(lodes, blocks[,c('census_block_id', 'census_2010_block_id')], by.x='Block', by.y='census_2010_block_id', all.x=TRUE)


# unroll into individual jobs
ublocks <- unique(lodes$census_block_id) %>% na.omit
append <- FALSE
jid <- 1
for(bl in ublocks) {
	recs <- subset(lodes, census_block_id==bl)
	njobs <- sum(recs$Jobs10)
	jobs <- data.frame(job_id=jid:(jid+njobs-1), 
						sector_id=rep(recs$sector_id, recs$Jobs10), 
						home_based_status=0, building_id=-1,
						census_block_id=bl)
	colnames(jobs) %>% paste0(":i4")
	write.table(jobs, "jobs.csv", sep=",", row.names=FALSE, quote=FALSE, append=append, col.names=!append)
	append <- TRUE
	jid <- jid+njobs
}
