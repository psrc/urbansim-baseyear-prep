# Hana Sevcikova, PSRC
# 11/26/2018
# The script matches residential units to 2017 OFM counts.
# Inputs: 
#	data.year - which year all the data files correspond to
#	The following files are expected to be in the directory "data{data.year}"
#	file buildings.csv (modelled 2017 buildings)
#	file of DU totals - it is read via the file data{data.year}/read_hh_totals.R
#			where the name of the file is set,
#			e.g. households2.csv = output of synthpop, or an OFM dataset.
#	file parcels.csv (base year 2014 parcels dataset)
# Outputs:
#	All output files are written into "data{data.year}".
#	file imputed_buildings_matched.csv - It has adjusted residential_units.
#	file imputed_buildings_matched_for_opus.csv - as above but has some parcels attributes removed. 
#		The latter can be used to convert to an opus cache using:
#		python -m opus_core.tools.convert_table csv flt -o ~/opus_cache/2010 -d . -t imputed_buildings_matched_for_opus			

library(data.table)
library(magrittr)

data.year <- 2017 # data files will be taken from "data{data.year}"
data.dir <- paste0("data", data.year)
output.file <- paste0("buildings_matched_OFM2017_", Sys.Date(), ".csv")

pcl.id <- 'parcel_id'

# load data
bld.imp <- fread(file.path(data.dir, "buildings.csv"), sep=',', header=TRUE)
source(file.path(data.dir, "read_hh_totals.R")) # reads DU totals from the data directory (creates object hhtots)
hhtots$HH <- as.integer(round(hhtots$HH))
pcl <- fread(file.path(data.dir, "parcels.csv"), sep=',', header=TRUE)
setkey(pcl, parcel_id)

# set-up the buildings table and merge with parcels
bld <- data.table(bld.imp)
setkey(bld, building_id, parcel_id)
bld %<>% cbind(residential_units_orig=extract2(., "residential_units"))
bld %<>% merge(pcl[,c(pcl.id, "census_block_id", "census_block_group_id", "county_id", "census_tract_id"), with=FALSE], by=pcl.id)


#match.by <- c('county_id', 'tract', 'block_group', "census_block_id")
match.by <- c("census_block_id")
cond.census_block_id <- function(i) return(with(bld, census_block_id == s$census_block_id[i]))
cond.pcl.census_block_id <- function(i) return(with(pcl, census_block_id == s$census_block_id[i]))
cond.block_group <- function(i) return(with(bld, county_id == s$county_id[i] & 
                                              census_tract_id == s$census_tract_id[i] & 
                                              census_block_group_id == s$census_block_group_id[i]))
cond.tract <- function(i) return(with(bld, county_id == s$county_id[i] & census_tract_id == s$census_tract_id[i]))
cond.func.name <- paste0("cond.", match.by[length(match.by)])
cond.pcl.func.name <- paste0("cond.pcl.", match.by[length(match.by)])

# aggregate the hhtots dataset to the corresponding geography
hhtots <- data.table(hhtots)[,list(HH=sum(HH), GQ = sum(GQ)), by=c(match.by, "county_id")]

mftypes <- c(12, 4)
allrestypes <- c(12, 4, 19, 11, 34, 10, 33)
reslutypes <- c(#8, # GQ
                13, # mobile home
                14, # MF
                15, # condo
                21, # recreation
                24, # SF
                26, # vacant developable
                30 # mix use
                )
gqlutypes <- c(8,  # GQ
               11  # military
               )
gqbtypes <- c(6 # group-quarters
              )

dev.blocks <- pcl[, .(Nrespcl = sum(land_use_type_id %in% reslutypes), 
                      Ndevpcl = sum(plan_type_id != 1000),
                      Ngqpcl = sum(land_use_type_id %in% gqlutypes),
                      Npcl = .N), 
                  by = match.by]

create.duhh.dt <- function() {
	du <- as.data.frame(bld[, list(
	        DU=sum(residential_units), 
	        number_of_buildings=.N, 
	        Nmf=sum(building_type_id %in% mftypes),
					Nres=sum(building_type_id %in% allrestypes),
					Ngq=sum(building_type_id %in% gqbtypes),
					DUimp=sum((residential_units - residential_units_orig) * (residential_units > residential_units_orig) * (building_type_id %in% allrestypes)),
					DUred=sum((residential_units_orig - residential_units) * (residential_units < residential_units_orig) * (building_type_id %in% allrestypes))
					), by=match.by])
	# merge with HH totals (created in read_hh_totals.R)
	duhh <- merge(du, hhtots, by=match.by, all=TRUE)
	duhh <- merge(duhh, dev.blocks, by=match.by, all=TRUE)
	duhh <- subset(duhh, !(is.na(DU) & HH==0)) # remove blocks with zero OFM and no buildings
	duhh[is.na(duhh$DU), c("DU", "number_of_buildings", "Nmf", "Nres", "DUimp", "DUred")] <- 0 # blocks with no buildings in our dataset
	duhh <- data.table(duhh) %$% cbind(., dif=DU-HH)
	duhh[is.na(Nrespcl), Nrespcl := 0]
	duhh[is.na(Ndevpcl), Ndevpcl := 0]
	duhh[is.na(Ngq), Ngq := 0]
	duhh[is.na(Ngqpcl), Ngqpcl := 0]
	duhh[, gqdif := (Ngq > 0) - (GQ > 0)]
	duhh
}

duhh <- create.duhh.dt()
negdt <- subset(duhh, dif < 0) 
# print(sum(with(negdt, HH-DU)))
# head(negdt[order(negdt$dif),], 50)
print(duhh[,list(DU=sum(DU), DUofm=sum(HH, na.rm=TRUE), dif=sum(dif, na.rm=TRUE)), by=county_id])

# for parcels with no res buildings build new buildings where possible 
new.bldgs <- NULL
s <- subset(negdt, Nres == 0)
if(nrow(s) > 0) {
  cat('\n\n')
  for (i in 1:nrow(s)){
    cat('\rProgress ', round(i/nrow(s)*100), '%')
    is_pcl <- do.call(cond.pcl.func.name, list(i))
    pidx <- which(is_pcl & pcl$land_use_type_id %in% reslutypes)
    if(length(pidx) == 0) { # no residential or vacant LUT
      if(s$number_of_buildings[i] > 0) next # if there is a building don't do anything
      pidx <- which(is_pcl & pcl$plan_type_id != 1000) # take any developable parcel
      if(length(pidx) == 0)
        pidx <- which(is_pcl)  # take all parcels
    }
    new <- pcl[pidx, c(pcl.id, "census_block_id", "census_block_group_id", "county_id", "census_tract_id"), with = FALSE]
    new[, `:=`(residential_units = 1, # seed the building with 1 DU
               residential_units_orig = 0, 
               non_residential_sqft = 0,
               building_type_id = 12, # new buildings will all be MF
               imp_residential_units = 1)]
    new.bldgs <- rbind(new.bldgs, new)
  }
  cat('\nImputed ', nrow(new.bldgs), ' buildings for ', length(unique(new.bldgs$census_block_id)), 'blocks with no residential buildings.')
  # new building ids
  start.id <- max(bld$building_id) + 1
  new.bldgs <- new.bldgs[, building_id := seq(start.id, start.id + nrow(new.bldgs) - 1)]
  # combine original buildings with the new ones
  bld.current <- copy(bld)
  bld <- rbind(bld, new.bldgs, fill = TRUE)
}

duhh <- create.duhh.dt()
negdt <- subset(duhh, dif < 0) 


set.seed(1)
imputed.du <- 0
imputed.bld <- 0

# 1 building: place DU independent of type of building
s <- subset(negdt, number_of_buildings == 1)
if(nrow(s) > 0) {
  cat('\n\n')
  for (i in 1:nrow(s)){
    cat('\rProgress ', round(i/nrow(s)*100), '%')
	  bidx <- which(do.call(cond.func.name, list(i)))
	  bld[bidx, "residential_units"] <- bld$residential_units[bidx] + s$HH[i] - s$DU[i]
	  bld[bidx, "imp_residential_units"] <- 1
	  imputed.du <- imputed.du + s$HH[i] - s$DU[i]
	  imputed.bld <- imputed.bld + 1
  }
  cat('\nImputed ', imputed.du, ' units into ', imputed.bld, ' buildings for blocks with 1 building.')
}

# > 1 buildings & 1 multi-family building: place DU into that MF building
last.imputed.du <- imputed.du
last.imputed.bld <- imputed.bld
s <- subset(negdt, number_of_buildings > 1 & Nmf==1)
if(nrow(s) > 0) {
  cat('\n\n')
  for (i in 1:nrow(s)) {
    cat('\rProgress ', round(i/nrow(s)*100), '%')
	  bidx <- which(do.call(cond.func.name, list(i)) & bld$building_type_id %in% mftypes)
	  bld[bidx, "residential_units"] <- bld$residential_units[bidx] + s$HH[i] - s$DU[i]
	  bld[bidx, "imp_residential_units"] <- 1
	  imputed.du <- imputed.du + s$HH[i] - s$DU[i]
	  imputed.bld <- imputed.bld + 1
  }
  cat('\nImputed ', imputed.du-last.imputed.du, ' units into ', imputed.bld-last.imputed.bld, ' buildings for blocks with 1 MF building.')
}

# > 1 multi-family buildings
last.imputed.du <- imputed.du
last.imputed.bld <- imputed.bld
s <- subset(negdt, number_of_buildings > 1 & Nmf>1)
if(nrow(s) > 0) {
  cat('\n\n')
  for (i in 1:nrow(s)){
	  cat('\rProgress ', round(i/nrow(s)*100), '%')
	  bidx <- which(do.call(cond.func.name, list(i)) & bld$building_type_id %in% mftypes)
    # distribute units across MF buildings
		imp.idx <- bidx
	  sDUimp <- sum(bld$residential_units[imp.idx])
	  probs <- if(sDUimp == 0) rep(1, length=length(imp.idx))/length(imp.idx) else bld$residential_units[imp.idx]/sDUimp
	  if(length(imp.idx)==1) {
		  imp.idx <- rep(imp.idx,2) # sample interprets things differently if the first number is just one integer
		  probs <- rep(probs,2)
	  }
	  sampled.idx <- sample(imp.idx, s$HH[i] - s$DU[i], replace=TRUE, prob=probs)
	  tab <- table(sampled.idx)
	  row.idx <- as.integer(names(tab))
	  bld[row.idx, "residential_units"] <- bld$residential_units[row.idx] + tab
	  bld[row.idx, "imp_residential_units"] <- 1
	  imputed.du <- imputed.du + s$HH[i] - s$DU[i]
	  imputed.bld <- imputed.bld + length(row.idx)
  }
  cat('\nImputed ', imputed.du-last.imputed.du, ' units into ', imputed.bld-last.imputed.bld, ' buildings for ', nrow(s), ' blocks with multiple MF buildings.')
}

# 0 multi-family buildings & > 0 residential (other residential type than MF)
last.imputed.du <- imputed.du
last.imputed.bld <- imputed.bld
s <- subset(negdt, number_of_buildings > 1 & Nmf==0 & Nres > 0)
if(nrow(s) > 0) {
  cat('\n\n')
  for (i in 1:nrow(s)){
	  cat('\rProgress ', round(i/nrow(s)*100), '%')
	  bidx <- which(do.call(cond.func.name, list(i)) & bld$building_type_id %in% allrestypes)
    # distribute units across res buildings
		imp.idx <- bidx
	  sDUimp <- sum(bld$residential_units[imp.idx])
	  probs <- if(sDUimp == 0) rep(1, length=length(imp.idx))/length(imp.idx) else bld$residential_units[imp.idx]/sDUimp
	  if(length(imp.idx)==1) {
		  imp.idx <- rep(imp.idx,2) # sample interprets things differently if the first number is just one integer
		  probs <- rep(probs,2)
	  }
	  sampled.idx <- sample(imp.idx, s$HH[i] - s$DU[i], replace=TRUE, prob=probs)
	  tab <- table(sampled.idx)
	  row.idx <- as.integer(names(tab))
	  bld[row.idx, "residential_units"] <- bld$residential_units[row.idx] + tab
	  bld[row.idx, "imp_residential_units"] <- 1
	  imputed.du <- imputed.du + s$HH[i] - s$DU[i]
	  imputed.bld <- imputed.bld + length(row.idx)
  }
  cat('\nImputed ', imputed.du-last.imputed.du, ' units into ', imputed.bld-last.imputed.bld, ' buildings for ', nrow(s), 'blocks with non-MF residential buildings.\n')
}

# 0 residential buildings (only non-residential type)
last.imputed.du <- imputed.du
last.imputed.bld <- imputed.bld
s <- subset(negdt, number_of_buildings > 1 & Nres == 0)
if(nrow(s) > 0) {
  cat('\n\n')
  for (i in 1:nrow(s)){
	  cat('\rProgress ', round(i/nrow(s)*100), '%')
	  bidx <- which(do.call(cond.func.name, list(i)))
    # distribute units across buildings proportional to non-res sqft
		imp.idx <- bidx
	  sSQimp <- sum(bld$non_residential_sqft[imp.idx])
	  probs <- if(sSQimp == 0) rep(1, length=length(imp.idx))/length(imp.idx) else bld$non_residential_sqft[imp.idx]/sSQimp # take non-res sqft as  a proxy for the size
	  if(length(imp.idx)==1) {
		  imp.idx <- rep(imp.idx,2) # sample interprets things differently if the first number is just one integer
		  probs <- rep(probs,2)
	  }
	  sampled.idx <- sample(imp.idx, s$HH[i] - s$DU[i], replace=TRUE, prob=probs)
	  tab <- table(sampled.idx)
	  row.idx <- as.integer(names(tab))
	  bld[row.idx, "residential_units"] <- bld$residential_units[row.idx] + tab
	  bld[row.idx, "imp_residential_units"] <- 1
	  imputed.du <- imputed.du + s$HH[i] - s$DU[i]
	  imputed.bld <- imputed.bld + length(row.idx)
	  if(length(row.idx) > 0) {
		  bld[row.idx, "building_type_id"] <- 12 # set to MF residential
	  }
  }
  cat('\nImputed ', imputed.du-last.imputed.du, ' units into ', imputed.bld-last.imputed.bld, ' buildings for ', nrow(s), 'blocks with non-residential buildings.\n')
}
cat('\nTotals: ', imputed.du, 'units, ', imputed.bld, ' buildings\n')


# Reduce DUs for MF-buildings where Census numbers are lower
# Do not reduce units from new imputed buildings
reduction.factor <- 1.2
duhh2 <- create.duhh.dt()
s <- subset(duhh2, DU > ceiling(reduction.factor*HH) & Nmf > 0)
reduced.du <- 0
reduced.bld <- 0
if(nrow(s) > 0) {
	cat('\n')
  for (i in 1:nrow(s)){
	  cat('\rProgress ', round(i/nrow(s)*100), '%')
    if(s$census_block_id[i] %in% new.bldgs$census_block_id) next
	  bidx <- which(do.call(cond.func.name, list(i)) & bld$building_type_id %in% mftypes)
	  reduction <- floor(s$DU[i] - reduction.factor*s$HH[i])
	  if(sum(bld$residential_units[bidx])==0) next
	  probs <- bld$residential_units[bidx]/sum(bld$residential_units[bidx])
	  if(length(bidx)==1) {
		  bidx <- rep(bidx,2) # sample interprets things differently if the first number is just one integer
		  probs <- rep(probs,2)
	  }
	  sampled.idx <- sample(bidx, reduction, replace=TRUE, prob=probs)
	  tab <- table(sampled.idx)
	  row.idx <- as.integer(names(tab))
	  value <- pmax(pmin(pmax(bld$stories[row.idx],1)*2, bld$residential_units[row.idx]), bld$residential_units[row.idx] - tab)
	  reduced.du <- reduced.du + sum(bld$residential_units[row.idx] - value)
	  reduced.bld <- reduced.bld + sum((bld$residential_units[row.idx] - value) > 0)
	  bld[row.idx, "residential_units"] <- as.integer(value)
  }
  cat('\n\nResidential units reduced by ', reduced.du, ' in ', reduced.bld, ' buildings from ', nrow(s), 'blocks.\n') 
}


# print summary
duhh.end <- create.duhh.dt()
print(duhh.end[,list(DU=sum(DU), DUofm=sum(HH, na.rm=TRUE), 
                     DUimp=sum(DUimp, na.rm=TRUE), DUred=sum(DUred, na.rm=TRUE), dif=sum(dif, na.rm=TRUE)), 
               by=county_id])

tot <- sum(with(bld, residential_units))
tot.orig <- sum(with(bld, residential_units_orig))
cat('\nTotal change: ', tot-tot.orig, ' residential units (from ', tot.orig, ' to ', tot, ')')

# write outputs
building.schema <- c('building_id', 'residential_units', 'non_residential_sqft', 'year_built', 'not_demolish', 'parcel_id', 'land_area', 'improvement_value', 
					 'stories', 'building_type_id', 'job_capacity', 'sqft_per_unit', 'building_quality_id')
building.schema.extended <- c(building.schema, 'building_type_id_orig', 'imp_residential_units', 'imp_non_residential_sqft', 'imp_improvement_value', 'residential_units_orig')					 
fwrite(bld[, colnames(bld)%in% building.schema.extended, with=FALSE], file=file.path(data.dir, output.file), sep=',', row.names=FALSE)

# for exportng to opus cache, remove the parcels attributes, since they are not needed
#bld.for.opus <- as.data.frame(bld[,colnames(bld)%in% building.schema, with=FALSE])
#colnames(bld.for.opus)[colnames(bld.for.opus) == pcl.id] <- 'parcel_id'
#colnames(bld.for.opus) <- paste0(colnames(bld.for.opus), ':i4')
#for(col in colnames(bld.for.opus)) bld.for.opus[,col] <- as.integer(bld.for.opus[,col])
#write.table(bld.for.opus, file=file.path(data.dir, "imputed_buildings_matched_for_opus.csv"), sep=',', row.names=FALSE)

