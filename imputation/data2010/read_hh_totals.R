# Must create an object hhtots which is a data table with columns:
# county_id, tract, block_group, HH

hhs <- data.table(read.table(file.path(data.dir, "households2.csv"), sep=',', header=TRUE))
hhtots <- hhs[, .N, by=c('county', 'tract', 'block_group')]
colnames(hhtots)[c(1,4)] <- c('county_id', 'HH')
