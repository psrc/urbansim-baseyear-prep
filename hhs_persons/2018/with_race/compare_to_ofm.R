library(data.table)
library(bit64)

ofm <- fread("ofm_controls_2018.csv", colClasses = c(block_group_id = "character"))
hh <- fread("output/households.csv")
pers <- fread("output/persons.csv")
colnames(hh) <- substr(colnames(hh), 1, nchar(colnames(hh))-3)
colnames(pers) <- substr(colnames(pers), 1, nchar(colnames(pers))-3)

hhpers <- pers[, .(white = sum(race == 1), black = sum(race == 2), other = sum(race > 2),
                   age_19_and_under = sum(age <= 19), age_20_to_35 = sum(age > 19 & age <= 35),
                   age_35_to_60 = sum(age > 35 & age <= 60), age_above_60 = sum(age > 60),
                   school_yes = sum(student > 0)
                   ), by = household_id]
hh <- merge(hh, hhpers, by = "household_id")

synthh <- fread("synthetic_households_all_pums.csv", 
                colClasses = c(block_group_id = "character", GEOID10 = "character"))
bg <- fread("census_block_groups.csv", colClasses = c(census_2010_block_group_id = "character"))
synthh[bg, census_block_group_id := i.census_block_group_id, on = c(GEOID10 = "census_2010_block_group_id")]

ofm[synthh, census_block_group_id := i.census_block_group_id, on = "block_group_id"]
ofm[, `:=`(persons = male + female, hh = hh_size_1 + hh_size_2 + hh_size_3 + hh_size_4 + hh_size_5 + hh_size_6 + hh_size_7_plus)]

hhbg <- hh[, .(hh = .N, persons = sum(persons),
                income_lt15 = sum(income <= 15000),
               `income_gt15-lt30` = sum(income > 15000 & income <= 30000),
               `income_gt30-lt60` = sum(income > 30000 & income <= 60000),
               `income_gt60-lt100` = sum(income > 60000 & income <= 100000),
               income_gt100 = sum(income > 100000),
               hh_size_1 = sum(persons == 1),
               hh_size_2 = sum(persons == 2),
               hh_size_3 = sum(persons == 3),
               hh_size_4 = sum(persons == 4),
               hh_size_5 = sum(persons == 5),
               hh_size_6 = sum(persons == 6),
               hh_size_7_plus = sum(persons > 6),
               workers_0 = sum(workers == 0),
               workers_1 = sum(workers == 1),
               workers_2 = sum(workers == 2),
               workers_3_plus = sum(workers > 2),
               white = sum(white),
               black = sum(black),
               other = sum(other),
               school_yes = sum(school_yes),
               age_19_and_under = sum(age_19_and_under),
               age_20_to_35 = sum(age_20_to_35),
               age_35_to_60 = sum(age_35_to_60),
               age_above_60 = sum(age_above_60),
               is_worker = sum(workers)
               ), by = census_block_group_id]

hhjoin <- merge(hhbg, ofm[, colnames(hhbg), with = FALSE], by = "census_block_group_id")

summ <- fread("final_summary_block_group_id.csv")
#summ[, persons_diff := - female_control - male_control + female_result + male_result]
pdf('comparison_ofm_synth_20210330.pdf', width=10)
par(mfrow = c(3,4))
par(mar=c(2,2,1,0.4)+0.1, mgp=c(1,0.3,0))
for(attr in setdiff(colnames(hhbg), "census_block_group_id")) {
    values.ofm <- hhjoin[[paste0(attr, ".y")]]
    values.synt <- hhjoin[[paste0(attr, ".x")]]
    plot(values.ofm, values.synt, xlab = "OFM", ylab = "Synthesizer", 
         main = attr)
    abline(0,1)
    idx <- which.max(abs(values.ofm - values.synt))
    if(paste0(attr, "_diff") %in% colnames(summ)) {
        qc <- summ[[paste0(attr, "_diff")]]
        avg.qc <- round(mean(qc), 1)
        sum.qc <- round(sum(qc), 1)
    } else qc <- NULL
    avg.dif <- round(mean(values.synt - values.ofm), 1)
    sum.dif <- round(sum(values.synt - values.ofm), 1)
    legend("topleft", legend = c(paste("max abs dif:", abs(values.ofm - values.synt)[idx], "(BG", hhjoin[idx, census_block_group_id], ")"),
                                 paste("avg dif:", avg.dif, if(!is.null(qc) && avg.qc != avg.dif) paste0("(", avg.qc, ")") else ""),
                                 paste("sum dif:", sum.dif, if(!is.null(qc) && sum.qc != sum.dif) paste0("(", sum.qc, ")") else "")), bty = "n")
    points(values.ofm[idx], values.synt[idx], col = "red")
}
dev.off()


