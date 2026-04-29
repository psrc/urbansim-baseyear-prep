# read new FLU and old FLU files
#fluall <- fread(new.flu.name)
fluall <- data.table(read_xlsx(new.flu.name))

# extract bonus/no-bonus records
#flubonus <- fluall[Bonus_included == "Y"]
#flunobonus <- fluall[is.na(Bonus_included) | Bonus_included == ""]
#flu <- rbind(flubonus, flunobonus[!juris_zn %in% flubonus$juris_zn])[Zone != "ERROR"]

flu <- fluall[Zone != "ERROR"] # for now keep all records (bonus & no-bonus)

# collect columnn names
id.cols <- intersect(colnames(flu), c(str_subset(colnames(flu), "^Juris|Zone"), "Key", "Definition"))
use.cols <- str_subset(colnames(flu), "^[R|C|O|I|M].*_Use$")
max.cols <- c(str_subset(colnames(flu), "^MaxD.*_[R].*"), str_subset(colnames(flu), "^MaxF.*_[C|O|I|M].*"))
min.cols <- c(str_subset(colnames(flu), "^MinD.*_[R].*"), str_subset(colnames(flu), "^MinF.*_[C|O|I|M].*"))
maxht.cols <- str_subset(colnames(flu), "^MaxHt.*_[R|C|O|I|M].*")
lc.cols <- str_subset(colnames(flu), "^LC_[R|C|O|I|M].*")

# organize column names into sets of lists
# an iterator for loops
cols.sets <- pmap(list(use.cols, min.cols, max.cols, maxht.cols, lc.cols), list) %>% 
    map(~set_names(.x, c("use", "min_dens", "max_dens", "height", "lc"))) %>% 
    set_names(c("Res", "Comm", "Office", "Indust", "Mixed"))

# clean Use columns
for(col in use.cols){
    flu[, (col) := as.logical(ifelse(!is.na(get(col)) & tolower(get(col)) == "y", TRUE, FALSE))]
}

# add missing density items
cols.sets[['Mixed']][['max_dens']] <- list(du = "MaxDU_Mixed", far = cols.sets[['Mixed']][['max_dens']])
cols.sets[['Mixed']][['min_dens']] <- list(du = "MinDU_Mixed", far = cols.sets[['Mixed']][['min_dens']])
cols.sets[['Res']][['max_dens']] <- list(du = cols.sets[['Res']][['max_dens']], far = "MaxFAR_Res", lot = "ResDU_lot")
cols.sets[['Res']][['min_dens']] <- list(du = cols.sets[['Res']][['min_dens']], far = "MinFAR_Res")


# Clean FLU ---------------------------------------------------------------

clean.cols <- unique(c(maxht.cols, min.cols, max.cols, lc.cols, 
                       unlist(cols.sets$Mixed[c('max_dens', 'min_dens')]),
                       unlist(cols.sets$Res[c('max_dens', 'min_dens')])))

for (col in clean.cols) {
    # clean various numeric columns (set "None" to NA & "unlimited" to -1)
    if(is.character(flu[[col]]))
        flu[get(col) %in% c("", "None"), (col) := NA]
    flu[get(col) == "unlimited", (col) := -1]   
}

# fix Ht (some records have info in terms of stories, e.g. "3 story")
for(col in maxht.cols)
    flu[grepl(" story", get(col)), (col) := as.integer(gsub(" story", "", get(col))) * 12]

# there are some values in parentheses, remove it (but double check if it makes sense)
#flu[,.N, by = "MaxFAR_Mixed"]
for(col in clean.cols){
    flu[, (col) := gsub("\\s*\\([^\\)]+\\)", "", get(col))]
}

# In Sumner MaxDU_Res there are values defined as ranges, take the middle
col <- "MaxDU_Res"
flu[, c("rng1", "rng2") := tstrsplit(get(col), "-", type.convert = TRUE)]
flu[!is.na(rng2), c(col, "juris_zn", "rng1", "rng2"), with = FALSE] # view affected rows
flu[!is.na(rng2), (col) := round(rng1 + (rng2 - rng1)/2)][
    , `:=`(rng1 = NULL, rng2 = NULL)]

for (col in clean.cols) {
    # convert cols to double type
    # if there is a warning, investigate in which column and why, 
    # by setting options(warn = 2) & flu[,.N, by = col]
    if(is.character(flu[[col]]))
        flu[ , (col) := as.double(get(col))]
}

# combine FLU with bonuses and no-bonuses while removing duplicates
##########
# which columns should be considered to identify duplicates
cols.for.dupl <- c(clean.cols, use.cols, "Juris", "juris_zn", "rural")

flub <- flu[Bonus_included == "Y"]
nflu <- nrow(flub)
flub[, .N, by = "juris_zn"][order(-N)][N > 1]
flub <- flub[!duplicated(flub, by = cols.for.dupl)] # removes duplicates
cat("\nRemoved ", nflu - nrow(flub), " duplicate rows from bonus records.\n")

flunob <- flu[is.na(Bonus_included) | Bonus_included == ""]
nflu <- nrow(flunob)
flunob[, .N, by = "juris_zn"][order(-N)][N > 1]
flunob <- flunob[!duplicated(flunob, by = cols.for.dupl)] # removes duplicates
cat("\nRemoved ", nflu - nrow(flunob), " duplicate rows from no-bonus records.\n")

# check if no-bonus records have more non-missing values than bonus records
flub[, noNA := rowSums(sapply(flub[, clean.cols, with = FALSE], function(x) !is.na(x)))]
flunob[, noNA := rowSums(sapply(flunob[, clean.cols, with = FALSE], function(x) !is.na(x)))]
flunob[flub, noNAbon := i.noNA, on = "juris_zn"]
zn <- flunob[!is.na(noNAbon) & noNA > noNAbon, juris_zn] # which records have more non-missing values for no-bonus
zn_max <- flu[juris_zn %in% zn, lapply(.SD, max, na.rm = TRUE), by = "juris_zn", .SDcols = clean.cols] # take the max from these records

# combine these two types of records
flu <- rbind(flub[!juris_zn %in% zn],
             merge(zn_max, flub[juris_zn %in% zn, c("juris_zn", setdiff(colnames(flub), colnames(zn_max))), 
                                with = FALSE], by = "juris_zn")
)
flu <- rbind(flu, flunob[!juris_zn %in% flu[, juris_zn]], fill = TRUE)[
    , `:=`(noNA = NULL, noNAbon = NULL)]

cat("\nCombined dataset has", nrow(flu), "rows.\n")

process_rural <- function(flu){
    flu[, rural := ifelse(rural == "Y", TRUE, ifelse(rural %in% c("N", "not"), FALSE, NA))]
    # set various zones as "rural" (21 records)
    # found via flu[grepl("rural", Definition) & is.na(rural)]
    flu[(Zone %in% c("FL", "EPF-RAN", "RIC", "RNC", "RSR", "RR", "RW", "RCO", "SVLR", "FRL", "ARL", "RSep") |
             juris_zn %in% c("Pierce_County_GC", "Kenmore_GC", "Kenmore_P", "Kent_A-10", "Kitsap_RP",
                             "Kitsap_RI", "Kitsap_REC", "Kitsap_TTEC", "Arlington_RULC")) 
        & is.na(rural), rural := TRUE]
    # set NA rural records to FALSE 
    flu[is.na(rural), rural := FALSE]
    
    # set rural to FALSE if MaxDU_Res > 20 (3 records)
    flu[rural == TRUE & MaxDU_Res > 20, rural := FALSE]
}

more_cleaning <- function(flu){
    flu[!is.na(ResDU_lot) & ResDU_lot > 100, ResDU_lot := NA] # these records seem to contain sqft (and not DU) in this field 
    # set to residential use if ResDU_lot given (three records found)
    flu[((!is.na(ResDU_lot) & ResDU_lot != 0) | (!is.na(MaxDU_Res) & MaxDU_Res > 0)) & Res_Use == FALSE, Res_Use := TRUE]
    
    # if "missing middle" is in the description but neither ResDU_lot nor MaxDU_Res given,
    # set ResDU_lot to -1 (i.e. follow the HC1110 law). It applies to two zones in Marysville
    flu[grepl("missing middle", Definition) & Res_Use == TRUE & is.na(ResDU_lot) & is.na(MaxDU_Res),
        ResDU_lot := -1]
    
    # if use-specific LC not given but LC_Mixed given, set the use-specific LC to that
    for(use in c("Res", "Comm", "Office", "Indust")){
        flu[get(paste0(use, "_Use")) == TRUE & is.na(get(paste0("LC_", use))) & !is.na(LC_Mixed),
            (paste0("LC_", use)) := LC_Mixed]
    }
}