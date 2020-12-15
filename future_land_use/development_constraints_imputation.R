# Impute development constraints and compare to previous density table

library(stringr)
library(purrr)
library(magrittr)
library(data.table)
library(foreign)


# Setup -------------------------------------------------------------------

out.path <- "C:/Users/clam/Desktop/urbansim-baseyear-prep/future_land_use"

# read new FLU and old FLU files
flu <- fread("W:/gis/projects/compplan_zoning/density_table_4_gis.csv")
oflu <- read.dbf("X:/DSA/FutureLandUse/FLU_2016/FLU_for_DC.dbf", as.is = TRUE) %>% as.data.table

# missing Mixed
id.cols <- c(str_subset(colnames(flu), "^Juris|Zone"), "Key", "Definition")
use.cols <- str_subset(colnames(flu), "^[R|C|O|I].*_Use$")
max.cols <- c(str_subset(colnames(flu), "^MaxD.*_[R].*"), str_subset(colnames(flu), "^MaxF.*_[C|O|I].*"))
maxht.cols <- str_subset(colnames(flu), "^MaxHt.*_[R|C|O|I].*")
lc.cols <- str_subset(colnames(flu), "^LC_[R|C|O|I].*")

# organize column names into sets of lists
cols.sets <- pmap(list(use.cols, max.cols, maxht.cols, lc.cols), list) %>% 
  map(~set_names(.x, c("use", "dens", "height", "lc"))) %>% 
  set_names(c("Res", "Comm", "Office", "Indust"))

# add mixed use to cols.sets
cols.sets[['Mixed']] <- list(use = "Mixed_Use", 
                             dens = list(du = "MaxDU_Mixed", far = "MaxFAR_Mixed"),
                             height = "MaxHt_Mixed",
                             lc = "LC_Mixed")

# clean data
for (col in unlist(c(maxht.cols, max.cols, lc.cols, cols.sets$Mixed$dens, cols.sets$Mixed$height, cols.sets$Mixed$lc))) {
  if(is.character(flu[[col]]))
    flu[[col]] <- as.double(flu[[col]])
}

# QC
fm1 <- flu[is.na(MaxDU_Res) & !is.na(MaxHt_Res) & !is.na(LC_Res),] # 3 recs with where Res_Use == N.
fm2 <- flu[is.na(MaxDU_Res) & !is.na(MaxHt_Res) & is.na(LC_Res),]

# New flu, impute ---------------------------------------------------------


# loop through cols.sets, create new '_imp' columns with imputed values if max height is available
for (i in 1:length(cols.sets)) {
  print(names(cols.sets[i]))
  
  for (j in cols.sets[[i]]$dens) {
    print(j)
    
    use.col <-cols.sets[[i]]$use
    ht.col <- cols.sets[[i]]$height
    lc.col <- cols.sets[[i]]$lc
    
    newcolnm <- paste0(j, "_imp")
    newcolnm_tag <- paste0(j, "_src")
    
    # density columns (switch for Mixed Use)
    if (names(cols.sets[i]) == "Mixed") {
      ifelse(str_detect(j, "DU"), density.col <- cols.sets[[i]]$dens$du, density.col <- cols.sets[[i]]$dens$far)
    } else {
      density.col <- cols.sets[[i]]$dens
    }
    
    if (str_detect(j, "DU")) {
     
      # Records with missing DU/acre, non-missing heights and non-missing lot coverage(LC)
      equat <- parse(text = paste0("\`:=\`(", newcolnm, "= (exp(1.335 + 0.647*log(", ht.col, ") + 2.132*log(", lc.col, "))),", newcolnm_tag, "= 'imputed')"))
      flu[get(eval(use.col)) == "Y" & (is.na(get(eval(density.col))) | get(eval(density.col)) == 0) & !is.na(get(eval(ht.col))) & !is.na(get(eval(lc.col))), eval(equat)]
      
      # Records with missing DU/acre, non-missing heights and missing lot coverage
      equat <- parse(text = paste0("\`:=\`(", newcolnm, "= (exp(-2.987 + 1.407*log(", ht.col, "))),", newcolnm_tag, "= 'imputed')"))
      flu[get(eval(use.col)) == "Y" & (is.na(get(eval(density.col))) | get(eval(density.col)) == 0) & !is.na(get(eval(ht.col))) & is.na(get(eval(lc.col))), eval(equat)]
      
    } else {
      equat <- parse(text = paste0("\`:=\`(", newcolnm, "= ", ht.col, "/20,", newcolnm_tag, "= 'imputed')"))
      flu[get(eval(use.col)) == "Y" & (is.na(get(eval(density.col))) | get(eval(density.col)) == 0) & !is.na(get(eval(ht.col))), eval(equat)]
    }
    
  }
}


# Compile previous flu ----------------------------------------------------


# clean old flu (2016)
oflu[, Max_Height := as.numeric(Max_Height)]

oflu.max.cols <- str_subset(colnames(oflu), "^Max_.*")
oflu.cols <- c("Jurisdicti", "Key", "Definition", oflu.max.cols)
oflu <- oflu[, ..oflu.cols]

# join flu to old flu (2016)
# _new = flu, _prev = old flu
# all join/union, to see what joins and what doesn't
flu.join <- merge(flu, oflu, by = c("Key"), suffixes = c("_new", "_prev"), all = TRUE)
setnames(flu.join, oflu.max.cols, paste0(oflu.max.cols, "_prev"))


# Export file to compare collected, imputed, previous flu vals --------------------------

export.for.comparison <- function(flu.join, out.filepath) {
  # flu comparison
  comp.cols <- c("Key", 
                 str_subset(colnames(flu.join), "_new"),
                 use.cols,
                 max.cols,
                 maxht.cols,
                 unname(unlist(flatten(cols.sets[['Mixed']]))),
                 str_subset(colnames(flu.join), "_imp"),
                 str_subset(colnames(flu.join), "_prev"),
                 str_subset(colnames(flu.join), "_src"))
  comp.flu <- flu.join[!is.na(Jurisdicti_new), ..comp.cols]
  setcolorder(comp.flu, c("Key", 
                          str_subset(colnames(comp.flu), "_new"),
                          str_subset(colnames(comp.flu), "Res"),
                          str_subset(colnames(comp.flu), "Comm"),
                          str_subset(colnames(comp.flu), "Indust"),
                          str_subset(colnames(comp.flu), "Office"),
                          str_subset(colnames(comp.flu), "Mixed"),
                          str_subset(colnames(comp.flu), "_prev")))
  
  # export comp.flu for review
  fwrite(comp.flu, file.path(out.filepath, paste0("impute_flu_comparison_filled_", Sys.Date(),".csv")), row.names = F)
}

# for use with development_constraints_compare.Rmd
# export.for.comparison(flu.join, "J:/Staff/Christy/usim-baseyear/flu")


# Update remaining na in _imp columns if prev values available -------------


flu.imp <- copy(flu.join)

# if max height is missing 
# loop thru cols.sets, update col ending '_imp' with prev du/far if available
# optional to copy original Max du/far to '_imp' cols
for (i in 1:length(cols.sets)) {
  print(names(cols.sets[i]))
  s <- cols.sets[[i]]
  
  for (j in s$dens) {
    use.col <-s$use
    ht.col <- s$height
    
    ifelse(str_detect(j, "DU"), prev.dens.col <- "Max_DU_Ac_prev", prev.dens.col <- "Max_FAR_prev")
    
    if (names(cols.sets[i]) == "Mixed") {
      ifelse(str_detect(j, "DU"), density.col <- s$dens$du, density.col <- s$dens$far)
    } else {
      density.col <- s$dens
    }
    
    imp.density.col <- paste0(density.col, "_imp")
    newcolnm_tag <- paste0(j, "_src")
    prev.equat <- parse(text = paste0("\`:=\`(", imp.density.col, "= ", prev.dens.col, ",", newcolnm_tag, "= 'prev')"))
    orig.equat <- parse(text = paste0("\`:=\`(", imp.density.col, "= ", density.col, ",", newcolnm_tag, "= 'collected')"))
    
    # update col ending '_imp' with prev du/far
    flu.imp[!is.na(Jurisdicti_new) &
               get(eval(use.col)) == "Y" &
               is.na(get(eval(imp.density.col))) &
               is.na(get(eval(density.col))) &
               is.na(get(eval(ht.col))) &
               (get(eval(prev.dens.col)) > 0), eval(prev.equat)]

    # update col ending '_imp' with original du/far
    flu.imp[!is.na(Jurisdicti_new) &
               get(eval(use.col)) == "Y" &
               is.na(get(eval(imp.density.col))) &
               (get(eval(density.col)) > 0), eval(orig.equat)]
  }
}


# Final output ------------------------------------------------------------


# exclude _prev records that didn't match current flu records
flu.fin.prep <- flu.imp[!is.na(Jurisdicti_new)]

# subset columns and rename in preparation for 'unroll_constraints' .py script
ff.types <- c("Res", "Mixed", "Office", "Indust", "Comm")
ff.max.cols <- c(paste0("MaxDU_", ff.types), paste0("MaxFAR_", ff.types))
ff.excl.cols <- c(str_subset(colnames(flu.fin.prep), "_prev"), ff.max.cols)

ff.cols <- setdiff(colnames(flu.fin.prep), ff.excl.cols)
# remove '_imp' from col name
flu.fin.prep <- flu.fin.prep[, ..ff.cols] 
colnames(flu.fin.prep) <- str_trim(str_replace_all(colnames(flu.fin.prep), "_imp", ""))
colnames(flu.fin.prep) <- str_trim(str_replace_all(colnames(flu.fin.prep), "_new", ""))

# remove duplicate rows (in preparation for assigning plan_type_ids in unroll_constraints.py)
gb.cols <- setdiff(colnames(flu.fin.prep), "Zone_adj")
flu.fin <- unique(flu.fin.prep, by = gb.cols, fromLast = T)

fwrite(flu.fin, file.path(out.path, paste0("final_flu_imputed_", Sys.Date(), ".csv")))


