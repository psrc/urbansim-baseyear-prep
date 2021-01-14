# Impute development constraints and compare to previous density table

library(stringr)
library(purrr)
library(magrittr)
library(data.table)
library(foreign)

export.for.comparison <- function(flu.join, out.filepath) {
  # Export file to compare collected, imputed, previous flu vals in .Rmd
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


# clean FLU ---------------------------------------------------------------

clean.cols <- c(maxht.cols, max.cols, lc.cols, cols.sets$Mixed$dens, cols.sets$Mixed$height, cols.sets$Mixed$lc, 'MaxFAR_Res')
for (col in unlist(clean.cols)) {
  if(is.character(flu[[col]]))
    flu[[col]] <- as.double(flu[[col]])
}

# adjust MaxHt_Res column prior to imputation (retain original)
flu[, MaxHt_Res_orig := MaxHt_Res]
adj1 <- with(flu, !is.na(MaxFAR_Res) & !is.na(MaxFAR_Mixed) & MaxFAR_Res < MaxFAR_Mixed & MaxFAR_Res > 0)
adj2 <- with(flu, !is.na(MaxFAR_Res) & !is.na(MaxFAR_Mixed) & MaxFAR_Res > MaxFAR_Mixed)
flu[adj1, MaxHt_Res := round(MaxHt_Res * MaxFAR_Res/MaxFAR_Mixed)]
flu[adj2, MaxHt_Res := round(MaxHt_Res * MaxFAR_Res/(MaxFAR_Mixed + MaxFAR_Res))]


# New flu, impute ---------------------------------------------------------


# coefficients
coeff <- list(a = 1.635, b = 0.582, c = 2.182, d = -2.697, e = 1.340)

# Impute max DU/ac, residential height, and FAR
for (i in 1:length(cols.sets)) {
  # create new '_imp' columns with imputed values if criteria is met and tag '_src' column
  # Impute height for Residential cases (DU/ac)
  # create new 'MaxHt_XXX_imp' columns with imputed values if criteria is met and tag 'MaxHt_XXX_src' column
  print(names(cols.sets[i]))
  
  for (j in cols.sets[[i]]$dens) {
    print(j)
    
    use.col <-cols.sets[[i]]$use
    ht.col <- cols.sets[[i]]$height
    lc.col <- cols.sets[[i]]$lc
    
    newcolnm <- paste0(j, "_imp")
    newcolnm_tag <- paste0(j, "_src")
    
    newcolnm.ht <- paste0(ht.col, "_imp")
    newcolnm_tag.ht <- paste0(ht.col, "_src")
    
    # density columns (switch for Mixed Use)
    if (names(cols.sets[i]) == "Mixed") {
      ifelse(str_detect(j, "DU"), density.col <- cols.sets[[i]]$dens$du, density.col <- cols.sets[[i]]$dens$far)
    } else {
      density.col <- cols.sets[[i]]$dens
    }
    
    if (str_detect(j, "DU")) {
     
      # Records with missing DU/acre, non-missing heights and non-missing lot coverage(LC)
      equat1 <- parse(text = paste0("\`:=\`(", newcolnm, "= (exp(",coeff$a," + ", coeff$b,"*log(", ht.col, ") +", coeff$c,"*log(", lc.col, "))),",
                                   newcolnm_tag, "= 'imputed')"))
      flu[get(eval(use.col)) == "Y" & 
            (is.na(get(eval(density.col))) | get(eval(density.col)) == 0) & 
            !is.na(get(eval(ht.col))) & 
            !is.na(get(eval(lc.col))), eval(equat1)]
    
      # Records with missing DU/acre, non-missing heights and missing lot coverage
      equat2 <- parse(text = paste0("\`:=\`(", newcolnm, "= (exp(", coeff$d ,"+", coeff$e,"*log(", ht.col, "))),", 
                                   newcolnm_tag, "= 'imputed')"))
      flu[get(eval(use.col)) == "Y" & 
            (is.na(get(eval(density.col))) | get(eval(density.col)) == 0) & 
            !is.na(get(eval(ht.col))) & is.na(get(eval(lc.col))), eval(equat2)]
      
      # Records with missing height, non-missing DU/acre and non-missing lot coverage:
      equat3 <- parse(text = paste0("\`:=\`(", newcolnm.ht, "= (exp((log(", density.col,") - ", coeff$a,"-", coeff$c,"*log(", lc.col,"))/", coeff$b ,")),", 
                                   newcolnm_tag.ht, "= 'imputed')"))
      flu[get(eval(use.col)) == "Y" &
            (is.na(get(eval(ht.col))) | get(eval(ht.col)) == 0) &
            (!is.na(get(eval(density.col))) | get(eval(density.col)) != 0) &
            (!is.na(get(eval(lc.col))) | get(eval(lc.col)) != 0), eval(equat3)]
      
      # Records with missing height, non-missing DU/acre and missing lot coverage:
      # height = exp[(log(DU/acre) + 2.987)/1.407]
      # equat <- parse(text = paste0("\`:=\`(", newcolnm.ht, "= (exp((log(", density.col,") + 2.987)/1.407)),", newcolnm_tag.ht, "= 'imputed')"))
      equat4 <- parse(text = paste0("\`:=\`(", newcolnm.ht, "= (exp((log(", density.col,")-",coeff$d,")/",coeff$e,")),", 
                                   newcolnm_tag.ht, "= 'imputed')"))
      flu[get(eval(use.col)) == "Y" &
            (is.na(get(eval(ht.col))) | get(eval(ht.col)) == 0) &
            (!is.na(get(eval(density.col))) | get(eval(density.col)) != 0) &
            (is.na(get(eval(lc.col))) | get(eval(lc.col)) == 0), eval(equat4)]
      
    } else {
      equat <- parse(text = paste0("\`:=\`(", newcolnm, "= ", ht.col, "/20,", newcolnm_tag, "= 'imputed')"))
      flu[get(eval(use.col)) == "Y" & 
            (is.na(get(eval(density.col))) | get(eval(density.col)) == 0) & 
            !is.na(get(eval(ht.col))), eval(equat)]
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


# Update remaining na in _imp columns if prev values available -------------


flu.imp <- copy(flu.join)

# loop thru cols.sets, 
# update col ending '_imp' with prev du/far if available and copy original Max du/far to '_imp' cols
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

# if Residential max height is missing
# loop thru cols.sets, update col ending '_imp' with prev MaxHt if available
# optional to copy original Max du/far to '_imp' cols
for (stype in c('Res', 'Mixed')) {
  s <- cols.sets[[stype]]

  use.col <-s$use
  ht.col <- s$height
  
  prev.ht.col <- "Max_Height_prev"
  imp.ht.col <- paste0(ht.col, "_imp")
  newcolnm_tag <- paste0(ht.col, "_src")
  
  prev.equat <- parse(text = paste0("\`:=\`(", imp.ht.col, "= ", prev.ht.col, ",", newcolnm_tag, "= 'prev')"))
  orig.equat <- parse(text = paste0("\`:=\`(", imp.ht.col, "= ", ht.col, ",", newcolnm_tag, "= 'collected')"))
  
  # update col ending '_imp' with prev height
  flu.imp[!is.na(Jurisdicti_new) &
            get(eval(use.col)) == "Y" &
            is.na(get(eval(imp.ht.col))) &
            is.na(get(eval(ht.col))) &
            (get(eval(prev.dens.col)) > 0), eval(prev.equat)]
  
  # update col ending '_imp' with collected height
  flu.imp[!is.na(Jurisdicti_new) &
            get(eval(use.col)) == "Y" &
            is.na(get(eval(imp.ht.col))) &
            (get(eval(ht.col)) > 0), eval(orig.equat)]
}

# exclude _prev records that didn't match current flu records
flu.fin.prep <- flu.imp[!is.na(Jurisdicti_new)]

# temp write for QC (kitchen sink file)
# fwrite(flu.fin.prep, file.path(out.path, paste0("temp_flu_imputed_", Sys.Date(), ".csv")))


# Final output ------------------------------------------------------------


# subset columns and rename in preparation for 'unroll_constraints' .py script
ff.types <- c("Res", "Mixed", "Office", "Indust", "Comm")
ff.max.cols <- c(paste0("MaxDU_", ff.types), paste0("MaxFAR_", ff.types))
ff.excl.cols <- c(str_subset(colnames(flu.fin.prep), "_prev"), ff.max.cols, 
                  "MaxHt_Res", "MaxHt_Mixed", "MaxHt_Res_orig") # add MaxHt cols

ff.cols <- setdiff(colnames(flu.fin.prep), ff.excl.cols)
flu.fin.prep <- flu.fin.prep[, ..ff.cols] 

# remove '_imp' from col name
colnames(flu.fin.prep) <- str_trim(str_replace_all(colnames(flu.fin.prep), "_imp", ""))
colnames(flu.fin.prep) <- str_trim(str_replace_all(colnames(flu.fin.prep), "_new", ""))

# remove duplicate rows (in preparation for assigning plan_type_ids in unroll_constraints.py)
gb.cols <- setdiff(colnames(flu.fin.prep), "Zone_adj")
flu.fin <- unique(flu.fin.prep, by = gb.cols, fromLast = T)

fwrite(flu.fin, file.path(out.path, paste0("final_flu_imputed_", Sys.Date(), ".csv")))

# for use with development_constraints_compare.Rmd
# export.for.comparison(flu.imp, "J:/Staff/Christy/usim-baseyear/flu")


