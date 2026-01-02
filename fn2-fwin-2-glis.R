# Libraries
library(foreign)
library(dplyr)
library(readxl)
library(lubridate)
library(ggplot2)
library(stringr)
library(RODBC)
library(gfsR)
library(readr)

odbcCloseAll()

load("CORP_WBY.RData")
CORP_WBY <- CORP_WBY %>% 
  mutate(SILOC = paste0(LAT, LON)) %>% 
  mutate(DD_LAT = SILOCgetLAT12(SILOC), DD_LON = SILOCgetLON12(SILOC))

CORP_WBY <- CORP_WBY %>% select(WATERBODY_NAME, FN2_WBY, DD_LAT, DD_LON)

# FN2 Files
allfiles <- dir("FN2Data/45D/Ia01_STO", recursive = T, full.names = T)
dbffiles <- allfiles[str_detect(allfiles, pattern = "DBF$")]

# GL Template 5 table names
dbase_template <- "Great_Lakes_Assessment_Template_5.accdb"
conn_template <- odbcConnectAccess2007(dbase_template, uid = "", pwd = "")
fn011_names <- sqlColumns(conn_template, "FN011")$COLUMN_NAME
fn012_names <- sqlColumns(conn_template, "FN012")$COLUMN_NAME
fn022_names <- sqlColumns(conn_template, "FN022")$COLUMN_NAME
fn026_names <- sqlColumns(conn_template, "FN026")$COLUMN_NAME
fn026_sub_names <- sqlColumns(conn_template, "FN026_Subspace")$COLUMN_NAME
fn028_names <- sqlColumns(conn_template, "FN028")$COLUMN_NAME
fn121_names <- sqlColumns(conn_template, "FN121")$COLUMN_NAME
fn122_names <- sqlColumns(conn_template, "FN122")$COLUMN_NAME
fn123_names <- sqlColumns(conn_template, "FN123")$COLUMN_NAME
fn123nonfish_names <- sqlColumns(conn_template, "FN123_NonFish")$COLUMN_NAME
fn124_names <- sqlColumns(conn_template, "FN124")$COLUMN_NAME
fn125_names <- sqlColumns(conn_template, "FN125")$COLUMN_NAME
fn125lamprey_names <- sqlColumns(conn_template, "FN125_lamprey")$COLUMN_NAME
fn127_names <- sqlColumns(conn_template, "FN127")$COLUMN_NAME
odbcClose(conn_template)

# FN011
FN011 <- read.dbf(dbffiles[str_detect(dbffiles, pattern = "FN011")])
FWIN_WBY <- inner_join(FN011, CORP_WBY, by = c('WBY' = "FN2_WBY")) # CORP_WBY needs PRJ_CD downstream

setdiff(fn011_names, names(FN011))
FN011 <- FN011 %>% 
  mutate(PROTOCOL = "FWIN", LAKE = WBY_NM) %>% 
  mutate(PRJ_DATE0 = fix_date(PRJ_DATE0), PRJ_DATE1 = fix_date(PRJ_DATE1)) %>% 
  mutate(YEAR = year(PRJ_DATE0))
  
FN011 <- FN011 %>% select(all_of(fn011_names))


# FN012
FN012 <- read.dbf(dbffiles[str_detect(dbffiles, pattern = "FN012")])
FN012_glfishr <- read_csv("FN012_ALL.csv")
setdiff(names(FN012_glfishr), names(FN012))
FN012 <- left_join(FN012, FN012_glfishr)
setdiff(fn012_names, names(FN012)) # TISSUE, AGEST, LAMSAM
FN012 <- FN012 %>% 
  mutate(TISSUE = 1, AGEST = 1, LAMSAM = 0)
setdiff(fn012_names, names(FN012)) # should match now
names(FN012) == fn012_names
names(FN012)
fn012_names
FN012 <- FN012 %>% select(all_of(fn012_names))

# FN022
FN022 <- read.dbf(dbffiles[str_detect(dbffiles, pattern = "FN022")])
FN022 <- FN022 %>% select(all_of(fn022_names)) %>% 
  mutate(SSN_DATE0 = fix_date(SSN_DATE0), SSN_DATE1 = fix_date(SSN_DATE1))

# FN026
FN026 <- read.dbf(dbffiles[str_detect(dbffiles, pattern = "FN026")])
FN026 <- left_join(FN026, FWIN_WBY, by = "PRJ_CD")
FN026 <- FN026 %>% 
  select(all_of(fn026_names)) 

FN026_SUBSPACE <- FN026 %>% 
  mutate(SUBSPACE = SPACE,
        SUBSPACE_DES = SPACE_DES) %>% 
  mutate(SUBSPACE_WT = NA)

FN026_SUBSPACE <- FN026_SUBSPACE %>% select(all_of(fn026_sub_names))

# FN028
FN028 <- read.dbf(dbffiles[str_detect(dbffiles, pattern = "FN028")])
FN028 <- FN028 %>% select(all_of(fn028_names)) 
FN028$GR <- "FWIN"
FN028 <- FN028 %>% 
  # mutate(EFFTM0_GE = as.character(EFFTM0_GE),
  #       EFFTM0_LT = as.character(EFFTM0_LT))
mutate(EFFTM0_GE = "08:00:00",
        EFFTM0_LT = "14:00:00")

# FN121
FN121 <- read.dbf(dbffiles[str_detect(dbffiles, pattern = "FN121")])
FN121 <- FN121 %>% 
  mutate(EFFDT0 = fix_date(EFFDT0), EFFDT1 = fix_date(EFFDT1)) %>% 
  mutate(EFFDUR = ymd_hm(paste(EFFDT1, EFFTM1, sep = " "))-ymd_hm(paste(EFFDT0, EFFTM0, sep = " "))) %>% 
  mutate(EFFDUR = round(as.numeric(EFFDUR), 2)) %>% 
  mutate(EFFTM1 = paste0(as.character(EFFTM1), ":00"),
        EFFTM0 = paste0(as.character(EFFTM0), ":00")
) %>% 
  mutate(SAM = as.numeric(SAM)) %>% 
  mutate(PROCESS_TYPE = 1) %>% # 1 = by net, 3 = panel group
  mutate(SSN = FN022$SSN) %>% # only works if there is one season 
  mutate(SUBSPACE = AREA) %>% # hack - likely needs a FN026_subspace table
  mutate(MODE = FN028$MODE) # only works if there is one mode

FN121 <- left_join(FN121, FWIN_WBY, by = "PRJ_CD") %>% 
  rename(DD_LON0 = DD_LON, DD_LAT0 = DD_LAT)

missing_cols <- setdiff(fn121_names, names(FN121))
for (col in missing_cols) {
  FN121[[col]] <- NA
}

FN121 <- FN121 %>% select(all_of(fn121_names))

# this isn't needed for migration but is a good check
library(leaflet)
leaflet(FN121) %>% addTiles() %>% 
  addMarkers(lng = ~DD_LON0, lat = ~DD_LAT0, 
    popup = ~SAM,    
    clusterOptions = markerClusterOptions(
      spiderfyOnEveryZoom = TRUE,  # keep spiderfied at max zoom when appropriate
      spiderfyDistanceMultiplier = 1.2,
      showCoverageOnHover = FALSE
    )
  )

# FN122
# Gear Tables
FN013 <- read.dbf(dbffiles[str_detect(dbffiles, pattern = "FN013")])
FN014 <- read.dbf(dbffiles[str_detect(dbffiles, pattern = "FN014")])

fn122_names

FN122 <- FN121 %>% select(PRJ_CD, SAM, SIDEP0, SIDEP1) %>% 
  rename(GRDEP0 = SIDEP0, 
        GRDEP1 = SIDEP1) %>% 
  mutate(EFF = FN014$EFF, EFFDST = FN013$EFFDST)

missing_cols <- setdiff(fn122_names, names(FN122))
for (col in missing_cols) {
  FN122[[col]] <- NA
}

FN122$WATERHAUL <- as.character(FN122$WATERHAUL)

FN122 <- FN122 %>% select(all_of(fn122_names))
FN123 <- read.dbf(dbffiles[str_detect(dbffiles, pattern = "FN123")]) %>% 
    mutate(SAM = as.numeric(SAM))

## need to check FN123 catches for empty nets.
emptynets <- anti_join(FN122, FN123) 

if(nrow(emptynets)==0) {
  FN122$WATERHAUL <- 0
}else{
  emptynets$WATERHAUL <- 1
  FN122 <- rows_update(FN122, emptynets, by= c("PRJ_CD", "SAM"))
}

# FN123
missing_cols <- setdiff(fn123_names, names(FN123))
for (col in missing_cols) {
  FN123[[col]] <- NA
}

FN123$GRP <- "00"
FN123 <- FN123 %>% select(all_of(fn123_names))

# FN124 - if needed. Most FWINs likely don't use FN124
# FN124 <- read.dbf(dbffiles[str_detect(dbffiles, pattern = "FN124")])

# FN125
FN125 <- read.dbf(dbffiles[str_detect(dbffiles, pattern = "FN125")]) %>% 
    mutate(SAM = as.numeric(SAM),
          FISH = as.numeric(FISH))

missing_cols <- setdiff(fn125_names, names(FN125))
for (col in missing_cols) {
  FN125[[col]] <- NA
}

FN125$GRP <- "00"
FN125$FATE <- "K"
FN125 <- FN125 %>% select(all_of(fn125_names))

# FN127
FN127 <- read.dbf(dbffiles[str_detect(dbffiles, pattern = "FN127")]) %>% 
    mutate(SAM = as.numeric(SAM),
          FISH = as.numeric(FISH))

missing_cols <- setdiff(fn127_names, names(FN127))
for (col in missing_cols) {
  FN127[[col]] <- NA
}

FN127$GRP <- "00"
FN127$PREFERRED <- 1
FN127 <- FN127 %>% select(all_of(fn127_names))

# there seems to be, at least for this project a mismatch in fish numbers in 
# FN125 and FN127 - possible renumbering before sending to aging lab?

FN127 <- semi_join(FN127, FN125) # returns on FN127 records with known parent in FN125

# Create T5 data base
dbase_write <- file.path("TemplatedData", paste0(FN011$PRJ_CD, "_T5.accdb"))
if(file.exists(dbase_write)) {file.remove(dbase_write)} # remove any previous versions
file.copy(dbase_template, dbase_write) # write blank database

# Write new data to template DB
conn_write <- odbcConnectAccess2007(dbase_write, uid = "", pwd = "")
isverbose = FALSE
sqlSave(conn_write, FN011, tablename = "FN011", append = TRUE, rownames = FALSE, verbose = isverbose)
sqlSave(conn_write, FN012, tablename = "FN012", append = TRUE, rownames = FALSE, verbose = isverbose)
sqlSave(conn_write, FN022, tablename = "FN022", append = TRUE, rownames = FALSE, verbose = isverbose)
sqlSave(conn_write, FN026, tablename = "FN026", append = TRUE, rownames = FALSE, verbose = isverbose)
sqlSave(conn_write, FN026_SUBSPACE, tablename = "FN026_Subspace", append = TRUE, rownames = FALSE, verbose = isverbose)
sqlSave(conn_write, FN028, tablename = "FN028", append = TRUE, rownames = FALSE, verbose = isverbose)
sqlSave(conn_write, FN121, tablename = "FN121", append = TRUE, rownames = FALSE, verbose = isverbose)
sqlSave(conn_write, FN122, tablename = "FN122", append = TRUE, rownames = FALSE, verbose = isverbose)
sqlSave(conn_write, FN123, tablename = "FN123", append = TRUE, rownames = FALSE, verbose = isverbose)
sqlSave(conn_write, FN125, tablename = "FN125", append = TRUE, rownames = FALSE, verbose = isverbose)
sqlSave(conn_write, FN127, tablename = "FN127", append = TRUE, rownames = FALSE, verbose = isverbose)
odbcClose(conn_write)

odbcCloseAll()

# end