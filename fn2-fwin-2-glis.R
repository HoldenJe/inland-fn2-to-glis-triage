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
odbcClose(conn_template)

# FN011
FN011 <- read.dbf(dbffiles[str_detect(dbffiles, pattern = "FN011")])
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
FN026 <- FN026 %>% 
  mutate(DD_LAT = NA, DD_LON = NA) %>% 
  select(all_of(fn026_names)) 

# Create T5 data base
dbase_write <- file.path("TemplatedData", paste0(FN011$PRJ_CD, "_T5.accdb"))
if(file.exists(dbase_write)) {file.remove(dbase_write)} # remove any previous versions
file.copy(dbase_template, dbase_write) # write blank database

conn_write <- odbcConnectAccess2007(dbase_write, uid = "", pwd = "")
isverbose = FALSE
sqlSave(conn_write, FN011, tablename = "FN011", append = TRUE, rownames = FALSE, verbose = isverbose)
sqlSave(conn_write, FN012, tablename = "FN012", append = TRUE, rownames = FALSE, verbose = isverbose)
sqlSave(conn_write, FN022, tablename = "FN022", append = TRUE, rownames = FALSE, verbose = isverbose)
sqlSave(conn_write, FN026, tablename = "FN026", append = TRUE, rownames = FALSE, verbose = isverbose)
odbcClose(conn_write)

# end