library(tidyverse)
library(RJDBC)
library(keyring)
library(lubridate)
library(readxl)
library(reticulate)

# read most recent manager file

driver.sup <- read_excel(
paste("CO2 Report ",
      paste(month(floor_date(today(), "week", 1)),
            day(floor_date(today(), "week", 1)),
            (year(floor_date(today(), "week", 1)) - 2000), sep = "-"),
      ".xlsx",
      sep = ""))

old <- read.csv("outputs/data.csv", stringsAsFactors = FALSE) %>%
  rename(`Drivers Name` = Drivers.Name,
         `Driver Sup` = Driver.Sup,
         `HANA ship` = HANA.ship) %>%
# mutate(Date = as.Date(Date, "%m/%d/%Y"))
mutate(Date = as.Date(Date))

py_run_string("from shareplum import Site")
py_run_string("from shareplum import Office365")
py_run_string("import pandas as pd")

py_run_string("authcookie = Office365('https://cocacolaflorida.sharepoint.com', username='fl014036@cocacolaflorida.com', password='Anime8!cfcokU').GetCookies()")

py_run_string("site = Site('https://cocacolaflorida.sharepoint.com/Distribution/', authcookie=authcookie)")

py_run_string("sp_list = site.List('Hazardous Material Disclosure Statement') ")

py_run_string("data = sp_list.GetListItems()")

py_run_string("data_df = pd.DataFrame(data)")

py_run_string("data_df = data_df[['Date','Drivers Name', 'Shipment number', 'DSD Location']]")

py_run_string("print(data_df)")

SP.shipments <- py$data_df

# import and establish hana connection ----

options(java.parameters = "-Xmx8048m")
# memory.limit(size=10000000000024)

# classPath="C:/Program Files/sap/hdbclient/ngdbc.jar"
# For ngdbc.jar use        # jdbcDriver <- JDBC(driverClass="com.sap.db.jdbc.Driver",
# For HANA Studio jar use  # jdbcDriver <- JDBC(driverClass="com.sap.ndb.studio.jdbc.JDBCConnection",

jdbcDriver <- JDBC(driverClass="com.sap.db.jdbc.Driver",
                   classPath="C:/Program Files/sap/hdbclient/ngdbc.jar")

jdbcConnection <- dbConnect(jdbcDriver,
                            "jdbc:sap://vlpbid001.cokeonena.com:30015/_SYS_BIC",
                            "fl014036",
                            key_get("hana.pw"))


# Fetch all results

sql <- "SELECT TO_Number(\"ActualDeliveryDate\") as \"HANA date\",
\"DriverDesc\",
\"DriverSupervisorName\",
\"ShipmentNumber\" as \"HANA ship\"
FROM \"cona-reporting.delivery-execution::Q_CA_S_DeliveryExecution\"(
            'PLACEHOLDER' = ('$$IP_KeyDate$$', '202001')) where
\"PackTypeDesc\" = ? AND
\"ActualDeliveryDate\" > ? AND
\"ActualDeliveryDate\" < ? AND
\"Loaded\" > ?"

param1 <- 'CO2 Tank'
param2 <- gsub("-", "", max(old$Date)) # date of last in old
param3 <- gsub("-", "", Sys.Date()) # current date
param4 <- '0'

hana.shipments <- dbGetQuery(jdbcConnection, sql, param1, param2, param3, param4)

sql <- "SELECT * FROM \"ccf-edw.self-service.MDM::Q_CA_R_CCF_FISCAL_CAL\""

fiscal.cal <- dbGetQuery(jdbcConnection, sql)

dbDisconnect(jdbcConnection)

rm(jdbcConnection, jdbcDriver, param1, param2, sql)

# do join ----

class(SP.shipments$`Shipment number`)

SP.shipments$`Shipment number` <- unlist(SP.shipments$`Shipment number`)

class(SP.shipments$`DSD Location`)

SP.shipments$`DSD Location` <- unlist(SP.shipments$`DSD Location`)

SP.shipments <- SP.shipments %>%
  rename(SP.ship = `Shipment number`) %>%
  unique()

df <- hana.shipments %>%
  filter(!is.na(`HANA ship`)) %>%
  mutate(`HANA ship` = substring(`HANA ship`, 3)) %>%
  left_join(SP.shipments, by = c("HANA ship" = "SP.ship")) %>%
  select(-c(Date)) %>%
  rename(Date = `HANA date`) %>%
  select(-c(`Drivers Name`)) %>%
  rename(`Drivers Name` = DriverDesc) %>%
  mutate(DriverSupervisorName = recode("SOUTHWELL CRAIG" = "SNOWDEN HAROLD",
                                       "ROBERTS NATHAN" = "JANER HENRY",
                                       DriverSupervisorName)) %>%
  left_join(driver.sup %>%
              select("First Name", "Last Name", "Manager Display Name") %>%
              mutate(Driver = toupper(paste(`First Name`, `Last Name`))) %>%
              mutate("Manager Display Name" = sapply(strsplit(`Manager Display Name`, split=" "),function(x)
              {paste(rev(x),collapse=" ")}),
              "Manager Display Name" = toupper(`Manager Display Name`)) %>% unique(), by = c("Drivers Name" = "Driver")) %>%
  mutate(MATCH = ifelse(is.na(`DSD Location`), "NO FORM", "FORM FILLED"),
         Date = as.character(Date), Date = ymd(Date),
         MATCH.binary = ifelse(MATCH == "FORM FILLED", 1, 0)) %>%
  select(-c(`DSD Location`)) %>%
  left_join(fiscal.cal %>%
              mutate(Date = as.Date(Date)), by = "Date") %>%
  mutate(`Driver Sup` = DriverSupervisorName)

df$`Driver Sup`[is.na(df$`Driver Sup`)] <- as.character(df$`Manager Display Name`[is.na(df$`Driver Sup`)])

df <- df %>% drop_na(`Driver Sup`)

df <- df %>%
  left_join(driver.sup %>%
  select("Manager Display Name", "Facility Name") %>%
  mutate("Manager Display Name" = sapply(strsplit(`Manager Display Name`, split=" "),function(x)
  {paste(rev(x),collapse=" ")}),
  "Manager Display Name" = toupper(`Manager Display Name`)) %>% unique(), by = c("Driver Sup" = "Manager Display Name")) %>%
  left_join(data.frame(`Location Code` = c("I000",
                                       "I001",
                                       "I002",
                                       "I003",
                                       "I004",
                                       "I005",
                                       "I006",
                                       "I007",
                                       "I010",

                                       "I012",
                                       "I013",

                                       "I017",
                                       "I018",
                                       "I019",
                                       "I020",
                                       "I000"),
                       Location = c("Tampa Madison",
                                    "Orlando",
                                    "Jacksonville",
                                    "Tampa Sabal",
                                    "Lakeland",
                                    "Sarasota",
                                    "Ft Myers",
                                    "Ft Pierce",
                                    "Daytona",

                                    "Gainesville",
                                    "Brevard",

                                    "Broward",
                                    "Palm Beach",
                                    "Miami Dade",
                                    "The Keys",
                                    "Tampa Madison"),
                       `Facility Name` = c("Tampa DC (Madison) - 3C05",
                                           "Orlando Combo Center - 3C14",
                                           "Jacksonville Combo Center - 3C13",
                                           "Tampa PC (Sabal) - 3C09",
                                           "Lakeland Distribution Center - 3C03",
                                           "Sarasota Distribution Center - 3C04",
                                           "Ft. Myers Distribution Center - 3C01",
                                           "Ft. Pierce Distribution Center - 3C02",
                                           "Daytona Beach Distribution Center - 3C11",

                                           "Gainesville Distribution Center - 3C12",
                                           "Brevard Distribution Center - 3C10",

                                           "Broward Combo Center - 3C19",
                                           "Palm Beach Distribution Center - 3C22",
                                           "Miami-Dade Distribution Center - 3C21",
                                           "The Keys Distribution Center - 3C18",
                                           "Spring Hill Distribution Center - 3C23"
                                           )), by = c("Facility Name" = "Facility.Name")) %>%
  select(Date, "Drivers Name", "Driver Sup", "HANA ship", Location.Code, MATCH, MATCH.binary, FiscalYearPeriod, FiscalYearWeek, Location)

df <- rbind(old, df)

missing <- read.csv("missing.csv", stringsAsFactors = FALSE)

df <- df %>%
  rename(Driver.Sup = `Driver Sup`) %>%
  left_join(missing, by= "Driver.Sup") %>%
  mutate(Location = coalesce(Location.x, Location.y),
         Location.Code = coalesce(Location.Code.x, Location.Code.y)) %>%
  select(-c(Location.x, Location.y, Location.Code.x, Location.Code.y)) %>%
  rename(`Driver Sup` = Driver.Sup)


missing <- df %>%
  select(`Driver Sup`, Location)

missing <- missing[is.na(missing$Location),] %>% unique()

write.csv(df, "outputs/data.csv", row.names = FALSE)
