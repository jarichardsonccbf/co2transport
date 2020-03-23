#C:/Users/fl014036/Documents/R/R-3.6.2/library/taskscheduleR/extdata

library(tidyverse)
library(RJDBC)
library(keyring)
library(lubridate)
library(readxl)
library(reticulate)

py_run_string("from shareplum import Site")
py_run_string("from shareplum import Office365")
py_run_string("import pandas as pd")

py_run_string("authcookie = Office365('https://cocacolaflorida.sharepoint.com', username='SharePointAdmin@cocacolaflorida.com', password='P@$$word').GetCookies()")

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
\"ShippingPoint\",
\"ShipmentNumber\" as \"HANA ship\",
\"PackTypeDesc\",
\"TransportationPlanningPoint\",
\"SalesOffice\" FROM \"cona-reporting.delivery-execution::Q_CA_S_DeliveryExecution\"(
            'PLACEHOLDER' = ('$$IP_KeyDate$$', '202001')) where
\"PackTypeDesc\" = ? AND
\"ActualDeliveryDate\" > ?"

param1 <- 'CO2 Tank'
param2 <- '20190101'

hana.shipments <- dbGetQuery(jdbcConnection, sql, param1, param2)

sql <- "SELECT * FROM \"ccf-edw.self-service.MDM::Q_CA_R_CCF_FISCAL_CAL\""

fiscal.cal <- dbGetQuery(jdbcConnection, sql)

# sql <- 'SELECT \"CustomerNumber\",
#                \"DeliveringPlant\"
#          FROM \"cona-mdm::Q_CA_R_MDM_Customer_GeneralSalesArea\"'
#
# delivery <- dbGetQuery(jdbcConnection, sql)

dbDisconnect(jdbcConnection)

rm(jdbcConnection, jdbcDriver, param1, param2, sql)

# do join ----

class(SP.shipments$`Shipment number`)

SP.shipments$`Shipment number` <- unlist(SP.shipments$`Shipment number`)

class(SP.shipments$`DSD Location`)

SP.shipments$`DSD Location` <- unlist(SP.shipments$`DSD Location`)

SP.shipments <- SP.shipments %>%
  rename(SP.ship = `Shipment number`)

df <- hana.shipments %>%
  filter(!is.na(`HANA ship`)) %>%
  mutate(`HANA ship` = substring(`HANA ship`, 3)) %>%
  left_join(SP.shipments, by = c("HANA ship" = "SP.ship")) %>%
  select(-c(PackTypeDesc, Date)) %>%
  rename(Date = `HANA date`) %>%
  select(-c(`Drivers Name`)) %>%
  rename(`Drivers Name` = DriverDesc) %>%
  mutate(MATCH = ifelse(is.na(`DSD Location`), "NO FORM", "FORM FILLED"),
         Date = as.character(Date),
         Date = ymd(Date),
         MATCH.binary = ifelse(MATCH == "FORM FILLED", 1, 0)) %>%
  select(-c(`DSD Location`)) %>%
  left_join(fiscal.cal %>%
              mutate(Date = as.Date(Date)), by = "Date") %>%
  left_join(data.frame(SalesOffice = c("I000",
                                       "I001",
                                       "I002",
                                       "I003",
                                       "I004",
                                       "I005",
                                       "I006",
                                       "I007",
                                       "I010",
                                       "I011",
                                       "I012",
                                       "I013",
                                       "I015",
                                       "I017",
                                       "I018",
                                       "I019",
                                       "I020",
                                       "I021",
                                       "I022"),
                       Location = c("Tampa Madison",
                                    "Orlando",
                                    "Jacksonville",
                                    "Tampa Sabal",
                                    "Lakeland",
                                    "Sarasota",
                                    "Ft Myers",
                                    "Ft Pierce",
                                    "Daytona",
                                    "Orlando MRC",
                                    "Gainesville",
                                    "Brevard",
                                    "Jacksonville H2",
                                    "Broward",
                                    "Palm Beach",
                                    "Miami Dade",
                                    "The Keys",
                                    "Seneca",
                                    "Cabot")), by = "SalesOffice") %>%
  select(Date, "Drivers Name", DriverSupervisorName, "HANA ship", SalesOffice, MATCH, MATCH.binary, FiscalYearPeriod, FiscalYearWeek, Location) %>%
  left_join(read_excel("CO2 Report 2-25-20.xlsx") %>%
              select("First Name", "Last Name", "Manager Display Name") %>%
              mutate(Driver = toupper(paste(`First Name`, `Last Name`))) %>%
              select("Driver", "Manager Display Name"), by = c("Drivers Name" = "Driver"))

df$DriverSupervisorName[is.na(df$DriverSupervisorName)] <- as.character(df$`Manager Display Name`[is.na(df$DriverSupervisorName)])

df <- df %>%
  select(-c(`Manager Display Name`)) %>%
  rename(`Driver Supervisor` = DriverSupervisorName,
         Driver = `Drivers Name`) %>%
  mutate(`Driver Supervisor` = toupper(`Driver Supervisor`))

write.csv(df, "outputs/data.csv", row.names = FALSE)
