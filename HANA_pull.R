library(tidyverse)
library(RJDBC)
library(keyring)
library(lubridate)

source("SP_pull.Rmd")

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

# "SELECT
# \"SalesOffice\",
# \"_GCC_G_ADDKF1\" as \"Additional Key Figures 1\"
# where
# \"TradeNameText\" = ? OR
# \"TradeNameText\" = ? OR
# \"TradeNameText\" = ? OR
# \"TradeNameText\" = ?

sql <- "SELECT TO_Number(\"ActualDeliveryDate\") as \"HANA date\",
\"DriverDesc\",
\"DriverSupervisorName\",
\"ShipmentNumber\" as \"HANA ship\",
\"PackTypeDesc\",
\"SalesOffice\" FROM \"cona-reporting.delivery-execution::Q_CA_S_DeliveryExecution\"(
            'PLACEHOLDER' = ('$$IP_KeyDate$$', '202001')) where
\"PackTypeDesc\" = ? AND
\"ActualDeliveryDate\" > ?"

param1 <- 'CO2 Tank'
param2 <- '20190101'

hana.shipments <- dbGetQuery(jdbcConnection, sql, param1, param2)

dbDisconnect(jdbcConnection)

rm(jdbcConnection, jdbcDriver, param1, param2, sql)

# do join ----

class(SP.shipments$`Shipment number`)

SP.shipments$`Shipment number` <- unlist(SP.shipments$`Shipment number`)

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
  select(-c(`DSD Location`))

write.csv(df, "outputs/data.csv", row.names = FALSE)
