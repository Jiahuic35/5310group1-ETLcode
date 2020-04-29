# -- START R CODE --

# Load needed packages
require('RPostgreSQL')

#Load the PostgreSQL driver:
drv <- dbDriver('PostgreSQL')

# list credentials here
hostname = 'cu-spring2020-group1.cggz75b61mlh.us-east-2.rds.amazonaws.com'
username = 'postgres'
pwd = 'postgres'
database = 'nyc311'

#Create a connection 
con <- dbConnect(drv, dbname = database,
                 host = hostname, port = 5432,
                 user = username, password = pwd)

# -- END R CODE --

#read csv
df <- read.csv('311.csv')

#make sure there is no null value
summary(df)

stmt = "
CREATE TABLE Agency(
agency_id	integer not null,
agency_abr		varchar(10),
agency_name   varchar(100),
primary key(agency_id)
);

CREATE TABLE Channel(
channel_id	integer not null,
open_data_channel_type		varchar(100),
primary key(channel_id)
);

CREATE TABLE Complain(
complain_id	integer not null,
complain_type		varchar(50),
descriptor      varchar(150),
primary key(complain_id),
foreign key (location_id) references Location (location_id)
);

CREATE TABLE Date(
date_id		integer not null,
created_date		date,
primary key(date_id)
);

CREATE TABLE Case(
case_id		integer not null,
complain_id		integer not null,
agency_id		integer not null,
channel_id 		integer not null,
status_id		integer not null,
date_id		integer not null,
primary key(case_id),
foreign key(complain_id) references Complain(complain_id),
foreign key(agency_id) references Agency(agency_id),
foreign key(channel_id) references Channel(channel_id),
foreign key(status_id) references Status(status_id),
foreign key(date_id) references Date(date_id)
);

CREATE TABLE Zip(
zip_id		integer not null,
incident_zip		varchar(5),
primary key(zip_id)
);


CREATE TABLE Borough(
borough_id		integer not null,
borough		varchar(20),
primary key(borough_id)
);

CREATE TABLE City(
city_id		integer not null,
city		varchar(20),
primary key(city_id)
);

CREATE TABLE Location(
location_id		integer not null,
location_type		varchar(30),
primary key(location_id)
);

CREATE TABLE Status(
status_id	integer not null,
status		varchar(20),
primary key(status_id)
);

CREATE TABLE Address(
address_id		integer not null,
incident_address		varchar(50),
primary key(address_id)
);


CREATE TABLE Street(
street_id		integer not null,
cross_street_1		varchar(50),
cross_street_2		varchar(50),
intersection_street_1   varchar(50),
intersection_street_2   varchar(50),
cross_intersection_street   varchar(200),
primary key(street_id)
);


CREATE TABLE Geography(
geo_id		integer not null,
latitude  numeric(12,10),
longitude numeric(12,10),
latitude_longitude		varchar(50),
primary key(geo_id)
);


CREATE TABLE Coordinates(
coord_id		integer not null,
x_coordinate		integer,
y_coordinate		integer,
x_y_coordinate		varchar(50),
primary key(coord_id)
);

CREATE TABLE Sum_Location(
case_id	integer not null,
location_id	integer not null,
city_id	integer not null,
borough_id	integer not null,
zip_id	integer not null,
address_id	integer not null,
street_id	integer not null,
geo_id	integer not null,
coord_id	integer not null,
primary key(case_id),
foreign key (location_id) references Location(location_id),
foreign key (city_id) references City(city_id),
foreign key (borough_id) references Borough(borough_id),
foreign key (zip_id) references Zip(zip_id),
foreign key (address_id) references Address(address_id),
foreign key (street_id) references Street(street_id),
foreign key (geo_id) references Geography(geo_id),
foreign key (coord_id) references Coordinates(coord_id)

);"

rs <- dbGetQuery(con, stmt)

###complain table
# Create temporary dataframe with unique complain id
temp_complain_df <- data.frame('complain_type' = unique(df$Complaint.Type))


# Add incrementing integers
temp_complain_df$complain_id <- 1:nrow(temp_complain_df)

dbWriteTable(con, name="Complain", value=temp_complain_df, row.names=FALSE, append=TRUE)

complain_id_list <- sapply(df$Complaint.Type, function(x) temp_complain_df$complain_id[temp_complain_df$complain_type == x])

df$complain_id <- complain_id_list


### create status table
# Create temporary dataframe with unique status_id
temp_status_df <- data.frame('status' = unique(df$Status))

# Add incrementing integers
temp_status_df$status_id <- 1:nrow(temp_status_df)

status_id_list <- sapply(df$Status, function(x) temp_status_df$status_id[temp_status_df$status == x])

df$status_id <- status_id_list



###channel table
# Create temporary dataframe with unique channel id
temp_channel_df <- data.frame('Open.Data.Channel.Type' = unique(df$Open.Data.Channel.Type))

# Add incrementing integers
temp_channel_df$channel_id <- 1:nrow(temp_channel_df)

dbWriteTable(con, name="channel", value=temp_channel_df, row.names=FALSE, append=TRUE)

channel_id_list <- sapply(df$Open.Data.Channel.Type, function(x) temp_channel_df$channel_id[temp_channel_df$Open.Data.Channel.Type == x])

df$channel_id <- channel_id_list


###agency table
# Create temporary dataframe with unique agency id
temp_agency_df <- data.frame('Agency' = unique(df$Agency))

# Add incrementing integers
temp_agency_df$agency_id <- 1:nrow(temp_agency_df)

dbWriteTable(con, name="agency", value=temp_agency_df, row.names=FALSE, append=TRUE)

agency_id_list <- sapply(df$Agency, function(x) temp_agency_df$agency_id[temp_agency_df$Agency == x])

df$agency_id <- agency_id_list


###date table
# Create temporary dataframe with unique date id
temp_date_df <- data.frame('created_date' = unique(df$Created.Date))

# Add incrementing integers
temp_date_df$date_id <- 1:nrow(temp_date_df)

dbWriteTable(con, name="date", value=temp_date_df, row.names=FALSE, append=TRUE)

date_id_list <- sapply(df$Created.Date, function(x) temp_date_df$date_id[temp_date_df$created_date == x])

df$date_id <- date_id_list


###case table
names(df)[1]<-paste("case_id")

case_table <- df[c('case_id','complain_id','agency_id','channel_id','status_id')]
dbWriteTable(con, name="case", 
             value=case_table[!duplicated(case_table[c('case_id', 'complain_id','agency_id','channel_id','status_id')]),],
             row.names=FALSE, append=TRUE)


###zip table

# Create temporary dataframe with unique incident_zip
temp_zip_df <- data.frame('incident_zip' = unique(df$Incident.Zip))

# Add incrementing integers
temp_zip_df$zip_id <- 1:nrow(temp_zip_df)

#push the data into the database
dbWriteTable(con, name="zip", value=temp_zip_df, row.names=FALSE, append=TRUE)

# Map zip_id
zip_id_list <- sapply(df$Incident.Zip, function(x) temp_zip_df$zip_id[temp_zip_df$incident_zip == x])

# Add zip_id to the main dataframe
df$zip_id <- zip_id_list


### Borough table

# Create temporary dataframe with unique borough_id
temp_borough_df <- data.frame('borough' = unique(df$Borough))

# Add incrementing integers
temp_borough_df$borough_id <- 1:nrow(temp_borough_df)

#push the borough data into the database
dbWriteTable(con, name="borough", value=temp_borough_df, row.names=FALSE, append=TRUE)

# Map borough_id
borough_id_list <- sapply(df$Borough, function(x) temp_borough_df$borough_id[temp_borough_df$borough == x])

# Add zip_id to the main dataframe
df$borough_id <- borough_id_list


### City table

# Create temporary dataframe with unique city_id
temp_city_df <- data.frame('city' = unique(df$City))

# Add incrementing integers
temp_city_df$city_id <- 1:nrow(temp_city_df)

#push the city data into the database
dbWriteTable(con, name="city", value=temp_city_df, row.names=FALSE, append=TRUE)

# Map borough_id
city_id_list <- sapply(df$City, function(x) temp_city_df$city_id[temp_city_df$city == x])

# Add borough_id to the main dataframe
df$city_id <- city_id_list


###location table

# Create temporary dataframe with unique location_id
temp_location_df <- data.frame('location_type' = unique(df$Location.Type))

# Add incrementing integers
temp_location_df$location_id <- 1:nrow(temp_location_df)

#push the location data into the database
dbWriteTable(con, name="location", value=temp_location_df, row.names=FALSE, append=TRUE)

# Map location_id
location_id_list <- sapply(df$Location.Type, function(x) temp_location_df$location_id[temp_location_df$location_type == x])

# Add location_id to the main dataframe
df$location_id <- location_id_list


###Status

# Create temporary dataframe with unique status_id
temp_status_df <- data.frame('status' = unique(df$Status))

# Add incrementing integers
temp_status_df$status_id <- 1:nrow(temp_status_df)

#push the status data into the database
dbWriteTable(con, name="status", value=temp_status_df, row.names=FALSE, append=TRUE)

# Map status_id
status_id_list <- sapply(df$Status, function(x) temp_status_df$status_id[temp_status_df$status == x])

# Add status_id to the main dataframe
df$status_id <- status_id_list

###address table

# Create temporary dataframe with unique incident_address
temp_add_df <- data.frame('incident_address' = unique(df$Incident.Address))

# Add incrementing integers
temp_add_df$address_id <- 1:nrow(temp_add_df)

#push the data into the database
dbWriteTable(con, name="address", value=temp_add_df, row.names=FALSE, append=TRUE)

# Map address_id
address_id_list <- sapply(df$Incident.Address, function(x) temp_add_df$address_id[temp_add_df$incident_address == x])
class(address_id_list)
# Add zip_id to the main dataframe
df$address_id <- address_id_list

###street table

# Create temporary dataframe with unique street_name

temp_street_df <-df[c('Cross.Street.1', 'Cross.Street.2',
                      'Intersection.Street.1', 'Intersection.Street.2','Cross.Intersection.Street')][!duplicated(df[c( 'Cross.Street.1', 'Cross.Street.2',
                                                                                                                       'Intersection.Street.1', 'Intersection.Street.2','Cross.Intersection.Street')]),]
colnames(temp_street_df) <- c('cross_street_1','cross_street_2','intersection_street_1', 'intersection_street_2','cross_intersection_street')
# Add incrementing integers
temp_street_df$street_id <- 1:nrow(temp_street_df)
class(temp_street_df$street_id)
#push the data into the database
dbWriteTable(con, name="street", value=temp_street_df, row.names=FALSE, append=TRUE)

# Map street_id
street_id_list <- sapply(df$Cross.Intersection.Street, function(x) temp_street_df$street_id[temp_street_df$cross_intersection_street == x])
class(df$Cross.Intersection.Street)
class(street_id_list)
# Add street_id to the main dataframe
df$street_id <- street_id_list

### Geography table 

# Create temporary dataframe with unique latitude_longitude
temp_geo_df <-df[c('Latitude.Longitude','Latitude', 'Longitude')][!duplicated(df[c('Latitude.Longitude','Latitude', 'Longitude')]),]
colnames(temp_geo_df) <- c('latitude_longitude','latitude','longitude')

# Add incrementing integers
temp_geo_df$geo_id <- 1:nrow(temp_geo_df)

#push the geography data into the database
dbWriteTable(con, name="geography", value=temp_geo_df, row.names=FALSE, append=TRUE)

# Map geo_id
geo_id_list <- sapply(df$Latitude.Longitude, function(x) temp_geo_df$geo_id[temp_geo_df$latitude_longitude == x])

# Add geo_id to the main dataframe
df$geo_id <- geo_id_list

head(df$geo_id)

### Coordinates table 

# Create temporary dataframe with unique x_y_coordinate
temp_coord_df <-df[c('X.Coordinate..State.Plane.','Y.Coordinate..State.Plane.', 'X.Y.Coordinate')][!duplicated(df[c('X.Coordinate..State.Plane.','Y.Coordinate..State.Plane.', 'X.Y.Coordinate')]),]
colnames(temp_coord_df) <- c('x_coordinate','y_coordinate','x_y_coordinate')


# Add incrementing integers
temp_coord_df$coord_id <- 1:nrow(temp_coord_df)

#push the geography data into the database
dbWriteTable(con, name="coordinates", value=temp_coord_df, row.names=FALSE, append=TRUE)

# Map geo_id
coord_id_list <- sapply(df$X.Y.Coordinate, function(x) temp_coord_df$coord_id[temp_coord_df$x_y_coordinate == x])

# Add geo_id to the main dataframe
df$coord_id <- coord_id_list

### Sum_Location table 
temp_sum_location_df <-df[c('Unique.Key', 'location_id','city_id','borough_id',
                            'zip_id','address_id','street_id','geo_id','coord_id')][!duplicated(df[c('Unique.Key', 'location_id','city_id','borough_id',
                                                                                                     'zip_id','address_id','street_id','geo_id','coord_id')]),]

colnames(temp_sum_location_df) <- c('case_id','location_id','city_id','borough_id',
                                    'zip_id','address_id','street_id','geo_id','coord_id')

dbWriteTable(con, name="sum_location", value=temp_sum_location_df, row.names=FALSE, append=TRUE)

