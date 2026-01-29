/* ============================================================
   PROJECT  : Economic Census Data Scraping & Consolidation
   PURPOSE  : Cleansing, deduplication, and consolidation of
              multi-part scraped business data into a unified
              Economic Census dataset
   CONTEXT  : Economic Census 2026 Preparation
   DBMS     : MySQL 8.0+
   AUTHOR   : Erkhamuddin & Team
   ============================================================ */


/* ============================================================
   STEP 0: ENVIRONMENT CHECK
   ============================================================ */
SHOW VARIABLES LIKE 'local_infile';
SHOW VARIABLES LIKE 'secure_file_priv';
SET GLOBAL local_infile = 1;


/* ============================================================
   STEP 1: STAGING TABLE (RAW DATA)
   ============================================================
   - Used to load each CSV part independently
   - All columns stored as TEXT to avoid schema conflicts
   - Acts as a temporary landing zone
   ============================================================ */
CREATE TABLE IF NOT EXISTS sensus_ekonomi_staging (
    idsbr TEXT,
    `Query` TEXT,
    `Actual Place Name` TEXT,
    `Category` TEXT,
    `Rating` TEXT,
    `Address` TEXT,
    `Phone Number` TEXT,
    `Website` TEXT,
    `Latitude` TEXT,
    `Longitude` TEXT,
    `Status` TEXT,
    `Open Status` TEXT,
    `Operation Hours` TEXT,
    `Place` TEXT,
    `Validasi` TEXT
);


/* ============================================================
   STEP 2: LOAD CSV INTO STAGING
   ============================================================
   - Executed repeatedly for each CSV part (Part1–PartN)
   - CSV format standardized using semicolon delimiter
   ============================================================ */
LOAD DATA LOCAL INFILE 'C:/Users/~.csv'
INTO TABLE sensus_ekonomi_staging
FIELDS TERMINATED BY ';'
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;


/* ============================================================
   STEP 3: RAW DATA INSPECTION
   ============================================================ */
SELECT COUNT(*) AS total_row FROM sensus_ekonomi_staging;
SELECT DISTINCT Validasi FROM sensus_ekonomi_staging;


/* ============================================================
   STEP 4: TEXT NORMALIZATION & VALIDATION CLEANSING
   ============================================================
   - Normalizes inconsistent validation labels
   - Removes escape characters and line breaks
   ============================================================ */
UPDATE sensus_ekonomi_staging
SET Validasi =
  CASE
    WHEN LOWER(
           TRIM(
             REPLACE(
               REPLACE(
                 REPLACE(Validasi, '\\', ''),
                 '\r', ''
               ),
               '\n', ''
             )
           )
         ) = 'ditemukan'
    THEN 'Ditemukan'
    ELSE 'Tidak Ditemukan'
  END;


/* ============================================================
   STEP 5: CONSOLIDATED MASTER TABLE
   ============================================================
   - Holds merged data from all CSV parts
   ============================================================ */
CREATE TABLE IF NOT EXISTS sensus_ekonomi_full (
    idsbr TEXT,
    `Query` TEXT,
    `Actual Place Name` TEXT,
    `Category` TEXT,
    `Rating` TEXT,
    `Address` TEXT,
    `Phone Number` TEXT,
    `Website` TEXT,
    `Latitude` TEXT,
    `Longitude` TEXT,
    `Status` TEXT,
    `Open Status` TEXT,
    `Operation Hours` TEXT,
    `Place` TEXT,
    `Validasi` TEXT
);


/* ============================================================
   STEP 6: APPEND STAGING DATA INTO MASTER TABLE
   ============================================================ */
INSERT INTO sensus_ekonomi_full
SELECT *
FROM sensus_ekonomi_staging;


/* ============================================================
   STEP 7: CLEAR STAGING TABLE
   ============================================================
   - Prepares table for next CSV part
   ============================================================ */
DELETE FROM sensus_ekonomi_staging;


/* ============================================================
   STEP 8: DUPLICATE DETECTION
   ============================================================
   - Uses window function to identify duplicate records
   ============================================================ */
WITH duplicate_check AS (
    SELECT *,
           ROW_NUMBER() OVER (
             PARTITION BY
               idsbr,
               `Query`,
               `Actual Place Name`,
               Category,
               Rating,
               Address,
               `Phone Number`,
               Website,
               Latitude,
               Longitude,
               Status,
               `Open Status`,
               `Operation Hours`,
               Place,
               Validasi
           ) AS rn
    FROM sensus_ekonomi_full
)
SELECT *
FROM duplicate_check
WHERE rn > 1;


/* ============================================================
   STEP 9: CREATE CLEAN & DEDUPLICATED TABLE
   ============================================================ */
CREATE TABLE sensus_ekonomi_clean AS
SELECT DISTINCT
    idsbr,
    `Query`,
    `Actual Place Name`,
    Category,
    Rating,
    Address,
    `Phone Number`,
    Website,
    Latitude,
    Longitude,
    Status,
    `Open Status`,
    `Operation Hours`,
    Place,
    Validasi
FROM sensus_ekonomi_full;


/* ============================================================
   STEP 10: EXPORT CLEAN DATA (VALID RECORDS ONLY)
   ============================================================
   - Filters only confirmed businesses ("Ditemukan")
   - Adds sequential ID for downstream analysis
   ============================================================ */
SELECT
  'idsbr',
  'Query',
  'Actual Place Name',
  'Category',
  'Rating',
  'Address',
  'Phone Number',
  'Website',
  'Latitude',
  'Longitude',
  'Status',
  'Open Status',
  'Operation Hours',
  'Place',
  'Validasi',
  'ids'
UNION ALL
SELECT
    idsbr,
    `Query`,
    `Actual Place Name`,
    Category,
    Rating,
    Address,
    `Phone Number`,
    Website,
    Latitude,
    Longitude,
    Status,
    `Open Status`,
    `Operation Hours`,
    Place,
    Validasi,
    ROW_NUMBER() OVER () AS ids
FROM sensus_ekonomi_clean
WHERE Validasi = 'Ditemukan'
INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Sensus_Ekonomi_2026_CLEAN.csv'
FIELDS TERMINATED BY ';'
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n';

/* ============================================================
   STEP 11: FINAL TYPED DATASET GENERATION & EXPORT
   ============================================================
   - Converts cleansed TEXT-based data into strongly typed schema
   - Applies explicit casting for numeric & geographic fields
   - Generates sequential unique identifiers
   - Exports analysis-ready Economic Census dataset
   ============================================================ */
   
   CREATE TABLE sensus_ekonomi_final (
    ids                BIGINT,
    idsbr              VARCHAR(64),
    `Query`             VARCHAR(255),
    `Actual Place Name` VARCHAR(255),
    Category            VARCHAR(100),
    Rating              DECIMAL(3,2),
    Address             TEXT,
    `Phone Number`      VARCHAR(32),
    Website             VARCHAR(255),
    Latitude            DECIMAL(10,7),
    Longitude           DECIMAL(10,7),
    Status              VARCHAR(50),
    `Open Status`       VARCHAR(50),
    `Operation Hours`   TEXT,
    Place               VARCHAR(150),
    Validasi            ENUM('Ditemukan','Tidak Ditemukan')
);

INSERT INTO sensus_ekonomi_final (
    ids,
    idsbr,
    `Query`,
    `Actual Place Name`,
    Category,
    Rating,
    Address,
    `Phone Number`,
    Website,
    Latitude,
    Longitude,
    Status,
    `Open Status`,
    `Operation Hours`,
    Place,
    Validasi
)
SELECT
    ROW_NUMBER() OVER () AS ids,
    idsbr,
    `Query`,
    `Actual Place Name`,
    Category,
    NULLIF(Rating,'') + 0.0,
    Address,
    `Phone Number`,
    NULLIF(Website,''),
    NULLIF(Latitude,'') + 0.0,
    NULLIF(Longitude,'') + 0.0,
    Status,
    `Open Status`,
    `Operation Hours`,
    Place,
    Validasi
FROM sensus_ekonomi_clean
WHERE Validasi = 'Ditemukan';

/* NOTE:
   Internal IDs used for cleansing/deduplication only;
   excluded from final output to preserve original identifiers.
*/

SELECT
  'idsbr',
  'Query',
  'Actual Place Name',
  'Category',
  'Rating',
  'Address',
  'Phone Number',
  'Website',
  'Latitude',
  'Longitude',
  'Status',
  'Open Status',
  'Operation Hours',
  'Place',
  'Validasi'
UNION ALL
SELECT
  idsbr,
  `Query`,
  `Actual Place Name`,
  Category,
  Rating,
  Address,
  `Phone Number`,
  Website,
  Latitude,
  Longitude,
  Status,
  `Open Status`,
  `Operation Hours`,
  Place,
  Validasi
FROM sensus_ekonomi_final
INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Sensus_Ekonomi_2026_FINAL.csv'
FIELDS TERMINATED BY ';'
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n';
