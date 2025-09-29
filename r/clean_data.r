# --- 1. SETUP: Load libraries and data ---

# Load necessary libraries for data manipulation, date handling, string operations, and language detection.
# install.packages(c("data.table", "lubridate", "stringr", "arrow", "cld3"))
library(data.table)
library(lubridate)
library(stringr)
library(arrow)
library(cld3)

# Define the path to your input RDS file
input_file_path <- "data/final_data.rds"

# Load the dataset
if (!file.exists(input_file_path)) {
  stop("Input file not found. Please check the path.")
}
results_dt <- as.data.table(readRDS(input_file_path))


# --- 2. DEFINE OUTPUT DIRECTORY ---

# Define the target directory for all output files from this script.
output_dir <- "data"

# Create the directory if it doesn't exist to prevent errors.
if (!dir.exists(output_dir)) {
  dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)
}


# --- 3. CLEANING: Sequentially filter the dataset ---

# Create a working copy to preserve the original dataset.
final_dt <- copy(results_dt)

# Initialize a list to efficiently collect all rows that are filtered out.
# This is much faster than repeatedly adding rows to a data.frame.
excluded_chunks <- list()


## Step 3.1: Remove duplicate articles based on URL
# ---
# Find all rows where the URL has appeared before in the dataset.
duplicate_rows <- final_dt[duplicated(final_dt, by = "url")]

if (nrow(duplicate_rows) > 0) {
  # Add the reason for exclusion.
  duplicate_rows[, exclusion_reason := "duplicate"]
  
  # Add this chunk to the list of excluded data.
  excluded_chunks <- append(excluded_chunks, list(duplicate_rows))
  
  # Keep only the unique URLs in the main dataset.
  final_dt <- unique(final_dt, by = "url")
}


## Step 3.2: Remove articles outside the specified date range
# ---
# Define the valid date range for the dataset.
start_date <- as.POSIXct("2025-01-01 00:00:00", tz = "UTC")
end_date <- as.POSIXct("2025-02-23 23:59:59", tz = "UTC")

# Ensure the 'date_time' column is in the correct POSIXct format.
final_dt[, date_time := as.POSIXct(date_time, tz = "UTC")]

# Find all articles that fall outside this range.
out_of_range_rows <- final_dt[date_time < start_date | date_time > end_date]

if (nrow(out_of_range_rows) > 0) {
  # Add the reason for exclusion.
  out_of_range_rows[, exclusion_reason := "out_of_date_range"]
  
  # Add this chunk to the list of excluded data.
  excluded_chunks <- append(excluded_chunks, list(out_of_range_rows))
  
  # Keep only the articles within the valid date range.
  final_dt <- final_dt[date_time >= start_date & date_time <= end_date]
}


## Step 3.3: Remove non-German articles
# ---
# Apply language detection to the 'text' column of the remaining articles.
# This is done last as it is a computationally intensive step.
final_dt[, language := detect_language(text)]

# Find all articles not identified as German ('de') or where language is NA.
non_german_rows <- final_dt[language != "de" | is.na(language)]

if (nrow(non_german_rows) > 0) {
  # Remove the temporary 'language' column before adding to the excluded list.
  non_german_rows[, language := NULL]
  
  # Add the reason for exclusion.
  non_german_rows[, exclusion_reason := "non_german"]
  
  # Add this chunk to the list of excluded data.
  excluded_chunks <- append(excluded_chunks, list(non_german_rows))
  
  # Keep only the articles identified as German.
  final_dt <- final_dt[language == "de"]
}

# Remove the temporary 'language' helper column from the final dataset.
final_dt[, language := NULL]


# --- 4. FINALIZE EXCLUDED DATA: Combine all excluded rows ---

# Combine all the excluded data chunks into a single data.table.
# `rbindlist` is a very fast way to combine a list of data.tables.
excluded_dt <- rbindlist(excluded_chunks, use.names = TRUE, fill = TRUE)


# --- 5. TRANSFORMATION: Re-index, convert, and calculate columns ---

# Re-generate the 'id' column to be a simple, clean sequence from 1 to n.
final_dt[, id := .I]

# Convert the 'paywall' column from boolean (TRUE/FALSE) to integer (1/0).
final_dt[, paywall := as.integer(as.logical(paywall))]

# Calculate text length as word count.
final_dt[, text_length := ifelse(is.na(text), 0, str_count(text, "\\S+"))]


# --- 6. FINALIZING: Select and order columns ---

# Define the final set of columns and their order for the output dataset.
final_columns <- c(
  "id",
  "domain",
  "url",
  "date_time",
  "headline",
  "author",
  "text",
  "paywall",
  "text_length"
)

# Select and reorder the columns.
final_dt <- final_dt[, ..final_columns]


# --- 7. OUTPUT: Save the clean and excluded datasets ---

# Define the full, writable paths for the final output files.
output_csv_path <- file.path(output_dir, "huggingface_news_dataset.csv")
output_rds_path <- file.path(output_dir, "huggingface_news_dataset.rds")
excluded_rds_path <- file.path(output_dir, "excluded_articles.rds")

# Save the final, cleaned data to CSV and RDS files.
fwrite(final_dt, output_csv_path, na = "") # Write NA as empty string
saveRDS(final_dt, output_rds_path)

# Save the data that was filtered out to an RDS file.
if (nrow(excluded_dt) > 0) {
  saveRDS(excluded_dt, excluded_rds_path)
}


# --- 8. DESCRIPTIVE ANALYSIS: for the Huggingface data set card: Create a summary table ---

# Perform a quick analysis to get an overview of the data per domain.
# This uses data.table's fast grouping and aggregation capabilities.
domain_summary <- final_dt[, .(
  total_articles = .N,
  paywalled_articles = sum(paywall, na.rm = TRUE) # Since paywall is 1/0, sum() gives the count
), by = domain]

# Calculate the derived columns based on the aggregated data.
domain_summary[, has_paywalled_content := ifelse(paywalled_articles > 0, "Yes", "No")]
domain_summary[, percentage_without_paywall := ((total_articles - paywalled_articles) / total_articles) * 100]

# Format the percentage for better readability.
domain_summary[, percentage_without_paywall := sprintf("%.2f%%", percentage_without_paywall)]

# Rename columns to match the requested output.
setnames(domain_summary,
         old = c("domain", "total_articles", "has_paywalled_content", "paywalled_articles", "percentage_without_paywall"),
         new = c("Domain", "Total number of articles", "Does the domain have any paywalled contents?", "Number of paywalled articles", "percentage of complete articles without paywalls"))
