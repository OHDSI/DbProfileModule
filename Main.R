# Copyright 2022 Observational Health Data Sciences and Informatics
#
# This file is part of dbProfileModule
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# Module methods -------------------------
execute <- function(jobContext) {
	rlang::inform("Validating inputs")
	checkmate::assert_list(x = jobContext)
	
	if (is.null(jobContext$settings)) {
	  stop("Analysis settings not found in job context")
	}
	if (is.null(jobContext$sharedResources)) {
	  stop("Shared resources not found in job context")
	}
	if (is.null(jobContext$moduleExecutionSettings)) {
	  stop("Execution settings not found in job context")
	}

	if (jobContext$settings$addDQD) {
	  rlang::inform("Executing Achilles (if results are not found) and DQD.")
	} else {
	  rlang::inform("Executing Achilles (if results are not found) only.")
	}
	
	resultsFolder <- jobContext$moduleExecutionSettings$resultsSubFolder
	workFolder <- jobContext$moduleExecutionSettings$workSubFolder
	databaseId <- as.character(jobContext$moduleExecutionSettings$databaseId)
  
	cdmVersion <- getCdmVersion(jobContext$moduleExecutionSettings$connectionDetails)
	
	DbDiagnostics::executeDbProfile(
	  connectionDetails      = jobContext$moduleExecutionSettings$connectionDetails,
	  cdmDatabaseSchema      = jobContext$moduleExecutionSettings$cdmDatabaseSchema,
	  resultsDatabaseSchema  = jobContext$moduleExecutionSettings$workDatabaseSchema,
	  vocabDatabaseSchema    = jobContext$moduleExecutionSettings$cdmDatabaseSchema,
	  cdmSourceName          = databaseId,
	  outputFolder           = workFolder,
	  cdmVersion             = cdmVersion,
	  overwriteAchilles      = jobContext$settings$overwriteAchilles,
	  minCellCount           = jobContext$moduleExecutionSettings$minCellCount,
	  tableCheckThresholds   = jobContext$settings$tableCheckThresholds,
	  fieldCheckThresholds   = jobContext$settings$fieldCheckThresholds,
	  conceptCheckThresholds = jobContext$settings$conceptCheckThresholds,
	  addDQD                 = jobContext$settings$addDQD)
	
	rlang::inform("Formtting Achilles results")
	formatAchillesResults(
	  workFolder = workFolder,
	  resultsFolder = resultsFolder,
	  databaseId = databaseId
	)

	if (jobContext$settings$addDQD) {
		rlang::inform("Formtting DQD results")
		formatDQDResults(
		  workFolder = workFolder,
		  resultsFolder = resultsFolder,
		  databaseId = databaseId
		)
	}
	
	# Copy in the resultsDataModelSpecification.csv
	file.copy(from = "resultsDataModelSpecification.csv",
	          to = file.path(resultsFolder, "resultsDataModelSpecification.csv"))
	
	# Zip the results
	zipFile <- file.path(resultsFolder, "dbProfileResults.zip")
	resultFiles <- list.files(resultsFolder,
	                          pattern = ".*\\.csv$"
	)
	oldWd <- setwd(resultsFolder)
	on.exit(setwd(oldWd), add = TRUE)
	DatabaseConnector::createZipFile(
	  zipFile = zipFile,
	  files = resultFiles
	)
	rlang::inform(paste("Results available at:", zipFile))
}

# Private methods -------------------------
# Format Achilles results
formatAchillesResults <- function(
  workFolder,
	resultsFolder,
	databaseId
) {
	achillesResults <- CohortGenerator::readCsv(file = file.path(workFolder,"achilles_results.csv"),
	                                            warnOnCaseMismatch = FALSE)
	colnames(achillesResults) <- SqlRender::snakeCaseToCamelCase(colnames(achillesResults))
	achillesResults$stratum1 <- as.character(achillesResults$stratum1)
	achillesResults$stratum2 <- as.character(achillesResults$stratum2)
	achillesResults$stratum3 <- as.character(achillesResults$stratum3)
	achillesResults$stratum4 <- as.character(achillesResults$stratum4)
	achillesResults$stratum5 <- as.character(achillesResults$stratum5)
	achillesResults$databaseId <- databaseId
	CohortGenerator::writeCsv(x = achillesResults,
	                          file = file.path(resultsFolder, "dp_achilles_results.csv"))

	achillesResultsAugmented <- CohortGenerator::readCsv(file = file.path(workFolder,"achilles_results_augmented.csv"),
	                                                     warnOnCaseMismatch = FALSE)
	colnames(achillesResultsAugmented) <- SqlRender::snakeCaseToCamelCase(colnames(achillesResultsAugmented))
	achillesResultsAugmented$stratum1 <- as.character(achillesResultsAugmented$stratum1)
	achillesResultsAugmented$stratum2 <- as.character(achillesResultsAugmented$stratum2)
	achillesResultsAugmented$stratum3 <- as.character(achillesResultsAugmented$stratum3)
	achillesResultsAugmented$stratum4 <- as.character(achillesResultsAugmented$stratum4)
	achillesResultsAugmented$stratum5 <- as.character(achillesResultsAugmented$stratum5)
	achillesResultsAugmented$visitAncestorConceptId <- as.character(achillesResultsAugmented$visitAncestorConceptId)
	achillesResultsAugmented$databaseId <- databaseId
	CohortGenerator::writeCsv(x = achillesResultsAugmented,
	                          file = file.path(resultsFolder, "dp_achilles_results_augmented.csv"))
}

# Format DQD results
formatDQDResults <- function(
    workFolder,
    resultsFolder,
    databaseId
) {
	dqdJsonDf <- jsonlite::fromJSON(
	  file.path(workFolder,paste0(databaseId,"_DbProfile.json")),
	  simplifyDataFrame = TRUE)

	dpOverview <- as.data.frame(dqdJsonDf$Overview)
	dpOverview$DATABASE_ID <- databaseId
	colnames(dpOverview) <- SqlRender::snakeCaseToCamelCase(colnames(dpOverview))
	CohortGenerator::writeCsv(x = dpOverview,
	                          file = file.path(resultsFolder, "dp_overview.csv"))
	
	dpCheckResults <- as.data.frame(dqdJsonDf$CheckResults)
	dpCheckResults$DATABASE_ID  <- databaseId
	dpCheckResults$THRESHOLD_VALUE <- as.character(dpCheckResults$THRESHOLD_VALUE)
	colnames(dpCheckResults) <- SqlRender::snakeCaseToCamelCase(colnames(dpCheckResults))
	CohortGenerator::writeCsv(x = dpCheckResults,
	                          file = file.path(resultsFolder, "dp_check_results.csv"))
	
	dpMetadata <- as.data.frame(dqdJsonDf$Metadata)
	dpMetadata$DATABASE_ID <- databaseId
	colnames(dpMetadata) <- SqlRender::snakeCaseToCamelCase(colnames(dpMetadata))
	CohortGenerator::writeCsv(x = dpMetadata,
	                          file = file.path(resultsFolder, "dp_metadata.csv"))
}

# Get the CDM Version in the proper format
getCdmVersion <- function(connectionDetails, cdmDatabaseSchema) {
	
	sql <- "select right(left(cdm_version,4),3) as cdm_version from @cdmDatabaseSchema.cdm_source;"
	sql <- SqlRender::render(sql, cdmDatabaseSchema = cdmDatabaseSchema)
	sql <- SqlRender::translate(sql, targetDialect = connectionDetails$dbms)
	conn <- DatabaseConnector::connect(connectionDetails) 
	cdmVersion <- DatabaseConnector::querySql(conn,sql)$CDM_VERSION
	DatabaseConnector::disconnect(conn)
	return (cdmVersion)
}

