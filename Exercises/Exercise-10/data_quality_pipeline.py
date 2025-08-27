import os
import great_expectations as gx
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(levelname)s - %(message)s')

class BikeTripsDataQualityPipeline:
    def __init__(self, dataframe):
        self.logger = logging.getLogger(__name__)
        self.context = self.initialise_gx_context()
        self.dataframe = dataframe

    def initialise_gx_context(self):
        try:
            context = gx.get_context()
            self.logger.info("Using existing Great Expectation context.")
        except:
            context = gx.get_context(mode="file", project_root_dir=os.getcwd())
            self.logger.info("Created new Greate Expectation context.")
        
        return context
    
    def create_data_source_and_asset(self):
        datasource_name = "bike_trips_datasource"
        try:
            datasource = self.context.data_sources.get(datasource_name)
            self.logger.info(f"Using existing datasource: {datasource_name}")
        except:
            datasource = self.context.data_sources.add_spark(datasource_name)
            self.logger.info(f"Created new datasource: {datasource_name}")
        
        asset_name = "bike_trips_asset"
        try:
            data_asset = datasource.get_asset(asset_name)
            self.logger.info(f"Using existing data asset: {asset_name}.")
        except:
            data_asset = datasource.add_dataframe_asset(name=asset_name)
            self.logger.info(f"Created new data asset: {asset_name}.")

        return datasource, data_asset
    
    def create_batch_definition(self, data_asset):
        definition_name = "bike_trips_batch_definition"
        try:
            batch_definition = data_asset.get_batch_definition(definition_name)
            self.logger.info(f"Using existing batch definition name: {definition_name}.")
        except:
            batch_definition = data_asset.add_batch_definition_whole_dataframe(definition_name)
            self.logger.info(f"Created new batch definition: {definition_name}.")
        return batch_definition
    
    def create_expectation_suite(self):
        suite_name = "bike_trips_quality_suite"
        try:
            suite = self.context.suites.get(name=suite_name)
            self.logger.info(f"Using existing expectation suite: {suite_name}.")
        except:
            suite = gx.ExpectationSuite(name=suite_name)
            suite = self.context.suites.add(suite)
            self.logger.info(f"Created new expectation suite: {suite_name}.")

        suite.expectations = []

        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(column="duration_seconds", min_value=1, max_value=86400, strict_max=True)
        )

        self.logger.info(f"Created expectation suite with {len(suite.expectations)} expectations.")
        return suite
    
    def create_validation_definition(self, exp_suite, batch_definition):
        definition_name = "bike_trips_validation_definition"
        try:
            validation_definition = self.context.validation_definitions.get(definition_name)
        except:
            validation_definition = gx.ValidationDefinition(data=batch_definition, suite=exp_suite, name=definition_name)
        return validation_definition
    
    def run_validation(self, df, validation_definition):
        batch_parameters_dataframe = {"dataframe": df}
        validation_results = validation_definition.run(batch_parameters=batch_parameters_dataframe)
        return validation_results
    
    def summarize_and_print(self, result):
        print("\n=== Great Expectations Validation Report ===")
        print(f"Success: {result['success']}")
        for i, res in enumerate(result["results"], start=1):
            exp = res["expectation_config"]["type"]
            success = res["success"]
            detail = res.get("result", {})
            unexpected_count = detail.get("unexpected_count", 0)
            element_count = detail.get("element_count", 0)
            percentage = detail.get("unexpected_percent", 0)
            unexpected_list = detail.get("partial_unexpected_list", 0)
            print(f"{i:02d}. {exp}: success={success}, unexpected={unexpected_count}/{element_count} ({percentage or 'n/a'})")
            print(f" -> Unexpected values: {unexpected_list}")
        print("============================================\n")
    
    def validate(self):
        try:
            if not self.dataframe:
                raise RuntimeError("Invalid dataframe is entered")
            
            datasource, data_asset = self.create_data_source_and_asset()
            batch_definition = self.create_batch_definition(data_asset)
            exp_suite = self.create_expectation_suite()
            validation_definition = self.create_validation_definition(exp_suite, batch_definition)

            validation_result = self.run_validation(self.dataframe, validation_definition)
            self.logger.info(f"Validation completed. Success: {validation_result.success}")
            self.summarize_and_print(validation_result)

        except Exception as e:
            self.logger.error(f"Pipeline error: {str(e)}")
            
