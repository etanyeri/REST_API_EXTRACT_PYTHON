from utils import utils, SnowflakeConnector
from datetime import datetime, timedelta
from time import sleep, time
from requests.exceptions import Timeout

class JiraManager(utils.SnowflakeConnector):
    """
    This class manages the pipeline between Jira and Snowflake; it will take the configuration file provided, connect with Jira,
    pull data within the config based on its specifications, and export it to either the local system or to Snowflake.
    """

    def __init__(self, dirname, jira_config_filepath, local, **kwargs):
        super().__init__()

        with open(jira_config_filepath, "r") as f:
            config = json.load(f)
        f.close()

        self.jira_config = {
            source["NAME"]: source for source in config["SOURCE_TABLES"]
        }

        self.credentials = [
            creds["NAME"]
            for creds in config["CREDENTIALS"]
            if creds["ENVIRONMENT"] == os.environ["ENV"]
        ]

        if not local and self.credentials is None:
            raise Exception(
                "User must provide a variable indicating what environment the script is operating within"
            )

        self.dirname = dirname
        self.error_list = []
        self.bearer = utils.retrieve_jira_bearer_token()

def query_jira(self, query, output_path=None):
        if not query:
            return
        tok = utils.retrieve_jira_bearer_token()
        out = utils.call_and_configure_jira(tok, "xdbc", query=query)
        if output_path:
            out.to_csv(output_path, index=False)
        print(out)
        return out

    def pull_from_jira(
        self,
        export_name: str,
        verbose: bool = True,
    ) -> None:
        tik = time()
        sql_filepath = None
        now_dt = datetime.now()
        now = now_dt.strftime("%Y-%m-%d %H:%M:%S")
    
        try:
            sql_filepath = self.jira_config[export_name]["SOURCE_INFORMATION"]["INPUT_FILENAME"]
            table_schema = self.jira_config[export_name]["TARGET_INFORMATION"].get("TARGET_JIRA_SCHEMA")
            table_name = self.jira_config[export_name]["TARGET_INFORMATION"]["TABLE_NAME"]
            stage_table = f"STAGE_{table_name}"
            primary_keys = self.jira_config[export_name]["SOURCE_INFORMATION"]["PRIMARY_KEYS"]
            incremental_field = self.jira_config[export_name]["SOURCE_INFORMATION"]["INCREMENTAL_FIELD"]
        except KeyError as e:
            self.error_list.append(
                f"Expected the following key within the {export_name} section of the configuration file: {e}"
            )
            return
        except Exception as e:
            self.error_list.append(
                f"An unknown error has occurred when accessing the {export_name} section of the configuration file: {e}"
            )
            return

        if not sql_filepath:
            self.error_list.append(
                f"The export name needs to exist within the jira configuration file AND currently received {export_name}"
            )
            return
        
        try:
            with open(f"{self.dirname}/InputFiles/{sql_filepath}") as f:
                query = f.read()
            f.close()
        except FileNotFoundError as e:
            self.error_list.append(
                f"The basic SQL query filepath provided for {export_name} was not found; double check the configuration file and that the file exists under the 'dirname/InputFiles directory"
            )
            return
        
        if self.local:
            logging.info(f"Returning output of query for {export_name} to local")
            df = utils.call_and_configure_jira(
                access_token=self.bearer, action="dbc", query=query
            )
            df.to_csv(
                f"{self.dirname}/OutputFiles/{now.replace(':', '-').replace(' ', '_')}_{export_name}.csv",
                index=False,
            )
            logging.info("Operation complete")
            return
        else:
            window = 365 if table_name == 'JIRACOM' else 365
            look_back = (now_dt - timedelta(days=window)).strftime("%Y-%m-%d")
            query = query.format(table_schema=table_schema, stage_table=stage_table, look_back=look_back)
                # ...
            logging.info(f"Running the following key within the {export_name} section of the configuration file: {e}")
            
        try:
            self.con.autocommit(False)
            # Step 1: Truncate Stage table in case there's something in there from the last load
            self.execute_snowflake_query(
                f"TRUNCATE TABLE {os.environ['SNOWFLAKE_INGEST_DATABASE']}.{table_schema}.{stage_table};",
                verbose=True,
            )
            starting_stage_count = get_stage_count()[0]
            if starting_stage_count:
                self.error_list.append(
                    f"{os.environ['SNOWFLAKE_INGEST_DATABASE']}.{table_schema}.{stage_table} must be truncated"
                )
                return
            # Step 2: Insert into Stage
            self._upsert_jira_and_insert_to_staging(query, stage_table, verbose)
            stage_count = get_stage_count()[0]
            jira_count = utils.call_and_configure_jira(
                access_token=self.bearer, action="dbc",
                query=f"SELECT COUNT(*) FROM ({query})",
            ).iat[0, 0]
            logging.info(f"The query returns {jira_count} records in JIRA")
            logging.info(f"Inserted {stage_count} records into {os.environ['SNOWFLAKE_INGEST_DATABASE']}.{table_schema}.{stage_table}")
                        # ...
            if not stage_count:
                logging.warn("No records inserted. Continuing with next extraction")
                return
            elif stage_count != jira_count:
                self.error_list.append(
                    f"Successful API call, but there are more records in stage than what are returned by the Jira query. Stage: {stage_count}; Jira: {jira_count}"
                )
                return
            # Step 2.5: Ensure that the pipeline has completed. If not, check progress.
            current_stage_count = get_stage_count()[0]
            if current_stage_count > stage_count:
                logging.info(f"{current_stage_count - stage_count} records have been added since the last query. PriorStage: {stage_count}, CurrentStage: {current_stage_count}, Jira: {jira_count}")
                stage_count = current_stage_count
            # ...


           

