import utils, os,json,logging
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
        ][0]
        self.local = local
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
            sql_filepath = self.jira_config[export_name]["SOURCE_INFORMATION"][
                "INPUT_FILENAME"
            ]
            table_schema = self.jira_config[export_name]["TARGET_INFORMATION"].get(
                "TABLE_SCHEMA", os.environ[SNOWFLAKE_JIRA_SCHEMA]
            )
            table_name = self.jira_config[export_name]["TARGET_INFORMATION"][
                "TABLE_NAME"
            ].upper()
            stage_table = f"STAGE_{table_name}"
            primary_keys = self.jira_config[export_name]["SOURCE_INFORMATION"][
            "PRIMARY_KEYS"
            ]
            incremental_field = self.jira_config[export_name]["SOURCE_INFORMATION"][
            "INCREMENTAL_FIELD"
            ]
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
                f"The export name needs to exist within the jira configuration file NAME; currently received {export_name}"
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
                access_token=self.bearer, action="xdbc", query=query
            )
            df.to_csv(
                f"""{self.dirname}/OutputFiles/{now.replace(':', '-').replace(' ', '_')}_{export_name}.csv""",
                index=False,
            )
            logging.info("Operation complete")
            return
            
        else:
            window = 365 
            # if table_name == 'JIRACOM' else 365
            look_back = (now_dt - timedelta(days=window)).strftime("%Y-%m-%d")
            query = f"""{query} AND {incremental_field} > (CAST'{look_back}' AS DATE)
            
            get_stage_count = lambda: self.execute_fetch_n(
                 query = f"""SELECT COUNT(*) FROM {os.environ["SNOWFLAKE_ING_DATABASE"]}.{table_schema}.{stage_table}""",
                 one_Flag =TRUE
            )
            
             logging.info(f"Running the following query in Jira: {query}")         
        try:
            self.con.autocommit(False)
            # Step 1: Truncate Stage table in case there's something in there from the last load
            self.execute_snowflake_query(
                f"TRUNCATE TABLE {os.environ['SNOWFLAKE_ING_DATABASE']}.{table_schema}.{stage_table};",
                verbose=True,
            )
            starting_stage_count = get_stage_count()[0]
            if starting_stage_count:
                self.error_list.append(
                    f"{os.environ['SNOWFLAKE_ING_DATABASE']}.{table_schema}.{stage_table} must be truncated"
                )
            # Step 2: Insert into Stage
            self._upsert_jira_and_insert_to_staging(query, stage_table, verbose)
            stage_count = get_stage_count()[0]
            jira_count = utils.call_and_configure_jira(
                access_token=self.bearer, 
                action="dbc",
                query=f"SELECT COUNT(*) FROM ({query})",
            ).iat[0, 0]
            logging.info(f"The query returns {jira_count} records in JIRA")
            logging.info(
                f"""Inserted {stage_count} records into {os.environ['SNOWFLAKE_ING_DATABASE']}.{table_schema}.{stage_table}"""
                )
            if not stage_count:
                logging.warn("No records inserted. Continuing with next extraction")
                return
            elif stage_count > jira_count:
                self.error_list.append(
                    f"Stage can not have more records that the query pulling from Jira. Stage : {stage_count}, Jira: {jira_count}
                )
                
            # Step 2.5: Ensure that the pipeline has completed. If not, check progress.
            # Jira has a bad habit of ending calls prior to completion as well as keeping calls open after completion.
            # In both of these scenarios, the Snowflake table is still getting populated to match the equivalent query is Xactly 1-1.
            # This section of code allows the process/extraction to continue even after the call ends (through Xactly or timeout) as long as the delta narrows
            patience = 0
            theta = 10
            while stage_count != jira_count:
            
                 if patience == 3:
                     self.error_list.append(
                        f"Successful API call, but mismatching records counts between the query in Jira and staging table. No change in record count in stage for the past"
                )
                return
                elif stage_count > jira_count:
                    self.error_list.append(
                        f"Successful API call, but there are more records in stage than what are returned by the Jira query. Stage: {stage_count}; Jira: {jira_count}"
                )
                return
                current_stage_count = get_stage_count()[0]
                if current_stage_count > stage_count:
                    logging.info(
                    f"{current_stage_count - stage_count} records have been added since the last query. PriorStage: {stage_count}, CurrentStage: {current_stage_count}, Jira: {Jira_count}")
                )
                stage_count = current_stage_count
                    patience = 0
                else:
                logging.info(
                    f"No change between iterations. PriorStage: {stage_count}, CurrentStage: {current_stage_count}, Xactly: {xactly_count}"
                )
                    patience += 1
               sleep(theta)

             # Step 3: Perform Upsert
            self.upsert_table_into_snowflake(
                source_table=stage_table,
                database=os.environ["SNOWFLAKE_ING_DATABASE"],
                schema=table_schema,
                target_table=table_name,
                id_columns=primary_keys,
            )
     # Step 4: Update Records in Raw table to be Deleted if the pk doesn't exist within the staging table (and is less than 1 year old)
        pk = (
            f"CONCAT({', '.join(primary_keys)})"
            if len(primary_keys) > 1 
            else primary_keys[0]
        )
        deleted_records = self.execute_snowflake_query(
            f""" UPDATE {os.environ["SNOWFLAKE_INGEST_DATABASE"]}.{table_schema}.{table_name}
            SET IS_DELETED = TRUE
            WHERE {pk} NOT IN (
                SELECT {pk} FROM {os.environ["SNOWFLAKE_ING_DATABASE"]}.{table_schema}.{stage_table}
            )  AND TO_DATE({incremental_field}) > '{look_back}';"""
        )
        for cursor in deleted_records:
            for row in cursor:
                logging.warn(f"{row[0]} were set as having been DELETED")
        
        # Step 4B: Update Records in Row table to not be deleted if the pk exists within the staging table
        recovered_records = self.execute_snowflake_query(
            f"""UPDATE {os.environ["SNOWFLAKE_INGEST_DATABASE"]}.{table_schema}.{table_name}
            SET IS_DELETED = FALSE
            WHERE {pk} IN (
                SELECT {pk} FROM {os.environ["SNOWFLAKE_INGEST_DATABASE"]}.{table_schema}.{stage_table}
            );"""
        )
        for cursor in recovered_records:
             for row in cursor:
                logging.warn(f"{row[0]} records were UNDELETED")

        # Step 5: Truncate Staging table again
        self.execute_snowflake_query(
            f"TRUNCATE TABLE {os.environ['SNOWFLAKE_ING_DATABASE']}.{table_schema}.{stage_table};",
            verbose=True,
        )
        self.con.commit()
        tok = time()
        logging.info(f"Operation complete after {(tok - tik)} seconds")
        
        except Exception as e:
            self.con.rollback()
            self.error_list.append(
                f"The following error occurred when pulling data: {e}"
            )
            
        self.con.autocommit(True)
        
def _query_jira_and_insert_to_staging(self, query, stage_table, verbose):
        insertion_statement = f"""
        INSERT INTO snowflake(
            TableName = '{stage_table}',
            CredentialName = '{self.credentials}'
            Passthrough = True
        )
        {query}
        """

        logging.info(query)

        try:
            resps = utils.call_jira_connect(
                access_token = self.bearer, action = 'dbc', query =insertion_statement
            )
            if verbose:
                logging.info(resp)
        except Timeout as e:
            logging.warn(
                "Snowflake connector query timed out: {e}. Continuing script until targeted stage table is no longer updated and does not match the number of records returned by jira query."
            )

def has_errors(self):
    return len(self.error_list) > 0
        

      

           

