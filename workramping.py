from utils import utils, SnowflakeConnector
from datetime import datetime, timedelta
from time import sleep, time
from requests.exceptions import Timeout

class WorkrampingManager(utils.SnowflakeConnector):
    """
    This class manages the pipeline between Workramping and Snowflake; it will take the configuration file provided, connect with Workramping,
    pull data within the config based on its specifications, and export it to either the local system or to Snowflake.
    """

    def __init__(self, dirname, workramping_config_filepath, local, **kwargs):
        super().__init__()

        with open(workramping_config_filepath, "r") as f:
            config = json.load(f)
        f.close()

        self.workramping_config = {
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
        self.bearer = utils.retrieve_workramping_bearer_token()

def query_workramping(self, query, output_path=None):
        if not query:
            return
        tok = utils.retrieve_workramping_bearer_token()
        out = utils.call_and_configure_workramping(tok, "xdbc", query=query)
        if output_path:
            out.to_csv(output_path, index=False)
        print(out)
        return out

    def pull_from_workramping(
        self,
        export_name: str,
        verbose: bool = True,
    ) -> None:
        tik = time()
        sql_filepath = None
        now_dt = datetime.now()
        now = now_dt.strftime("%Y-%m-%d %H:%M:%S")
        # ...
