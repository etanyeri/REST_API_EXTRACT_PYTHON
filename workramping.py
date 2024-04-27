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
