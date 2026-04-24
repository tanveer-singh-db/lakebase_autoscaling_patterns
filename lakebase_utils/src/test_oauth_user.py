
from lakebase_utils.lakebase_connect import LakebaseAutoscalingClient
import os

user = "<your-oauth-user>"
host = "<your lakebase host>"
db_name = "databricks_postgres"

with LakebaseAutoscalingClient(
    auth_mode="oauth_token",
    connection_string=f"postgresql://{user_name}@{host}/{db_name}?sslmode=require",
    token=os.environ["LAKEBASE_API_TOKEN"],
) as client:
    print(client.fetch("SELECT current_user, now()"))
    # print(client.fetch("select * from <schema>.<table_name>"))
