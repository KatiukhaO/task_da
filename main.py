import os
from datetime import timedelta

import pandas as pandas
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions, ClusterTimeoutOptions


endpoint = "cb.jwehljizv78ywblm.cloud.couchbase.com"
username = "1109katiuha@gmail.com"
password = os.environ.get("PASSWORD")
bucket_name = "travel-sample"
FILE_PATH = r"C:\Users\1109k\Desktop\test_travel-sample_data.csv"

auth = PasswordAuthenticator(username, password)

timeout_opts = ClusterTimeoutOptions(kv_timeout=timedelta(seconds=10))

cluster = Cluster(f"couchbases://{endpoint}",
                  ClusterOptions(auth, timeout_options=timeout_opts))

cluster.wait_until_ready(timedelta(seconds=5))


def load_data():
    print("\nLoad data ...")
    try:
        sql_query = "SELECT * FROM `travel-sample`.inventory.airline"
        row_iter = cluster.query(sql_query)
        return row_iter
    except Exception as e:
        print(e)

def main():
    travel_data = load_data()

    df_ = pandas.DataFrame(travel_data)

    data_dict = {key : [] for row in df_.values for key in row[0]}

    for row in df_.values:
        for key in data_dict.keys():
            if data_dict.keys() == row[0].keys():
                data_dict[key].append(row[0][key])
            else:
                data_dict[key].append(None)

    df1 = pandas.DataFrame(data_dict)
    df1.to_csv(FILE_PATH)
    print("CSV file was create success")

    df2 = df1.copy()
    df2["textColumn"] = "test"

    df1 = df1.merge(df2, how="cross")
    print("Merge ...")
    print(df1)


if __name__ == "__main__":
    main()
