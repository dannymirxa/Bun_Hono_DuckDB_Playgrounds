import polars as pl
import json
import requests

def main():
    df = pl.read_csv("MYSG DSRApp copy.csv", null_values=["null", "NA", "na", "nil", "-"])

    df = df.with_columns([
        pl.col("converted_date").str.to_date("%Y-%m-%d"),	
        pl.col("deleted_date").str.to_date("%Y-%m-%d"),	
        pl.col("create_ts").str.to_datetime("%Y-%m-%d %H:%M:%S"),	    
        pl.col("update_ts").str.to_datetime("%Y-%m-%d %H:%M:%S")
    ])

    # df = df.with_columns([
    #     pl.col("post_code").str.extract(r"(\d+)", 1).cast(pl.Int64)
    # ])

    df = df.with_columns([
        pl.col("contact_phone").str.extract(r"(\d+)", 1).cast(pl.Int64).alias("contact_phone_int")
    ], )

    df = df.with_columns([
        pl.when(pl.col("converted_to_sales") == "no").then(False).otherwise(True).alias("converted_to_sales_bool").cast(pl.Boolean),
        pl.when(pl.col("is_lead_deleted") == "no").then(False).otherwise(True).alias("is_lead_deleted_bool").cast(pl.Boolean)
    ])

    df = df.drop(["contact_phone", "converted_to_sales", "is_lead_deleted"])

    df = df.rename({"contact_phone_int": "contact_phone", 
                    "converted_to_sales_bool": "converted_to_sales",
                    "is_lead_deleted_bool": "is_lead_deleted"
                    })
    
    df = df.with_columns([
        pl.col("contact_phone").cast(pl.Utf8),
        pl.col("post_code").cast(pl.Utf8)
    ])
    
    df = df.with_columns(pl.col('countrycode').str.to_uppercase())

    df_json_dict = json.dumps(df.write_json(row_oriented=True))

    # print(json.loads(df_json_dict))

    df_json_dict = json.loads(df_json_dict)

    df_json = json.loads(df_json_dict)

    url = "http://localhost:3030/insertNewDSR"
    headers = {"Content-Type": "application/json"}

    # post the data to the API

    # for x, i in enumerate(df_json_dict):
    #     print(f"index: {x}, {i}")
        # response = requests.post(url, headers=headers, json=i)
        # if response.status_code == 200:
        #     print(f"inserted json {i}")
        # else:
        #     print(f"failed to insert json {i}")

    # response = requests.post(url, json=df_json_dict)

    

    for row in df_json:
        # Send a POST request to the API
        
        # Convert the row to JSON
        data = json.dumps(row)

        print(data)

        response = requests.post(url, data=data)

        # Check the status of the request
        if response.status_code == 200:
            print("Data posted successfully.")
        else:
            print(f"Failed to post data. Status code: {response.status_code}.")

if __name__ == "__main__":
    main()