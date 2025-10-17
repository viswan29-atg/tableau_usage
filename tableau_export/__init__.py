import os
import io
import json
import zipfile
import logging
from datetime import date
import pandas as pd
import traceback
import azure.functions as func
from azure.storage.blob import BlobServiceClient
import tableauserverclient as TSC
from tableauhyperapi import HyperProcess, Connection, Telemetry

def main(mytimer: func.TimerRequest) -> None:
    try:
        SERVER_URL = os.getenv("SERVER_URL")
        SITE_NAME = os.getenv("SITE_NAME")
        TOKEN_NAME = os.getenv("TOKEN_NAME")
        TOKEN_SECRET = os.getenv("TOKEN_SECRET")
        BLOB_CONN_STR = os.getenv("BLOB_CONN_STR")
        BLOB_CONTAINER = os.getenv("BLOB_CONTAINER", "tableau-exports")

        TARGET_DATASOURCES = ["TS Events", "TS Users", "Groups", "Site Content"]
        current_date = date.today()
        print(BLOB_CONTAINER)
        blob_service = BlobServiceClient.from_connection_string(BLOB_CONN_STR)
        container_client = blob_service.get_container_client(BLOB_CONTAINER)

        tableau_auth = TSC.PersonalAccessTokenAuth(TOKEN_NAME, TOKEN_SECRET, SITE_NAME)
        server = TSC.Server(SERVER_URL, use_server_version=True)

        output_summary = []

        with server.auth.sign_in(tableau_auth):
            all_datasources, _ = server.datasources.get()

            for target_ds in TARGET_DATASOURCES:
                matched = False
                for ds in all_datasources:
                    if target_ds.lower() in ds.name.lower():
                        matched = True
                        ds_filename = f"{ds.name}-{current_date}.tdsx"
                        logging.info(f"Found: {ds.name}")

                        tdsx_bytes = io.BytesIO()
                        server.datasources.download(ds.id, filepath=tdsx_bytes)
                        tdsx_bytes.seek(0)

                        with zipfile.ZipFile(tdsx_bytes, 'r') as zip_ref:
                            hyper_files = [f for f in zip_ref.namelist() if f.endswith(".hyper")]
                            if not hyper_files:
                                output_summary.append({target_ds: "No .hyper file found"})
                                continue
                            hyper_filename = hyper_files[0]
                            extracted_path = f"/tmp/{hyper_filename}"
                            zip_ref.extract(hyper_filename, path="/tmp")

                        with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
                            with Connection(endpoint=hyper.endpoint, database=extracted_path) as connection:
                                schema = "public"
                                tables = connection.catalog.get_table_names(schema)
                                for table_name in tables:
                                    table_def = connection.catalog.get_table_definition(table_name)
                                    column_names = [col.name for col in table_def.columns]
                                    query = f'SELECT * FROM {table_name.schema_name}.{table_name.name}'
                                    rows = connection.execute_list_query(query)
                                    df = pd.DataFrame(rows, columns=column_names)

                                    csv_name = f"{ds.name.replace(' ', '_')}-{table_name.name}-{current_date}.csv"
                                    csv_buffer = io.StringIO()
                                    df.to_csv(csv_buffer, index=False)
                                    csv_buffer.seek(0)

                                    container_client.upload_blob(
                                        name=csv_name,
                                        data=csv_buffer.getvalue(),
                                        overwrite=True
                                    )

                                    output_summary.append({
                                        "datasource": ds.name,
                                        "table": table_name.name,
                                        "blob_file": csv_name,
                                        "rows": len(df)
                                    })
                        break
                if not matched:
                    output_summary.append({target_ds: "Datasource not found"})

        logging.info(json.dumps({"status": "success", "summary": output_summary}, indent=2))

    
    except Exception as e:
        error_message = {
            "status": "error",
            "message": str(e),
            "trace": traceback.format_exc()
        }
        logging.error(json.dumps(error_message, indent=2)



