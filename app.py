
from raw_processing import make_csv_flow,combine_data_flow
from parquet_processing import compute_vol_adj
from model_training import run_model


if __name__ == "__main__":
    num_of_partitions=40
    make_csv_flow(num_of_partitions)
    combine_data_flow(num_of_partitions)
    compute_vol_adj()
    run_model()

