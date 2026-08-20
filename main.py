import datetime
import pickle

import dagster as dg

USE_PICKLE = False


@dg.asset
def one() -> dg.MaterializeResult:
    if USE_PICKLE:
        with open("tmp_one.pkl", "rb") as f:
            return pickle.load(f)
    else:
        timestamp = str(datetime.datetime.now(tz=datetime.UTC))
        data_version = dg.DataVersion(timestamp)
        return dg.MaterializeResult(data_version=data_version, value=1)


@dg.asset
def two(one: dg.MaterializeResult) -> dg.MaterializeResult:
    return dg.MaterializeResult(
        data_version=one.data_version, value=one.value + one.value
    )


if __name__ == "__main__":
    dg.load_assets_from_current_module()
    print(dg.materialize_to_memory([two]))
