import dataclasses
import datetime
import pickle
from pathlib import Path

import dagster as dg

USE_PICKLE = True
CACHE_PATH = Path("tmp_one.pkl")


@dataclasses.dataclass
class VersionResult[T]:
    version: str
    value: T


@dg.asset(code_version="v1")
def one() -> dg.MaterializeResult[VersionResult[int]]:
    if USE_PICKLE and CACHE_PATH.exists():
        with CACHE_PATH.open("rb") as f:
            cached_result = pickle.load(f)

        version_result: VersionResult[int] = cached_result
    else:
        version = str(datetime.datetime.now(tz=datetime.UTC))
        version_result = VersionResult(version=version, value=1)

        with CACHE_PATH.open("wb") as f:
            pickle.dump(version_result, f)

    return dg.MaterializeResult(
        data_version=dg.DataVersion(version_result.version), value=version_result
    )


@dg.asset(code_version="v1")
def two(one: int):
    return one + one


assets = dg.load_assets_from_current_module()
defs = dg.Definitions(assets=assets)

if __name__ == "__main__":
    result = dg.materialize(
        [one, two],
        run_config={"loggers": {"console": {"config": {"log_level": "INFO"}}}},
    )
    print(result.asset_value("one"))
    print(result.asset_value("two"))
