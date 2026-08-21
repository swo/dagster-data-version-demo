import dataclasses
import datetime
import pickle
from pathlib import Path

import dagster as dg


@dataclasses.dataclass
class VersionResult[T]:
    version: str
    value: T


# dagster demands this annotation if assets are not of core python types
VERSION_RESULT_DAGSTER_TYPE = dg.PythonObjectDagsterType(
    python_type=VersionResult, name="VersionResult"
)


# note the type explicit type declaration for dagster
@dg.asset(code_version="v1", dagster_type=VERSION_RESULT_DAGSTER_TYPE)
def one() -> dg.MaterializeResult[VersionResult[int]]:
    # load this value from a cache if possible
    cache = Path("tmp_one.pkl")
    if cache.exists():
        with cache.open("rb") as f:
            version_result: VersionResult[int] = pickle.load(f)
    else:
        # otherwise, actually compute the value of interest
        value = 1

        # package the result
        version = str(datetime.datetime.now(tz=datetime.UTC))
        version_result = VersionResult(version=version, value=value)

        # pickle it
        with cache.open("wb") as f:
            pickle.dump(version_result, f)

    # return the MatRes-wrapped value
    return dg.MaterializeResult(
        data_version=dg.DataVersion(version_result.version), value=version_result
    )


@dg.asset(
    code_version="v1",
    # you need this annotation to keep dagster happy
    ins={"one": dg.AssetIn(dagster_type=VERSION_RESULT_DAGSTER_TYPE)},
)
def two(one: VersionResult[int]) -> int:
    print(f"upstream data version: {one.version}")
    return one.value + one.value


assets = dg.load_assets_from_current_module()
defs = dg.Definitions(assets=assets)

# run this with `make main`
if __name__ == "__main__":
    result = dg.materialize([one, two])
    print(result.asset_value("one"))
    print(result.asset_value("two"))
