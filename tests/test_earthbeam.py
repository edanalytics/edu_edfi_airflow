from pathlib import Path

import pandas as pd
import pytest

from edu_edfi_airflow.dags.earthbeam_dag import EarthbeamDAG

DEFAULT_FILE = "data"


@pytest.fixture
def basic_data():
    return pd.DataFrame(
        {
            "id": [i for i in range(6)],
            "tenant_code": (["tenant"] * 3) + (["tenant_2"] * 3),
            "api_year": (["1999"] * 2) + (["2000"] * 2) + (["2001"] * 2),
        }
    )


def validate_partitions(
    root,
    expect=[
        Path("tenant_code=tenant", "api_year=1999"),
        Path("tenant_code=tenant", "api_year=2000"),
        Path("tenant_code=tenant_2", "api_year=2000"),
        Path("tenant_code=tenant_2", "api_year=2001"),
    ],
    expect_not=[
        Path("tenant_code=tenant", "api_year=2001"),
        Path("tenant_code=tenant_2", "api_year=1999"),
    ],
):
    for path in expect:
        assert (root / path).is_dir()
        assert any((root / path).iterdir())

    for path in expect_not:
        assert not (root / path).is_dir()


def test_partition_basic(tmp_path, basic_data):
    # arrange
    data_path = tmp_path / f"{DEFAULT_FILE}.csv"
    basic_data.to_csv(data_path, index=False)

    # act
    EarthbeamDAG.partition_on_tenant_and_year(data_path, tmp_path)

    # assert
    validate_partitions(tmp_path / DEFAULT_FILE)


def test_partition_list_of_csvs(tmp_path, basic_data):
    # arrange: prepare multiple inputs for one function call
    data_file_1 = "data_file_1"
    data_file_2 = "data_file_2"
    data_file_3 = "data_file_3"

    data_path_1 = tmp_path / f"{data_file_1}.csv"
    data_path_2 = tmp_path / f"{data_file_2}.csv"
    data_path_3 = tmp_path / f"{data_file_3}.csv"

    basic_data.to_csv(data_path_1, index=False)
    basic_data.to_csv(data_path_2, index=False)
    basic_data.to_csv(data_path_3, index=False)

    # act: also test mixture of Path objects and strings
    EarthbeamDAG.partition_on_tenant_and_year(
        [data_path_1, str(data_path_2), data_path_3], tmp_path
    )

    # assert: each input file should get its own parquet
    validate_partitions(tmp_path / data_file_1)
    validate_partitions(tmp_path / data_file_2)
    validate_partitions(tmp_path / data_file_3)


def test_partition_col_names(tmp_path):
    # arrange: use different column names than what the function expects
    data = pd.DataFrame(
        {
            "id": [i for i in range(6)],
            "name": (["tenant"] * 3) + (["tenant_2"] * 3),
            "data_year": (["1999"] * 2) + (["2000"] * 2) + (["2001"] * 2),
        }
    )

    data_path = tmp_path / f"{DEFAULT_FILE}.csv"
    data.to_csv(data_path, index=False)

    # act: provide the alternative names
    EarthbeamDAG.partition_on_tenant_and_year(
        data_path, tmp_path, tenant_col="name", year_col="data_year"
    )

    # assert: result should be identical
    validate_partitions(tmp_path / DEFAULT_FILE)


def test_partition_col_maps(tmp_path):
    # arrange
    data = pd.DataFrame(
        {
            "id": [i for i in range(6)],
            "tenant_code": (["dt"] * 3) + (["dt2"] * 3),
            "api_year": (["99"] * 2) + (["00"] * 2) + (["01"] * 2),
        }
    )

    data_path = tmp_path / f"{DEFAULT_FILE}.csv"
    data.to_csv(data_path, index=False)

    # act: map the above values to the desired partition names
    EarthbeamDAG.partition_on_tenant_and_year(
        data_path,
        tmp_path,
        tenant_map={"dt": "tenant", "dt2": "tenant_2"},
        year_map={"99": "1999", "00": "2000", "01": "2001"},
    )

    # assert
    validate_partitions(tmp_path / DEFAULT_FILE)


def test_partition_col_names_and_maps(tmp_path):
    # arrange
    data = pd.DataFrame(
        {
            "id": [i for i in range(6)],
            "name": (["t"] * 3) + (["t2"] * 3),
            "data_year": (["99"] * 2) + (["00"] * 2) + (["01"] * 2),
        }
    )

    data_path = tmp_path / f"{DEFAULT_FILE}.csv"
    data.to_csv(data_path, index=False)

    # act
    EarthbeamDAG.partition_on_tenant_and_year(
        data_path,
        tmp_path,
        tenant_col="name",
        tenant_map={"t": "tenant", "t2": "tenant_2"},
        year_col="data_year",
        year_map={"99": "1999", "00": "2000", "01": "2001"},
    )

    # assert
    validate_partitions(tmp_path / DEFAULT_FILE)


def test_record_in_correct_partition(tmp_path):
    # arrange: large dataframe with a partition containing a single record
    tenant_codes = (["tenant"] * 3000000) + ["special"] + (["tenant_2"] * 2999999)
    api_years = (["1999"] * 2000000) + (["2000"] * 2000000) + (["2001"] * 2000000)
    data = pd.DataFrame(
        {
            "id": [i for i in range(6000000)],
            "tenant_code": tenant_codes,
            "api_year": api_years,
        }
    )

    data_path = tmp_path / f"{DEFAULT_FILE}.csv"
    data.to_csv(data_path, index=False)

    # act
    EarthbeamDAG.partition_on_tenant_and_year(data_path, tmp_path)

    # assert
    df = pd.read_parquet(tmp_path / DEFAULT_FILE / "tenant_code=special")

    assert len(df.index) == 1
    assert df.loc[df.index[0], "id"] == "3000000"


def test_partition_invalid_csv(tmp_path, basic_data):
    # arrange
    data_path = tmp_path / f"{DEFAULT_FILE}.yar"
    basic_data.to_csv(data_path, index=False)

    # assert: expect an error
    with pytest.raises(ValueError):
        # act: the function expects all inputs to be CSVs
        EarthbeamDAG.partition_on_tenant_and_year(data_path, tmp_path)


def test_partition_invalid_col(tmp_path, basic_data):
    # arrange
    data_path = tmp_path / f"{DEFAULT_FILE}.csv"
    basic_data.to_csv(data_path, index=False)

    # assert: expect error
    with pytest.raises(KeyError):
        # act: the function will fail to use this column
        EarthbeamDAG.partition_on_tenant_and_year(
            data_path, tmp_path, tenant_col="not_found"
        )
