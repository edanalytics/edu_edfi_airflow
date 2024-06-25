import pandas as pd
import pytest

from edu_edfi_airflow.dags.earthbeam_dag import EarthbeamDAG

def test_data_small():
    return pd.DataFrame({
        'id': [i for i in range(6)],
        'tenant_code': (['demo_tenant' ] * 3) + (['demo_tenant_2'] * 3),
        'api_year': (['1999'] * 2) + (['2000'] * 2) + (['2001'] * 2)
    })

def test_partition_basic(tmp_path):
    data_filename = 'data'
    data_path = tmp_path / f'{data_filename}.csv'

    test_data_small().to_csv(data_path, index=False)

    EarthbeamDAG.partition_on_tenant_and_year(data_path, tmp_path)

    assert (tmp_path / data_filename / "tenant_code=demo_tenant").is_dir()
    assert (tmp_path / data_filename / "tenant_code=demo_tenant" / "api_year=1999").is_dir()
    assert any((tmp_path / data_filename / "tenant_code=demo_tenant" / "api_year=1999").iterdir())

    assert (tmp_path / data_filename / "tenant_code=demo_tenant" / "api_year=2000").is_dir()
    assert not (tmp_path / data_filename / "tenant_code=demo_tenant" / "api_year=2001").is_dir()

    assert (tmp_path / data_filename / "tenant_code=demo_tenant_2").is_dir()
    assert not (tmp_path / data_filename / "tenant_code=demo_tenant_2" / "api_year=1999").is_dir()
    assert (tmp_path / data_filename / "tenant_code=demo_tenant_2" / "api_year=2000").is_dir()
    assert (tmp_path / data_filename / "tenant_code=demo_tenant_2" / "api_year=2001").is_dir()

def test_partition_list_of_csvs(tmp_path):
    simple_data = {
        'id': [i for i in range(6)],
        'tenant_code': (['demo_tenant' ] * 3) + (['demo_tenant_2'] * 3),
        'api_year': (['1999'] * 2) + (['2000'] * 2) + (['2001'] * 2)
    }

    # TODO: bad names
    data_file_1 = "data_file_1"
    data_file_2 = "data_file_2"
    data_file_3 = "data_file_3"

    data_path_1 = tmp_path / f'{data_file_1}.csv'
    data_path_2 = tmp_path / f'{data_file_2}.csv'
    data_path_3 = tmp_path / f'{data_file_3}.csv'

    pd.DataFrame(simple_data).to_csv(data_path_1, index=False)
    pd.DataFrame(simple_data).to_csv(data_path_2, index=False)
    pd.DataFrame(simple_data).to_csv(data_path_3, index=False)

    EarthbeamDAG.partition_on_tenant_and_year([data_path_1, str(data_path_2), data_path_3], tmp_path)

    assert (tmp_path / data_file_1 / "tenant_code=demo_tenant").is_dir()
    assert (tmp_path / data_file_1 / "tenant_code=demo_tenant" / "api_year=1999").is_dir()
    assert any((tmp_path / data_file_1 / "tenant_code=demo_tenant" / "api_year=1999").iterdir())

    assert (tmp_path / data_file_1 / "tenant_code=demo_tenant" / "api_year=2000").is_dir()
    assert not (tmp_path / data_file_1 / "tenant_code=demo_tenant" / "api_year=2001").is_dir()

    assert (tmp_path / data_file_1 / "tenant_code=demo_tenant_2").is_dir()
    assert not (tmp_path / data_file_1 / "tenant_code=demo_tenant_2" / "api_year=1999").is_dir()
    assert (tmp_path / data_file_1 / "tenant_code=demo_tenant_2" / "api_year=2000").is_dir()
    assert (tmp_path / data_file_1 / "tenant_code=demo_tenant_2" / "api_year=2001").is_dir()


    assert (tmp_path / data_file_2 / "tenant_code=demo_tenant").is_dir()
    assert (tmp_path / data_file_2 / "tenant_code=demo_tenant" / "api_year=1999").is_dir()
    assert any((tmp_path / data_file_2 / "tenant_code=demo_tenant" / "api_year=1999").iterdir())

    assert (tmp_path / data_file_2 / "tenant_code=demo_tenant" / "api_year=2000").is_dir()
    assert not (tmp_path / data_file_2 / "tenant_code=demo_tenant" / "api_year=2001").is_dir()

    assert (tmp_path / data_file_2 / "tenant_code=demo_tenant_2").is_dir()
    assert not (tmp_path / data_file_2 / "tenant_code=demo_tenant_2" / "api_year=1999").is_dir()
    assert (tmp_path / data_file_2 / "tenant_code=demo_tenant_2" / "api_year=2000").is_dir()
    assert (tmp_path / data_file_2 / "tenant_code=demo_tenant_2" / "api_year=2001").is_dir()


    assert (tmp_path / data_file_3 / "tenant_code=demo_tenant").is_dir()
    assert (tmp_path / data_file_3 / "tenant_code=demo_tenant" / "api_year=1999").is_dir()
    assert any((tmp_path / data_file_3 / "tenant_code=demo_tenant" / "api_year=1999").iterdir())

    assert (tmp_path / data_file_3 / "tenant_code=demo_tenant" / "api_year=2000").is_dir()
    assert not (tmp_path / data_file_3 / "tenant_code=demo_tenant" / "api_year=2001").is_dir()

    assert (tmp_path / data_file_3 / "tenant_code=demo_tenant_2").is_dir()
    assert not (tmp_path / data_file_3 / "tenant_code=demo_tenant_2" / "api_year=1999").is_dir()
    assert (tmp_path / data_file_3 / "tenant_code=demo_tenant_2" / "api_year=2000").is_dir()
    assert (tmp_path / data_file_3 / "tenant_code=demo_tenant_2" / "api_year=2001").is_dir()

def test_partition_col_names(tmp_path):
    data_filename = 'data'
    data_path = tmp_path / f'{data_filename}.csv'

    test_data_small().to_csv(data_path, index=False)

    EarthbeamDAG.partition_on_tenant_and_year(data_path, tmp_path, tenant_col="name", year_col="data_year")

    assert (tmp_path / data_filename / "tenant_code=demo_tenant").is_dir()
    assert (tmp_path / data_filename / "tenant_code=demo_tenant" / "api_year=1999").is_dir()
    assert any((tmp_path / data_filename / "tenant_code=demo_tenant" / "api_year=1999").iterdir())

    assert (tmp_path / data_filename / "tenant_code=demo_tenant" / "api_year=2000").is_dir()
    assert not (tmp_path / data_filename / "tenant_code=demo_tenant" / "api_year=2001").is_dir()

    assert (tmp_path / data_filename / "tenant_code=demo_tenant_2").is_dir()
    assert not (tmp_path / data_filename / "tenant_code=demo_tenant_2" / "api_year=1999").is_dir()
    assert (tmp_path / data_filename / "tenant_code=demo_tenant_2" / "api_year=2000").is_dir()
    assert (tmp_path / data_filename / "tenant_code=demo_tenant_2" / "api_year=2001").is_dir()

def test_partition_col_maps(tmp_path):
    simple_data = {
        'id': [i for i in range(6)],
        'tenant_code': (['dt' ] * 3) + (['dt2'] * 3),
        'api_year': (['99'] * 2) + (['00'] * 2) + (['01'] * 2)
    }

    data_filename = 'data'
    data_path = tmp_path / f'{data_filename}.csv'

    test_data_small().to_csv(data_path, index=False)

    EarthbeamDAG.partition_on_tenant_and_year(data_path, tmp_path, tenant_map={'dt': 'demo_tenant', 'dt2': 'demo_tenant_2'}, year_map={'99': '1999', '00': '2000', '01': '2001'})

    assert (tmp_path / data_filename / "tenant_code=demo_tenant").is_dir()
    assert (tmp_path / data_filename / "tenant_code=demo_tenant" / "api_year=1999").is_dir()
    assert any((tmp_path / data_filename / "tenant_code=demo_tenant" / "api_year=1999").iterdir())

    assert (tmp_path / data_filename / "tenant_code=demo_tenant" / "api_year=2000").is_dir()
    assert not (tmp_path / data_filename / "tenant_code=demo_tenant" / "api_year=2001").is_dir()

    assert (tmp_path / data_filename / "tenant_code=demo_tenant_2").is_dir()
    assert not (tmp_path / data_filename / "tenant_code=demo_tenant_2" / "api_year=1999").is_dir()
    assert (tmp_path / data_filename / "tenant_code=demo_tenant_2" / "api_year=2000").is_dir()
    assert (tmp_path / data_filename / "tenant_code=demo_tenant_2" / "api_year=2001").is_dir()

def test_partition_col_names_and_maps(tmp_path):
    data_filename = 'data'
    data_path = tmp_path / f'{data_filename}.csv'

    test_data_small().to_csv(data_path, index=False)

    EarthbeamDAG.partition_on_tenant_and_year(data_path, tmp_path, tenant_col="name", tenant_map={'dt': 'demo_tenant', 'dt2': 'demo_tenant_2'}, year_col="data_year", year_map={'99': '1999', '00': '2000', '01': '2001'})

    assert (tmp_path / data_filename / "tenant_code=demo_tenant").is_dir()
    assert (tmp_path / data_filename / "tenant_code=demo_tenant" / "api_year=1999").is_dir()
    assert any((tmp_path / data_filename / "tenant_code=demo_tenant" / "api_year=1999").iterdir())

    assert (tmp_path / data_filename / "tenant_code=demo_tenant" / "api_year=2000").is_dir()
    assert not (tmp_path / data_filename / "tenant_code=demo_tenant" / "api_year=2001").is_dir()

    assert (tmp_path / data_filename / "tenant_code=demo_tenant_2").is_dir()
    assert not (tmp_path / data_filename / "tenant_code=demo_tenant_2" / "api_year=1999").is_dir()
    assert (tmp_path / data_filename / "tenant_code=demo_tenant_2" / "api_year=2000").is_dir()
    assert (tmp_path / data_filename / "tenant_code=demo_tenant_2" / "api_year=2001").is_dir()


def test_record_in_correct_partition(tmp_path):
    simple_data = {
        'id': [i for i in range(6000000)],
        'tenant_code': (['demo_tenant' ] * 3000000) + ['special_tenant'] + (['demo_tenant_2'] * 2999999),
        'api_year': (['1999'] * 2000000) + (['2000'] * 2000000) + (['2001'] * 2000000)
    }

    data_filename = 'data'
    data_path = tmp_path / f'{data_filename}.csv'

    pd.DataFrame(simple_data).to_csv(data_path, index=False)

    EarthbeamDAG.partition_on_tenant_and_year(data_path, tmp_path)

    df = pd.read_parquet(tmp_path / data_filename / 'tenant_code=special_tenant') 

    assert len(df.index) == 1
    assert df.loc[df.index[0], 'id'] == '3000000'

def test_partition_invalid_csv(tmp_path):
    data_filename = 'data'

    # function expects all inputs to be CSVs
    data_path = tmp_path / f'{data_filename}.yar'

    test_data_small().to_csv(data_path, index=False)

    with pytest.raises(ValueError):
        EarthbeamDAG.partition_on_tenant_and_year(data_path, tmp_path)

def test_partition_invalid_col(tmp_path):
    data_filename = 'data'
    data_path = tmp_path / f'{data_filename}.csv'

    test_data_small().to_csv(data_path, index=False)

    with pytest.raises(KeyError):
        EarthbeamDAG.partition_on_tenant_and_year(data_path, tmp_path, tenant_col='not_found')