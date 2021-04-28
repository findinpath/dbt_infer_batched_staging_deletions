import json
import logging
import math
import os

import dbt
import dbt.main
import dtspec
import dtspec.specs
import pandas as pd
import sqlalchemy as sa
import yaml
from decimal import Decimal

logging.basicConfig()
logging.getLogger("dbt").setLevel(logging.INFO)

LOG = logging.getLogger('sqlalchemy.engine')
LOG.setLevel(logging.ERROR)

DBT_ROOT = os.getcwd()
DTSPEC_ROOT = os.path.join(os.getcwd(), "tests")

with open(os.path.join(os.getenv('HOME'), '.dbt', 'profiles.yml')) as f:
    DBT_PROFILE = yaml.safe_load(f)['dbt_shop']['outputs']['dev']

    SA_ENGINE = sa.create_engine(
        'snowflake://{user}:{password}@{account}.{region}/{database}/{schema}?warehouse={warehouse}&role={role}'.format(
            user=DBT_PROFILE['user'],
            password=DBT_PROFILE['password'],
            account=DBT_PROFILE['account'][:DBT_PROFILE['account'].find('.')],
            region=DBT_PROFILE['account'][DBT_PROFILE['account'].find('.') + 1:],
            database=DBT_PROFILE['database'],
            schema=DBT_PROFILE['schema'],
            warehouse=DBT_PROFILE['warehouse'],
            role=DBT_PROFILE['role']
        )
    )


def init_logger():
    level = logging.DEBUG if os.environ.get('DEBUG', False) else logging.INFO
    logging.getLogger("dbt").setLevel(logging.INFO)
    logging.getLogger('sqlalchemy.engine').setLevel(logging.ERROR)

    logging.basicConfig(level=level, format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    console = logging.StreamHandler()
    new_logger = logging.getLogger()
    new_logger.addHandler(console)
    return new_logger


LOGGER = init_logger()


def clean_test_data(api):
    LOGGER.info(
        f"Truncating data from the tables {list(set(list(api.spec['sources'].keys()) + list(api.spec['targets'].keys())))}")
    sqls = [f'TRUNCATE TABLE IF EXISTS {table};' for table in api.spec['targets'].keys()] + \
           [f'TRUNCATE TABLE IF EXISTS {table};' for table in api.spec['sources'].keys()]

    with SA_ENGINE.connect() as conn:
        for sql in set(sqls):
            conn.execute(sql)
        conn.execute("commit")


def load_sources(api):
    metadata = sa.MetaData()

    for source, data in api.spec['sources'].items():
        fqn_table_name_tokens = source.split('.')
        if (len(fqn_table_name_tokens)) == 3:
            schema_name = fqn_table_name_tokens[1]
            table_name = fqn_table_name_tokens[2]
        elif (len(fqn_table_name_tokens)) == 2:
            schema_name = fqn_table_name_tokens[0]
            table_name = fqn_table_name_tokens[1]
        else:
            schema_name = DBT_PROFILE['schema']
            table_name = source
        sa_table = sa.Table(
            table_name,
            metadata,
            autoload=True,
            autoload_with=SA_ENGINE,
            schema=schema_name
        )

        with SA_ENGINE.connect() as conn:
            serialized_data = data.serialize()
            if len(serialized_data) == 0:
                continue
            LOGGER.info(f"Inserting input data into the source table {source}")
            sa_insert = sa_table.insert().values(serialized_data)
            conn.execute(sa_insert)


class DbtRunError(Exception): pass


def run_dbt():
    dbt_project_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..')
    print(dbt_project_dir)
    dbt_args = ['run', '--project-dir', dbt_project_dir]

    dbt.logger.log_manager.reset_handlers()
    _, success = dbt.main.handle_and_check(dbt_args)
    if not success:
        raise DbtRunError('dbt failed to run successfully, please see log for details')


def _is_nan(value):
    try:
        return math.isnan(value)
    except TypeError:
        return False


def _is_null(value):
    return value in [None, pd.NaT] or _is_nan(value)


def _stringify_sa(df, sa_table):
    for col in sa_table.columns:
        # convert integer values which are represented as float in sqlalchemy back to int
        # see https://stackoverflow.com/a/53434116/497794
        if isinstance(col.type, sa.sql.sqltypes.DECIMAL) and col.type.scale == 0:
            df[col.name] = df[col.name].fillna(0).astype(int).astype(object).where(df[col.name].notnull())
        if pd.api.types.is_numeric_dtype(col):
            df[col.name] = df[col.name].apply(lambda v: "{:.4f}".format((Decimal(v))) if not pd.isna(v) else None)

    nulls_df = df.applymap(_is_null)
    str_df = df.astype({column: str for column in df.columns})

    def _replace_nulls(series1, series2):
        return series1.combine(series2, lambda value1, value2: value1 if not value2 else '{NULL}')

    return str_df.combine(nulls_df, _replace_nulls)


def load_actuals(api):
    metadata = sa.MetaData()
    serialized_actuals = {}
    with SA_ENGINE.connect() as conn:
        for target, _data in api.spec['targets'].items():
            LOGGER.info(f"Loading data from the target table {target}")
            fqn_table_name_tokens = target.split('.')
            if (len(fqn_table_name_tokens)) == 3:
                schema_name = fqn_table_name_tokens[1]
                table_name = fqn_table_name_tokens[2]
            elif (len(fqn_table_name_tokens)) == 2:
                schema_name = fqn_table_name_tokens[0]
                table_name = fqn_table_name_tokens[1]
            else:
                schema_name = DBT_PROFILE['schema']
                table_name = target
            sa_table = sa.Table(
                table_name,
                metadata,
                autoload=True,
                autoload_with=SA_ENGINE,
                schema=schema_name
            )
            df = _stringify_sa(
                pd.read_sql_table(table_name, conn, schema_name),
                sa_table
            )

            serialized_actuals[target] = {
                "records": json.loads(df.to_json(orient="records")),
                "columns": list(df.columns),
            }
    api.load_actuals(serialized_actuals)


def test_dtspec():
    with open(os.path.join(DBT_ROOT, "target", "manifest.json")) as mfile:
            dbt_manifest = json.loads(mfile.read())
    manifest = dtspec.specs.compile_dbt_manifest(dbt_manifest)

    compiled_specs = compile_dtspec(
        manifest=manifest
    )

    api = dtspec.api.Api(compiled_specs)

    api.generate_sources()
    clean_test_data(api)
    load_sources(api)
    run_dbt()
    load_actuals(api)
    api.assert_expectations()


def compile_dtspec(scenario_selector=None, case_selector=None, manifest=None):
    search_path = os.path.join(DTSPEC_ROOT, "specs")
    compiled_spec = dtspec.specs.compile_spec(
        search_path,
        scenario_selector=scenario_selector,
        case_selector=case_selector,
        manifest=manifest,
    )

    with open(os.path.join(DTSPEC_ROOT, "compiled_specs.yml"), "w") as compiled_file:
        compiled_file.write(yaml.dump(compiled_spec, default_flow_style=False))

    return compiled_spec


if __name__ == '__main__':
    test_dtspec()
