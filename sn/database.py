from typing import Callable

from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy as sa

    
def show_create_statements(
    base_model: sa.schema.Table,
    *,
    engine: sa.engine.Engine=None,
    log: Callable[[str], None]=print
) -> None:
    """
    TODO
    """
    if engine is None:
        engine = sa.create_engine('postgresql://')

    for name, __table__ in base_model.metadata.tables.items():
        stmt = sa.schema.CreateTable(__table__).compile(dialect=engine.dialect)
        log(stmt)
